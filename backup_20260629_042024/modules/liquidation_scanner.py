"""
CRYPTO MONITOR PRO — Liquidation Scanner + Aggregator
Detects: long/short liquidations, waves, dominant side
Provides: real liq clusters from WebSocket events
"""
import asyncio
import time
from datetime import datetime, timedelta
from collections import defaultdict
from loguru import logger

from config.settings import settings
from core.models import LiquidationData, LiquidationEvent, Direction, Exchange
from core.state_manager import state_manager


class LiquidationAggregator:
    """
    Collects real liquidation events from WebSocket and stores them
    in a rolling 24h window per symbol. Provides `get_clusters()` 
    for real liquidation zones based on ACTUAL data.
    """

    ROLLING_WINDOW = 86400  # 24h in seconds

    def __init__(self):
        # symbol -> [(timestamp, price, usdt, side)]
        self._events: dict[str, list] = defaultdict(list)
        self._last_cleanup = time.time()

    def add_event(self, symbol: str, price: float, usdt_value: float, side: str):
        now = time.time()
        self._events[symbol].append((now, price, usdt_value, side))
        # Periodic cleanup every 5 min
        if now - self._last_cleanup > 300:
            self._cleanup()

    def _cleanup(self):
        cutoff = time.time() - self.ROLLING_WINDOW
        for symbol in list(self._events.keys()):
            self._events[symbol] = [
                e for e in self._events[symbol] if e[0] >= cutoff
            ]
            if not self._events[symbol]:
                del self._events[symbol]
        self._last_cleanup = time.time()

    def get_clusters(self, symbol: str, min_usdt: float = 10_000) -> list[dict]:
        """
        Returns real liquidation clusters for a symbol.
        Each cluster: {"price": float, "total_usdt": float, "count": int, "side": "long_liq"|"short_liq"}
        Sorted by total_usdt descending.
        """
        events = self._events.get(symbol, [])
        if not events:
            return []

        cutoff = time.time() - self.ROLLING_WINDOW
        # Group by price levels (round to 0.5% buckets)
        buckets: dict[float, dict] = {}
        for ts, price, usdt, side in events:
            if ts < cutoff or price <= 0:
                continue
            # Round price to 0.5% buckets
            bucket_key = round(price * 20) / 20  # 5% precision
            if bucket_key not in buckets:
                buckets[bucket_key] = {"long_total": 0.0, "short_total": 0.0, "count": 0}
            if side == "BUY":
                # BUY order = SHORT position liquidated
                buckets[bucket_key]["short_total"] += usdt
            else:
                # SELL order = LONG position liquidated
                buckets[bucket_key]["long_total"] += usdt
            buckets[bucket_key]["count"] += 1

        clusters = []
        for price_level, data in buckets.items():
            if data["long_total"] >= min_usdt:
                clusters.append({
                    "price": price_level,
                    "total_usdt": data["long_total"],
                    "count": data["count"],
                    "side": "long_liq",
                })
            if data["short_total"] >= min_usdt:
                clusters.append({
                    "price": price_level,
                    "total_usdt": data["short_total"],
                    "count": data["count"],
                    "side": "short_liq",
                })

        clusters.sort(key=lambda x: x["total_usdt"], reverse=True)
        return clusters[:10]

    def get_total_24h(self, symbol: str) -> dict:
        events = self._events.get(symbol, [])
        cutoff = time.time() - self.ROLLING_WINDOW
        total_long = sum(e[2] for e in events if e[0] >= cutoff and e[3] != "BUY")
        total_short = sum(e[2] for e in events if e[0] >= cutoff and e[3] == "BUY")
        count = sum(1 for e in events if e[0] >= cutoff)
        return {"long": total_long, "short": total_short, "count": count, "total": total_long + total_short}


# Global aggregator instance
liq_aggregator = LiquidationAggregator()


class LiquidationScanner:
    """
    Tracks liquidation orders and detects waves.

    Binance convention:
    - BUY order forced = SHORT position was liquidated
    - SELL order forced = LONG position was liquidated

    Wave detection:
    - Multiple liquidations in short window = wave
    - Calculate long_liq_usdt vs short_liq_usdt
    - Determine dominant side
    """

    def __init__(self, event_callback):
        self.event_callback = event_callback
        self._wave_starts: dict[str, datetime] = {}
        self._running = False

    async def start(self):
        self._running = True
        asyncio.create_task(self._wave_detection_loop())
        logger.info("✅ Liquidation Scanner started")

    async def process_liquidation(self, liq: LiquidationData):
        """Called for every liquidation order received"""
        if not self._running:
            return

        if liq.usdt_value < settings.liq_min_usdt:
            return

        exchange = liq.exchange.value
        symbol = liq.symbol

        # Store in Redis window
        side = "BUY" if liq.side == Direction.BUY else "SELL"
        await state_manager.add_liquidation(
            exchange, symbol, side,
            liq.usdt_value, datetime.utcnow().timestamp()
        )

        # Feed into aggregator for real cluster tracking
        liq_aggregator.add_event(symbol, liq.price, liq.usdt_value, side)

        # Check for immediate large liquidation
        if liq.usdt_value >= settings.liq_min_usdt * 10:
            await self._check_and_emit(exchange, symbol)

    async def _wave_detection_loop(self):
        """Check all active symbols for liquidation waves every 5 seconds"""
        while self._running:
            try:
                # Get all symbols being tracked
                symbols = await state_manager.get_symbols("binance", "futures")
                tasks = [
                    self._check_and_emit("binance", symbol)
                    for symbol in list(symbols)[:200]  # limit
                ]
                if tasks:
                    await asyncio.gather(*tasks, return_exceptions=True)
            except Exception as e:
                logger.debug(f"Wave detection error: {e}")

            await asyncio.sleep(5)

    async def _check_and_emit(self, exchange: str, symbol: str):
        """Check liquidation window and emit event if significant"""
        try:
            liqs = await state_manager.get_liquidations_window(
                exchange, symbol, seconds=settings.liq_wave_window
            )

            if not liqs:
                return

            long_liq = sum(l["usdt"] for l in liqs if l["side"] == "SELL")  # SELL=long liq
            short_liq = sum(l["usdt"] for l in liqs if l["side"] == "BUY")  # BUY=short liq
            total = long_liq + short_liq

            if total < settings.liq_min_usdt:
                return

            # Check cooldown
            if await state_manager.is_event_sent(exchange, symbol, "liquidation"):
                return

            # Determine dominant side
            if long_liq > short_liq * 1.5:
                dominant = Direction.SELL  # Long positions dominated
            elif short_liq > long_liq * 1.5:
                dominant = Direction.BUY   # Short positions dominated
            else:
                dominant = Direction.NEUTRAL

            # Start time
            key = f"{exchange}:{symbol}"
            oldest_ts = min(l["ts"] for l in liqs)
            start_time = datetime.fromtimestamp(oldest_ts)

            if key not in self._wave_starts:
                self._wave_starts[key] = start_time

            duration = int((datetime.utcnow() - self._wave_starts[key]).total_seconds())
            is_wave = len(liqs) >= 5 and duration >= 10

            event = LiquidationEvent(
                symbol=symbol,
                exchange=Exchange(exchange),
                long_liq_usdt=long_liq,
                short_liq_usdt=short_liq,
                dominant_side=dominant,
                start_time=self._wave_starts[key],
                duration_seconds=duration,
                is_wave=is_wave
            )

            await state_manager.mark_event_sent(exchange, symbol, "liquidation", cooldown=60)
            await state_manager.increment_stat("liq_events")

            dominant_str = {
                Direction.SELL: "LONG DOMINANT",
                Direction.BUY: "SHORT DOMINANT",
                Direction.NEUTRAL: "MIXED"
            }[dominant]

            logger.info(
                f"💥 Liquidation: {symbol} | "
                f"Long: ${long_liq:,.0f} Short: ${short_liq:,.0f} | "
                f"{dominant_str}"
            )
            await self.event_callback(event)

            # Reset wave start after event
            if key in self._wave_starts:
                del self._wave_starts[key]

        except Exception as e:
            logger.debug(f"Liquidation check error {symbol}: {e}")

    async def stop(self):
        self._running = False
