"""
CRYPTO MONITOR PRO — Whale, Funding & Order Book Scanners

Tuzatishlar:
- Funding cooldown: 3600s (1 soat) — spam yo'q
- Whale cooldown: 60s
- OrderBook cooldown: 120s
- Ball tizimi olib tashlandi
"""
import asyncio
import time
from datetime import datetime
from collections import defaultdict
from loguru import logger

from config.settings import settings
from core.models import (
    TradeData, FundingData, OrderBookSnapshot, OrderBookLevel,
    WhaleEvent, FundingEvent, OrderBookEvent,
    Direction, Exchange
)
from core.state_manager import state_manager


# ════════════════════════════════════════════
# WHALE SCANNER
# ════════════════════════════════════════════

class WhaleScanner:
    """
    CEXTrack uslubida signal:
    #RAY buying 109K USDT in 49 sec (14%) on Binance
    P: 0.618  (1.98%)  Vol 24h: 870K USDT
    """

    def __init__(self, event_callback):
        self.event_callback = event_callback
        # Har bir symbol uchun trades ro'yxati
        self._trades: dict[str, list] = defaultdict(list)
        # 24h ticker cache: symbol -> {volume_24h, price_change_pct}
        self._ticker_24h: dict[str, dict] = {}
        self._running = False

    async def start(self):
        self._running = True
        asyncio.create_task(self._burst_check_loop())
        asyncio.create_task(self._fetch_24h_tickers())
        logger.info("✅ Whale Scanner started")

    async def _fetch_24h_tickers(self):
        """Har 5 daqiqada 24h ticker ma'lumotlarini yangilash"""
        import aiohttp
        while self._running:
            try:
                url = "https://fapi.binance.com/fapi/v1/ticker/24hr"
                async with aiohttp.ClientSession() as session:
                    async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                        if resp.status == 200:
                            data = await resp.json()
                            for t in data:
                                sym = t.get("symbol", "")
                                self._ticker_24h[sym] = {
                                    "volume_24h": float(t.get("quoteVolume", 0)),
                                    "price_change_pct": float(t.get("priceChangePercent", 0)),
                                    "last_price": float(t.get("lastPrice", 0)),
                                }
                            logger.debug(f"✅ 24h ticker yangilandi: {len(self._ticker_24h)} symbol")
            except Exception as e:
                logger.debug(f"24h ticker xato: {e}")
            await asyncio.sleep(300)  # 5 daqiqada bir

    async def process_whale_trade(self, trade: TradeData):
        if not self._running:
            return

        key = f"{trade.exchange.value}:{trade.symbol}"
        trade_ts = time.time()

        self._trades[key].append({
            "usdt": trade.usdt_value,
            "side": trade.side,
            "ts": trade_ts,
        })

    async def _burst_check_loop(self):
        """Har 3 sekundda burst tekshirish — @cryptoarsenal formati"""
        WINDOWS = [
            ("60s",  60),
            ("3m",   180),
            ("10m",  600),
            ("15m",  900),
        ]

        while self._running:
            now = time.time()

            for key, trades in list(self._trades.items()):
                if not trades:
                    continue

                exchange, symbol = key.split(":", 1)

                # DEBUG: qancha trade bor
                total_usdt = sum(t["usdt"] for t in trades)
                if total_usdt > 5_000:
                    logger.debug(f"🐋 {symbol}: {len(trades)} trades, total=${total_usdt:,.0f}")

                self._trades[key] = [t for t in trades if now - t["ts"] <= 900]
                trades = self._trades[key]

                if not trades:
                    continue

                ticker = self._ticker_24h.get(symbol, {})
                volume_24h = ticker.get("volume_24h", 0)
                price_change_pct = ticker.get("price_change_pct", 0)
                last_price = ticker.get("last_price", 0)

                for win_label, win_sec in WINDOWS:
                    cutoff = now - win_sec
                    window_trades = [t for t in trades if t["ts"] >= cutoff]
                    if not window_trades:
                        continue

                    total_buy = sum(t["usdt"] for t in window_trades if t["side"] == Direction.BUY)
                    total_sell = sum(t["usdt"] for t in window_trades if t["side"] == Direction.SELL)
                    total = total_buy + total_sell

                    vol_pct = (total / volume_24h * 100) if volume_24h > 0 else 0

                    # DEBUG: qancha trade bor
                    if total > 50_000:
                        logger.debug(f"🐋 {symbol} {win_label}: total=${total:,.0f} buy=${total_buy:,.0f} sell=${total_sell:,.0f}")

                    # CEXTrack format: 5%+ 24h hajm YOKI katta USDT
                    is_big_coin = symbol in ("BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT")
                    min_usdt = 50_000 if is_big_coin else 30_000

                    # 5% dan kichik bo'lsa — chiqarmaymiz
                    if vol_pct < 5.0:
                        continue
                    # Hajm 0 yoki manfiy bo'lsa — chiqarmaymiz
                    if volume_24h <= 0:
                        continue

                    # Direction — activity agar buy/sell yaqin bo'lsa (10% ichida)
                    if total_buy > 0 and total_sell > 0:
                        ratio = min(total_buy, total_sell) / max(total_buy, total_sell)
                        if ratio >= 0.9:  # 90% yaqin = activity
                            direction = Direction.NEUTRAL
                        elif total_buy >= total_sell:
                            direction = Direction.BUY
                        else:
                            direction = Direction.SELL
                    elif total_buy >= total_sell:
                        direction = Direction.BUY
                    else:
                        direction = Direction.SELL

                    event_key = f"whale_{win_label}"
                    if await state_manager.is_event_sent(exchange, symbol, event_key):
                        continue

                    oldest = min(t["ts"] for t in window_trades)
                    elapsed = int(now - oldest)
                    token_qty = total / last_price if last_price > 0 else 0

                    event = WhaleEvent(
                        symbol=symbol,
                        exchange=Exchange(exchange),
                        direction=direction,
                        volume_usdt=total,
                        token_qty=token_qty,
                        volume_24h=volume_24h,
                        volume_pct_of_24h=vol_pct,
                        start_time=datetime.utcfromtimestamp(oldest),
                        duration_seconds=elapsed,
                        order_count=len(window_trades),
                    )
                    event.price_change_pct = price_change_pct
                    event.buy_volume = total_buy
                    event.sell_volume = total_sell

                    cooldown = win_sec
                    await state_manager.mark_event_sent(exchange, symbol, event_key, cooldown=cooldown)
                    await state_manager.increment_stat("whale_events")

                    # DB ga saqlash
                    from db.whale_db import save_whale_event
                    save_whale_event(
                        symbol=symbol,
                        exchange=exchange,
                        direction="buy" if total_buy >= total_sell else "sell",
                        volume_usdt=total,
                        volume_24h=volume_24h,
                        volume_pct=vol_pct,
                        price=last_price,
                        price_change_pct=price_change_pct,
                        duration_seconds=elapsed,
                        order_count=len(window_trades),
                    )

                    # Emoji: 💰 buy / 💸 sell / 📊 activity
                    if total_buy > total_sell:
                        emoji = "💰"
                        dir_str = "buying"
                    elif total_sell > total_buy:
                        emoji = "💸"
                        dir_str = "selling"
                    else:
                        emoji = "📊"
                        dir_str = "activity"

                    # Oxirgi whale vaqti
                    from db.whale_db import get_last_whale_time, fmt_last_seen
                    last_time = get_last_whale_time(symbol)
                    last_seen_str = ""
                    if last_time:
                        ago = int(now - last_time)
                        if ago > 60:
                            last_seen_str = f"\nLast {fmt_last_seen(last_time)}"

                    exchange_name = "Binance Futures" if "futures" in exchange.lower() or "binance" in exchange.lower() else exchange
                    price_arrow = "⬆️" if price_change_pct >= 0 else "⬇️"

                    logger.info(
                        f"🎰 #{symbol.replace('USDT','')} {emoji} {dir_str} "
                        f"{self._fmt_usdt(total)} in {self._fmt_duration(elapsed)} ({vol_pct:.0f}%) "
                        f"on {exchange_name}\n"
                        f"P: {last_price} {price_arrow} ({price_change_pct:+.2f}%) "
                        f"Vol 24h: {self._fmt_usdt(volume_24h)}"
                        f"{last_seen_str}"
                    )
                    await self.event_callback(event)
                    break

            await asyncio.sleep(3)

    @staticmethod
    def _fmt_usdt(val: float) -> str:
        if val >= 1_000_000:
            return f"{val/1_000_000:.1f}M USDT"
        elif val >= 1_000:
            return f"{val/1_000:.0f}K USDT"
        return f"{val:.0f} USDT"

    @staticmethod
    def _fmt_duration(seconds: int) -> str:
        if seconds < 60:
            return f"{seconds} sec"
        elif seconds < 3600:
            return f"{seconds // 60} min"
        return f"{seconds // 3600}h {(seconds % 3600) // 60}min"

    async def stop(self):
        self._running = False


# ════════════════════════════════════════════
# FUNDING SCANNER
# ════════════════════════════════════════════

class FundingScanner:
    def __init__(self, event_callback):
        self.event_callback = event_callback
        self._running = False

    async def start(self):
        self._running = True
        logger.info("✅ Funding Scanner started")

    async def process_funding_update(self, funding: FundingData):
        if not self._running:
            return

        exchange = funding.exchange.value
        symbol = funding.symbol
        rate = funding.funding_rate

        is_extreme_positive = rate >= settings.funding_extreme_positive
        is_extreme_negative = rate <= settings.funding_extreme_negative

        if not (is_extreme_positive or is_extreme_negative):
            return

        current_data, prev_data = await state_manager.get_funding(exchange, symbol)
        prev_rate = prev_data["rate"] if prev_data else 0.0

        is_reversal = (
            prev_rate != 0.0 and
            (prev_rate > 0) != (rate > 0) and
            abs(rate - prev_rate) >= 0.001
        )

        # Cooldown: 3600s = 1 soat (spam oldini olish)
        if await state_manager.is_event_sent(exchange, symbol, "funding"):
            return

        event = FundingEvent(
            symbol=symbol,
            exchange=funding.exchange,
            funding_rate=rate,
            is_extreme=True,
            is_reversal=is_reversal,
            previous_rate=prev_rate,
            timestamp=funding.timestamp
        )

        await state_manager.mark_event_sent(exchange, symbol, "funding", cooldown=3600)
        await state_manager.increment_stat("funding_events")

        # Narxni REST dan ol
        try:
            import aiohttp
            url = f"https://fapi.binance.com/fapi/v1/ticker/price?symbol={symbol}"
            async with aiohttp.ClientSession() as session:
                async with session.get(url) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        price = float(data.get("price", 0))
                        if price > 0:
                            await state_manager.set_ticker(
                                exchange, symbol, {"price": price, "ts": 0}
                            )
                            from modules.price_tracker import price_tracker
                            price_tracker.update_price(symbol, price)
        except Exception:
            pass

        direction = "+" if rate > 0 else ""
        trend = "📈 POSITIVE" if rate > 0 else "📉 NEGATIVE"
        logger.info(
            f"💰 Extreme Funding: {symbol} {trend} {direction}{rate:.4f}%"
            f"{' [REVERSAL]' if is_reversal else ''}"
        )
        await self.event_callback(event)

    async def stop(self):
        self._running = False


# ════════════════════════════════════════════
# ORDER BOOK SCANNER
# ════════════════════════════════════════════

class OrderBookScanner:
    def __init__(self, event_callback, binance_connector):
        self.event_callback = event_callback
        self.connector = binance_connector
        self._running = False

    async def start(self):
        self._running = True
        asyncio.create_task(self._scan_loop())
        logger.info("✅ Order Book Scanner started")

    async def _scan_loop(self):
        while self._running:
            try:
                symbols = await state_manager.get_symbols("binance", "futures")

                # FIX: set() tasodifiy tartibda — OI hajmi bo'yicha saralash
                # OI ma'lumoti bor symbollarni avval skanerlash
                symbol_oi = []
                for sym in symbols:
                    oi_data = await state_manager._kv_get(f"crypto:binance:{sym}:oi_current")
                    oi_val = oi_data["oi_usdt"] if oi_data else 0
                    symbol_oi.append((sym, oi_val))

                # OI bo'yicha kamayish tartibida sort qil, top 200 ni skanerlash
                symbol_oi.sort(key=lambda x: x[1], reverse=True)
                top_symbols = [s[0] for s in symbol_oi[:200]]

                for symbol in top_symbols:
                    if not self._running:
                        break
                    await self._analyze_orderbook("binance", symbol)
                    await asyncio.sleep(0.05)  # 0.1 → 0.05 (tezroq)

            except Exception as e:
                logger.debug(f"OB scan error: {e}")

            await asyncio.sleep(15)

    async def _analyze_orderbook(self, exchange: str, symbol: str):
        try:
            ob_data = await self.connector.get_orderbook(symbol, limit=50)
            if not ob_data:
                return

            ticker = await state_manager.get_ticker(exchange, symbol)
            if not ticker:
                return

            current_price = ticker["price"]
            if current_price <= 0:
                return

            bids = ob_data.get("bids", [])
            asks = ob_data.get("asks", [])
            if not bids or not asks:
                return

            bid_levels = [
                OrderBookLevel(
                    price=float(b[0]),
                    quantity=float(b[1]),
                    usdt_value=float(b[0]) * float(b[1])
                ) for b in bids
            ]
            ask_levels = [
                OrderBookLevel(
                    price=float(a[0]),
                    quantity=float(a[1]),
                    usdt_value=float(a[0]) * float(a[1])
                ) for a in asks
            ]

            buy_wall = max(bid_levels, key=lambda x: x.usdt_value) if bid_levels else None
            sell_wall = max(ask_levels, key=lambda x: x.usdt_value) if ask_levels else None

            if not buy_wall or not sell_wall:
                return

            if (buy_wall.usdt_value < settings.orderbook_wall_min_usdt and
                    sell_wall.usdt_value < settings.orderbook_wall_min_usdt):
                return

            total_bids = sum(l.usdt_value for l in bid_levels)
            total_asks = sum(l.usdt_value for l in ask_levels)

            if total_asks <= 0 or total_bids <= 0:
                return

            imbalance = total_bids / total_asks
            threshold = settings.orderbook_imbalance_ratio
            is_buy_heavy = imbalance >= threshold
            is_sell_heavy = imbalance <= (1.0 / threshold)

            if not (is_buy_heavy or is_sell_heavy):
                return

            if await state_manager.is_event_sent(exchange, symbol, "orderbook"):
                return

            buy_wall_dist = abs(current_price - buy_wall.price) / current_price * 100
            sell_wall_dist = abs(sell_wall.price - current_price) / current_price * 100

            event = OrderBookEvent(
                symbol=symbol,
                exchange=Exchange(exchange),
                buy_wall_usdt=buy_wall.usdt_value,
                sell_wall_usdt=sell_wall.usdt_value,
                buy_wall_price=buy_wall.price,
                sell_wall_price=sell_wall.price,
                buy_wall_distance_pct=buy_wall_dist,
                sell_wall_distance_pct=sell_wall_dist,
                imbalance_ratio=imbalance,
                current_price=current_price,
                timestamp=datetime.utcnow()
            )

            await state_manager.mark_event_sent(exchange, symbol, "orderbook", cooldown=120)

            bias = "BUY" if is_buy_heavy else "SELL"
            logger.info(
                f"📚 OB: {symbol} {bias} | "
                f"ratio={imbalance:.1f}x | "
                f"buy={buy_wall.usdt_value:,.0f} sell={sell_wall.usdt_value:,.0f}"
            )
            await self.event_callback(event)

        except Exception as e:
            logger.debug(f"OB analyze error {symbol}: {e}")

    async def stop(self):
        self._running = False