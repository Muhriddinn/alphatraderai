"""
CRYPTO MONITOR PRO — Whale, Funding & Order Book Scanners

Tuzatishlar:
- Funding cooldown: 3600s (1 soat) — spam yo'q
- Whale cooldown: 60s
- OrderBook cooldown: 120s
- Ball tizimi olib tashlandi
"""
import asyncio
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
        now = trade.timestamp if trade.timestamp else datetime.utcnow()

        self._trades[key].append({
            "usdt": trade.usdt_value,
            "side": trade.side,
            "ts": now.timestamp(),
        })

    async def _burst_check_loop(self):
        """Har 3 sekundda burst tekshirish — turli vaqt oynalari"""
        WINDOWS = [
            ("60s",  60),
            ("3m",   180),
            ("10m",  600),
            ("15m",  900),
        ]

        while self._running:
            now = datetime.utcnow().timestamp()

            for key, trades in list(self._trades.items()):
                if not trades:
                    continue

                exchange, symbol = key.split(":", 1)

                # Eski tradelarni tozalash (15 daqiqadan eski)
                self._trades[key] = [t for t in trades if now - t["ts"] <= 900]
                trades = self._trades[key]

                if not trades:
                    continue

                ticker = self._ticker_24h.get(symbol, {})
                volume_24h = ticker.get("volume_24h", 0)
                price_change_pct = ticker.get("price_change_pct", 0)

                for win_label, win_sec in WINDOWS:
                    cutoff = now - win_sec
                    window_trades = [t for t in trades if t["ts"] >= cutoff]
                    if not window_trades:
                        continue

                    total_buy = sum(t["usdt"] for t in window_trades if t["side"] == Direction.BUY)
                    total_sell = sum(t["usdt"] for t in window_trades if t["side"] == Direction.SELL)
                    total = total_buy + total_sell

                    # Minimum threshold
                    if total < 30_000:
                        continue

                    # 24h hajmning necha foizi
                    vol_pct = (total / volume_24h * 100) if volume_24h > 0 else 0

                    # Signal uchun: 1% 24h hajm YOKI minimum $30K
                    if volume_24h > 0 and vol_pct < 1.0 and total < 30_000:
                        continue

                    direction = Direction.BUY if total_buy >= total_sell else Direction.SELL

                    event_key = f"whale_{win_label}"
                    if await state_manager.is_event_sent(exchange, symbol, event_key):
                        continue

                    # Real elapsed (birinchi trade dan beri)
                    oldest = min(t["ts"] for t in window_trades)
                    elapsed = int(now - oldest)

                    # Token quantity (coin miqdori)
                    ticker = self._ticker_24h.get(symbol, {})
                    last_price = ticker.get("last_price", 0) or 0
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

                    cooldown = win_sec  # Oyna uzunligi = cooldown
                    await state_manager.mark_event_sent(exchange, symbol, event_key, cooldown=cooldown)
                    await state_manager.increment_stat("whale_events")

                    dir_str = "BUY 🟢" if direction == Direction.BUY else "SELL 🔴"
                    logger.info(
                        f"🐋 Whale [{win_label}]: {symbol} {dir_str} "
                        f"${total:,.0f} ({vol_pct:.0f}% of 24h) | "
                        f"{elapsed}s ichida"
                    )
                    await self.event_callback(event)
                    break  # Eng kichik oynani topgach chiqamiz

            await asyncio.sleep(3)

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
                    oi_key = f"crypto:binance:{sym}:oi_current"
                    oi_data = state_manager._data.get(oi_key)
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