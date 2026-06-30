"""
CRYPTO MONITOR PRO — Binance WebSocket Connector (To'liq tuzatilgan)

Tuzatishlar:
1. _stream_mark_price → price_tracker.update_price() chaqiriladi (narx "—" muammosi hal)
2. Volume baseline bug: update_volume_baseline har trade da chaqirilardi → to'xtatildi
   (bu baseline ni doimiy oshirib yuborardi, spike hech qachon aniqlanmaydi)
3. Funding poll: 30s → 60s (rate limit tejash)
4. OI poll: batching yaxshilandi
"""
import asyncio
import time
import orjson
import aiohttp
import websockets
from datetime import datetime
from typing import Callable, Optional
from loguru import logger
from tenacity import retry, stop_after_attempt, wait_exponential

from config.settings import settings
from modules.price_tracker import price_tracker
from modules.cvd_tracker import cvd_tracker
from core.models import (
    Exchange, MarketType, TradeData, LiquidationData,
    OIData, FundingData, Direction
)
from core.state_manager import state_manager
from core.rate_limiter import rate_limiter, retry_handler


BINANCE_FUTURES_WS = "wss://fstream.binance.com/stream"
BINANCE_SPOT_WS = "wss://stream.binance.com:9443/stream"
BINANCE_FUTURES_REST = "https://fapi.binance.com"
BINANCE_SPOT_REST = "https://api.binance.com"


class BinanceFuturesConnector:
    """
    Manages WebSocket connections to Binance Futures.
    Subscribes to: aggTrade, liquidation, bookDepth, markPrice
    Handles reconnection automatically.
    """

    def __init__(self, callbacks: dict[str, Callable]):
        self.callbacks = callbacks
        self.symbols: list[str] = []
        self._running = False
        self._ws_tasks: list[asyncio.Task] = []
        self._session: Optional[aiohttp.ClientSession] = None

    async def start(self):
        self._running = True
        self._session = aiohttp.ClientSession()

        await self._discover_symbols()

        self._ws_tasks.append(asyncio.create_task(self._stream_mark_price()))
        self._ws_tasks.append(asyncio.create_task(self._stream_liquidations()))
        self._ws_tasks.append(asyncio.create_task(self._stream_trades_chunked()))
        self._ws_tasks.append(asyncio.create_task(self._poll_open_interest()))
        self._ws_tasks.append(asyncio.create_task(self._poll_funding_rates()))
        self._ws_tasks.append(asyncio.create_task(self._load_volume_baselines()))

        logger.info(f"✅ Binance Futures connector started | {len(self.symbols)} symbols")

    async def stop(self):
        self._running = False
        for task in self._ws_tasks:
            task.cancel()
        if self._session:
            await self._session.close()

    async def _discover_symbols(self):
        """Fetch all active USDT perpetual futures (with file cache)"""
        import os, json
        os.makedirs("logs", exist_ok=True)
        cache_file = "logs/exchange_info.json"
        data = None

        for attempt in range(3):
            try:
                await rate_limiter.acquire(weight=1)
                timeout = aiohttp.ClientTimeout(total=15)
                async with self._session.get(
                    f"{BINANCE_FUTURES_REST}/fapi/v1/exchangeInfo",
                    timeout=timeout
                ) as resp:
                    if resp.status == 418:
                        logger.error("⛔ Binance IP BAN (418) — 120 soniya kutish...")
                        await asyncio.sleep(120)
                        continue
                    if resp.status == 429:
                        await retry_handler.handle_429(resp)
                        continue
                    if resp.status == 200:
                        data = await resp.json()
                        with open(cache_file, "w") as f:
                            json.dump(data, f)
                        logger.info("📥 exchangeInfo cached to file")
                        break
                    else:
                        logger.warning(f"exchangeInfo HTTP {resp.status}")
            except asyncio.TimeoutError:
                logger.warning(f"exchangeInfo timeout (attempt {attempt+1}/3)")
                await asyncio.sleep(2)
            except Exception as e:
                logger.warning(f"exchangeInfo API xato (attempt {attempt+1}/3): {e}")
                await asyncio.sleep(2)

        if data is None:
            try:
                with open(cache_file, "r") as f:
                    data = json.load(f)
                logger.info("📂 exchangeInfo loaded from cache")
            except Exception as e:
                logger.error(f"exchangeInfo cache ham yo'q: {e}")
                self.symbols = ["BTCUSDT", "ETHUSDT", "BNBUSDT"]
                for sym in self.symbols:
                    await state_manager.add_symbol("binance", sym, "futures")
                return

        self.symbols = [
            s["symbol"] for s in data["symbols"]
            if s["quoteAsset"] == "USDT"
            and s["status"] == "TRADING"
            and s["contractType"] == "PERPETUAL"
        ]
        for sym in self.symbols:
            await state_manager.add_symbol("binance", sym, "futures")

        logger.info(f"📊 Discovered {len(self.symbols)} Binance Futures symbols")

        if "symbols_ready" in self.callbacks:
            await self.callbacks["symbols_ready"](self.symbols)

    async def _stream_liquidations(self):
        """Subscribe to all-market liquidation orders stream."""
        url = f"{BINANCE_FUTURES_WS}?streams=!forceOrder@arr"
        retry_count = 0
        base_delay = settings.ws_reconnect_delay

        while self._running:
            try:
                async with websockets.connect(
                    url,
                    ping_interval=20,
                    ping_timeout=10,
                    close_timeout=5,
                    max_size=10 * 1024 * 1024
                ) as ws:
                    retry_count = 0
                    logger.info("🔌 Liquidation stream connected")
                    async for raw in ws:
                        if not self._running:
                            break
                        await self._handle_liquidation(orjson.loads(raw))
                        await state_manager.increment_stat("ws_messages")

            except Exception as e:
                retry_count += 1
                delay = min(base_delay * (2 ** min(retry_count, 6)), 300)
                logger.warning(f"Liquidation WS disconnected (retry {retry_count}, {delay}s): {e}")
                if self._running:
                    await asyncio.sleep(delay)

    async def _handle_liquidation(self, data: dict):
        try:
            stream = data.get("data", data)
            if stream.get("e") != "forceOrder":
                return

            order = stream["o"]
            symbol = order["s"]
            side = order["S"]
            price = float(order["p"])
            qty = float(order["q"])
            usdt_value = price * qty

            direction = Direction.BUY if side == "BUY" else Direction.SELL

            liq = LiquidationData(
                symbol=symbol,
                exchange=Exchange.BINANCE,
                side=direction,
                quantity=qty,
                price=price,
                usdt_value=usdt_value,
                timestamp=datetime.utcnow()
            )

            await state_manager.add_liquidation(
                "binance", symbol, side, usdt_value,
                datetime.utcnow().timestamp()
            )

            if "liquidation" in self.callbacks:
                await self.callbacks["liquidation"](liq)

        except Exception as e:
            logger.debug(f"Liquidation parse error: {e}")

    async def _stream_trades_chunked(self):
        """Stream aggTrades for all symbols in chunks of 150."""
        chunk_size = 150
        chunks = [
            self.symbols[i:i + chunk_size]
            for i in range(0, len(self.symbols), chunk_size)
        ]

        tasks = [
            asyncio.create_task(self._stream_trades_chunk(chunk, idx))
            for idx, chunk in enumerate(chunks)
        ]
        await asyncio.gather(*tasks, return_exceptions=True)

    async def _stream_trades_chunk(self, symbols: list[str], chunk_id: int):
        streams = "/".join(f"{s.lower()}@aggTrade" for s in symbols)
        url = f"{BINANCE_FUTURES_WS}?streams={streams}"
        retry_count = 0
        base_delay = settings.ws_reconnect_delay

        while self._running:
            try:
                async with websockets.connect(
                    url,
                    ping_interval=20,
                    ping_timeout=10,
                    close_timeout=5,
                    max_size=10 * 1024 * 1024
                ) as ws:
                    retry_count = 0
                    logger.info(f"🔌 Trade stream chunk {chunk_id} connected ({len(symbols)} symbols)")
                    async for raw in ws:
                        if not self._running:
                            break
                        await state_manager.increment_stat("ws_messages")
                        await self._handle_trade(orjson.loads(raw))

            except Exception as e:
                retry_count += 1
                delay = min(base_delay * (2 ** min(retry_count, 6)), 300)
                logger.warning(f"Trade stream {chunk_id} disconnected (retry {retry_count}, {delay}s): {e}")
                if self._running:
                    await asyncio.sleep(delay)

    async def _handle_trade(self, data: dict):
        try:
            trade_data = data.get("data", data)
            if trade_data.get("e") != "aggTrade":
                return

            symbol = trade_data["s"]
            price = float(trade_data["p"])
            qty = float(trade_data["q"])
            usdt_value = price * qty
            is_maker = trade_data["m"]
            direction = Direction.SELL if is_maker else Direction.BUY

            trade = TradeData(
                symbol=symbol,
                exchange=Exchange.BINANCE,
                price=price,
                quantity=qty,
                usdt_value=usdt_value,
                side=direction,
                is_maker=is_maker,
                timestamp=datetime.fromtimestamp(trade_data["T"] / 1000)
            )

            # Whale trade tekshirish
            if usdt_value >= settings.whale_order_min_usdt:
                await state_manager.add_whale_trade(
                    "binance", symbol,
                    direction.value, usdt_value,
                    datetime.utcnow().timestamp()
                )
                if "whale_trade" in self.callbacks:
                    await self.callbacks["whale_trade"](trade)

            # ─── BUG TO'G'IRLANDI ───
            # Eski kod: har trade da update_volume_baseline chaqirilardi
            # Bu baseline ni doimiy yangilab, spike aniqlanmasdi
            # Endi faqat volume_scanner o'zi baseline ni boshqaradi
            # (quyidagi qator o'chirildi):
            # await state_manager.update_volume_baseline("binance", symbol, usdt_value)

            await cvd_tracker.process_trade(trade)

            if "trade" in self.callbacks:
                await self.callbacks["trade"](trade)

        except Exception as e:
            logger.warning(f"Trade parse error: {e}")

    async def _poll_open_interest(self):
        """Poll OI for all symbols every 60 seconds via REST"""
        await asyncio.sleep(300)  # Bootstrap tugashini kutish (5 daqiqa)
        while self._running:
            try:
                tasks = [
                    self._fetch_oi(symbol)
                    for symbol in self.symbols[:300]
                ]
                for i in range(0, len(tasks), 50):
                    batch = tasks[i:i+50]
                    await asyncio.gather(*batch, return_exceptions=True)
                    await asyncio.sleep(1.0)

            except Exception as e:
                logger.error(f"OI poll error: {e}")

            await asyncio.sleep(60)

    async def _fetch_oi(self, symbol: str):
        try:
            await rate_limiter.acquire(weight=1)
            url = f"{BINANCE_FUTURES_REST}/fapi/v1/openInterest"
            async with self._session.get(url, params={"symbol": symbol}) as resp:
                if resp.status == 418:
                    return None
                if resp.status == 429:
                    await retry_handler.handle_429(resp)
                    return await self._fetch_oi(symbol)
                if resp.status == 200:
                    data = await resp.json()
                    oi = float(data["openInterest"])

                    ticker = await state_manager.get_ticker("binance", symbol)
                    price = ticker["price"] if ticker else 0

                    # FIX: price=0 bo'lsa REST dan narxni ol
                    if price <= 0:
                        try:
                            await rate_limiter.acquire(weight=1)
                            price_url = f"{BINANCE_FUTURES_REST}/fapi/v1/ticker/price"
                            async with self._session.get(price_url, params={"symbol": symbol}) as pr:
                                if pr.status == 200:
                                    pd = await pr.json()
                                    price = float(pd.get("price", 0))
                                    if price > 0:
                                        await state_manager.set_ticker(
                                            "binance", symbol, {"price": price, "ts": 0}
                                        )
                                        price_tracker.update_price(symbol, price)
                        except Exception:
                            pass

                    # price hali ham 0 bo'lsa — bu symbol uchun OI skip
                    if price <= 0:
                        return

                    oi_usdt = oi * price

                    await state_manager.set_oi(
                        "binance", symbol, oi_usdt,
                        datetime.utcnow().timestamp()
                    )

                    oi_data = OIData(
                        symbol=symbol,
                        exchange=Exchange.BINANCE,
                        open_interest=oi,
                        open_interest_usdt=oi_usdt,
                        timestamp=datetime.utcnow()
                    )

                    if "oi_update" in self.callbacks:
                        await self.callbacks["oi_update"](oi_data)

        except Exception as e:
            logger.debug(f"OI fetch failed {symbol}: {e}")

    async def _poll_funding_rates(self):
        """Poll funding rates every 60 seconds"""
        while self._running:
            try:
                await rate_limiter.acquire(weight=1)
                url = f"{BINANCE_FUTURES_REST}/fapi/v1/premiumIndex"
                async with self._session.get(url) as resp:
                    if resp.status == 418:
                        logger.warning("⛔ Funding poll: IP BAN — 60 daqiqa kutish")
                        await asyncio.sleep(3600)
                        continue
                    if resp.status == 429:
                        await retry_handler.handle_429(resp)
                        continue
                    if resp.status == 200:
                        items = await resp.json()
                        for item in items:
                            symbol = item.get("symbol", "")
                            if not symbol.endswith("USDT"):
                                continue
                            rate = float(item.get("lastFundingRate", 0))
                            next_time = int(item.get("nextFundingTime", 0)) / 1000

                            await state_manager.set_funding(
                                "binance", symbol, rate, next_time
                            )

                            funding = FundingData(
                                symbol=symbol,
                                exchange=Exchange.BINANCE,
                                funding_rate=rate,
                                next_funding_time=datetime.fromtimestamp(next_time),
                                timestamp=datetime.utcnow()
                            )

                            if "funding_update" in self.callbacks:
                                await self.callbacks["funding_update"](funding)

            except Exception as e:
                logger.error(f"Funding poll error: {e}")

            await asyncio.sleep(60)  # oldin 30s edi

    async def _stream_mark_price(self):
        """
        Stream mark price for all symbols (price + funding updates)

        ─── BUG TO'G'IRLANDI ───
        Eski kod: faqat state_manager.set_ticker() chaqirilardi
        price_tracker.update_price() CHAQIRILMAYOTGAN EDI
        Shuning uchun Telegram signalida narx "—" ko'rinardi

        To'g'ri kod: price_tracker.update_price() ham chaqiriladi
        """
        url = f"{BINANCE_FUTURES_WS}?streams=!markPrice@arr@1s"
        retry_count = 0
        base_delay = settings.ws_reconnect_delay

        while self._running:
            try:
                async with websockets.connect(
                    url,
                    ping_interval=20,
                    ping_timeout=10,
                    close_timeout=5,
                    max_size=10 * 1024 * 1024
                ) as ws:
                    retry_count = 0
                    logger.info("🔌 Mark price stream connected")
                    async for raw in ws:
                        if not self._running:
                            break
                        data = orjson.loads(raw)
                        await state_manager.increment_stat("ws_messages")
                        stream_data = data.get("data", [])
                        for item in stream_data:
                            if item.get("e") == "markPriceUpdate":
                                symbol = item["s"]
                                price = float(item["p"])

                                # State manager ga saqla (OI va boshqa modullar uchun)
                                await state_manager.set_ticker(
                                    "binance", symbol,
                                    {"price": price, "ts": item["T"]}
                                )

                                # ✅ Price tracker ga ham uzat (narx "—" muammosi hal!)
                                price_tracker.update_price(symbol, price)

            except Exception as e:
                retry_count += 1
                delay = min(base_delay * (2 ** min(retry_count, 6)), 300)
                logger.warning(f"Mark price stream disconnected (retry {retry_count}, {delay}s): {e}")
                if self._running:
                    await asyncio.sleep(delay)

    async def get_orderbook(self, symbol: str, limit: int = 50) -> Optional[dict]:
        """Fetch order book snapshot via REST"""
        try:
            await rate_limiter.acquire(weight=5)
            url = f"{BINANCE_FUTURES_REST}/fapi/v1/depth"
            async with self._session.get(url, params={"symbol": symbol, "limit": limit}) as resp:
                if resp.status in (418, 429):
                    if resp.status == 418:
                        await asyncio.sleep(120)
                    else:
                        await retry_handler.handle_429(resp)
                    return await self.get_orderbook(symbol, limit)
                if resp.status == 200:
                    return await resp.json()
        except Exception as e:
            logger.debug(f"Orderbook fetch failed {symbol}: {e}")
        return None

    async def _load_volume_baselines(self):
        """
        Bootstrap — faqat birinchi ishga tushirishda.
        Agar 418 ban bo'lsa — to'xtatiladi, WebSocket yetarli.
        """
        logger.info("📊 Bootstrap: barcha coinlar uchun ma'lumot yuklanmoqda...")
        loaded = 0
        errors = 0
        banned = False
        total = len(self.symbols)
        batch_size = 2

        for start in range(0, total, batch_size):
            if banned:
                break
            batch = self.symbols[start:start + batch_size]
            tasks = [self._bootstrap_one(s) for s in batch]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for r in results:
                if r is True:
                    loaded += 1
                elif r == "BANNED":
                    banned = True
                    break
                elif isinstance(r, Exception):
                    errors += 1
                else:
                    errors += 1
            if not banned and (start // batch_size) % 10 == 0:
                logger.info(f"📊 Bootstrap: {start + len(batch)}/{total} yuklandi...")
            if not banned:
                await asyncio.sleep(30.0)

        if banned:
            logger.warning(f"⛔ Bootstrap to'xtatildi (IP ban). {loaded}/{total} symbol yuklandi.")
            logger.info("ℹ️ Bot WebSocket orqali ishlaydi — REST kerak emas!")
        else:
            logger.info(f"✅ Bootstrap tugadi: {loaded}/{total} symbol ({errors} errors)")

    async def _bootstrap_one(self, symbol: str):
        """Bitta coin uchun barcha bootstrap ma'lumotini yuklash"""
        try:
            await rate_limiter.acquire(weight=50)
            overall_timeout = aiohttp.ClientTimeout(total=15)
            connector = aiohttp.TCPConnector(limit=8, force_close=True)
            async with aiohttp.ClientSession(timeout=overall_timeout, connector=connector) as s:
                urls = {
                    "ticker": f"{BINANCE_FUTURES_REST}/fapi/v1/ticker/24hr?symbol={symbol}",
                    "funding": f"{BINANCE_FUTURES_REST}/fapi/v1/premiumIndex?symbol={symbol}",
                    "oi": f"{BINANCE_FUTURES_REST}/fapi/v1/openInterest?symbol={symbol}",
                    "k1m": f"{BINANCE_FUTURES_REST}/fapi/v1/klines?symbol={symbol}&interval=1m&limit=240",
                    "k5m": f"{BINANCE_FUTURES_REST}/fapi/v1/klines?symbol={symbol}&interval=5m&limit=72",
                    "k15m": f"{BINANCE_FUTURES_REST}/fapi/v1/klines?symbol={symbol}&interval=15m&limit=96",
                    "k1h": f"{BINANCE_FUTURES_REST}/fapi/v1/klines?symbol={symbol}&interval=1h&limit=48",
                    "depth": f"{BINANCE_FUTURES_REST}/fapi/v1/depth?symbol={symbol}&limit=20",
                }
                async def fetch(name, url):
                    try:
                        async with s.get(url, timeout=aiohttp.ClientTimeout(total=10)) as r:
                            if r.status == 418:
                                return ("BANNED", None)
                            if r.status == 200:
                                return (name, await r.json())
                    except: pass
                    return (name, None)

                task_list = [fetch(name, url) for name, url in urls.items()]
                results = await asyncio.gather(*task_list)
                res = dict(results)

                if "BANNED" in res:
                    return "BANNED"

                ticker = res.get("ticker") or {}
                funding_data = res.get("funding") or {}
                oi_data = res.get("oi") or {}
                k1m = res.get("k1m") or []
                k5m = res.get("k5m") or []
                k15m = res.get("k15m") or []
                k1h = res.get("k1h") or []
                depth = res.get("depth") or {}

                # Price
                price = float(ticker.get("lastPrice", 0) or 0) or float(funding_data.get("markPrice", 0) or 0)
                if price > 0:
                    await state_manager.set_ticker("binance", symbol, {"price": price, "ts": 0})
                    price_tracker.update_price(symbol, price)

                # 24h volume + change
                vol_24h = float(ticker.get("quoteVolume", 0) or 0)
                change_24h = float(ticker.get("priceChangePercent", 0) or 0)

                # OI
                oi_qty = float(oi_data.get("openInterest", 0) or 0)
                oi_usdt = oi_qty * price if price > 0 else 0

                # Funding
                funding_rate = float(funding_data.get("lastFundingRate", 0) or 0)
                next_funding = funding_data.get("nextFundingTime", 0)

                # Volume windows (scanner uchun)
                key = f"binance:{symbol}:volume_window"
                now_ts = time.time()
                window = []
                for k in k1m:
                    close_ts = float(k[6]) / 1000
                    quote_vol = float(k[7])
                    if quote_vol > 0:
                        window.append({"usdt": quote_vol, "ts": close_ts})
                window = [v for v in window if v["ts"] >= now_ts - 14400]
                await state_manager.set_volume_data(key, window)

                # Multi-timeframe volumes (extra ga saqlaymiz)
                extra = {
                    "price": price,
                    "price_change_24h": change_24h,
                    "volume_24h": vol_24h,
                    "oi_usdt": oi_usdt,
                    "funding_rate": funding_rate * 100,  # %
                    "vol_1m": sum(float(k[7]) for k in k1m[-1:]) if k1m else 0,
                    "vol_5m": sum(float(k[7]) for k in k5m[-1:]) if k5m else 0,
                    "vol_15m": sum(float(k[7]) for k in k15m[-1:]) if k15m else 0,
                    "vol_1h": sum(float(k[7]) for k in k1h[-1:]) if k1h else 0,
                }
                # Price changes from klines
                if len(k5m) >= 2:
                    p_now = float(k5m[-1][4])
                    p_prev = float(k5m[0][1])
                    if p_prev > 0:
                        extra["price_change_5m"] = (p_now - p_prev) / p_prev * 100
                if len(k1h) >= 2:
                    p_now = float(k1h[-1][4])
                    p_prev = float(k1h[0][1])
                    if p_prev > 0:
                        extra["price_change_1h"] = (p_now - p_prev) / p_prev * 100

                # OB walls
                bids = depth.get("bids", [])
                asks = depth.get("asks", [])
                buy_walls = []
                for p_str, q_str in bids:
                    usdt = float(p_str) * float(q_str)
                    if usdt >= 50_000:
                        buy_walls.append({"usdt": usdt, "price": float(p_str), "dist_pct": (float(p_str) - price) / price * 100 if price > 0 else 0})
                sell_walls = []
                for p_str, q_str in asks:
                    usdt = float(p_str) * float(q_str)
                    if usdt >= 50_000:
                        sell_walls.append({"usdt": usdt, "price": float(p_str), "dist_pct": (float(p_str) - price) / price * 100 if price > 0 else 0})
                buy_walls.sort(key=lambda x: x["usdt"], reverse=True)
                sell_walls.sort(key=lambda x: x["usdt"], reverse=True)
                extra["ob_buy_walls"] = buy_walls[:3]
                extra["ob_sell_walls"] = sell_walls[:3]
                total_buy = sum(w["usdt"] for w in buy_walls[:3])
                total_sell = sum(w["usdt"] for w in sell_walls[:3])
                extra["ob_imbalance"] = round(total_buy / total_sell, 2) if total_sell > 0 else 1.0

                # Bootstrap extra ma'lumotini state_manager ga saqlaymiz
                await state_manager.set_bootstrap(symbol, extra)

                return True
        except Exception as e:
            logger.debug(f"Bootstrap xato {symbol}: {e}")
            return False

    async def get_klines(self, symbol: str, interval: str = "1m", limit: int = 30) -> list:
        """Fetch klines/candlestick data for volume baseline"""
        try:
            await rate_limiter.acquire(weight=1)
            url = f"{BINANCE_FUTURES_REST}/fapi/v1/klines"
            params = {"symbol": symbol, "interval": interval, "limit": limit}
            async with self._session.get(url, params=params) as resp:
                if resp.status in (418, 429):
                    if resp.status == 418:
                        await asyncio.sleep(120)
                    else:
                        await retry_handler.handle_429(resp)
                    return await self.get_klines(symbol, interval, limit)
                if resp.status == 200:
                    return await resp.json()
        except Exception as e:
            logger.debug(f"Klines fetch failed {symbol}: {e}")
        return []
