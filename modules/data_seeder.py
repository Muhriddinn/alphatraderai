"""
Data Seeder — CVD data uchun tarixiy trade lar
REST ban bo'lsa — seeder avtomatik to'xtaydi, WebSocket yetarli
"""
import asyncio
import aiohttp
import time
from loguru import logger
from core.rate_limiter import rate_limiter, retry_handler


class DataSeeder:
    def __init__(self):
        self.running = False
        self._last_seed = 0
        self._banned = False

    async def start(self):
        logger.info("📥 Data Seeder ishga tushdi...")
        self.running = True
        asyncio.create_task(self._seed_loop())

    async def _seed_loop(self):
        await asyncio.sleep(120)
        while self.running:
            try:
                if self._banned:
                    logger.info("📥 DataSeeder: REST ban — 1 soat kutish")
                    await asyncio.sleep(3600)
                    continue

                symbols = await self._get_symbols()
                if not symbols:
                    logger.info("📥 DataSeeder: symbol topilmadi — 1 soat kutish")
                    await asyncio.sleep(3600)
                    continue

                total = len(symbols)
                logger.info(f"📥 DataSeeder: {total} symbol uchun CVD data yuklanmoqda...")

                total_trades = 0
                for i, symbol in enumerate(symbols):
                    if self._banned:
                        logger.warning(f"⛔ DataSeeder: ban bo'ldi — {i}/{total} da to'xtatildi")
                        break
                    try:
                        trades = await self._fetch_trades(symbol)
                        if trades:
                            total_trades += len(trades)
                            await self._process_trades(symbol, trades)
                        if (i + 1) % 50 == 0:
                            logger.info(f"📥 DataSeeder: {i+1}/{total} — {total_trades} trade")
                        await asyncio.sleep(2.0)
                    except Exception:
                        continue

                self._last_seed = time.time()
                logger.info(f"✅ DataSeeder tugadi: {total} coin, {total_trades} trade")
                await asyncio.sleep(86400)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"DataSeeder loop xato: {e}")
                await asyncio.sleep(600)

    async def _get_symbols(self):
        if self._banned:
            return []
        try:
            from core.binance_connector import connector
            symbols = getattr(connector, "symbols", [])
            if symbols:
                return symbols
        except Exception:
            pass
        return []

    async def _fetch_trades(self, symbol):
        if self._banned:
            return []
        try:
            await rate_limiter.acquire(weight=5)
            async with aiohttp.ClientSession() as session:
                url = "https://fapi.binance.com/fapi/v1/trades"
                params = {"symbol": symbol, "limit": 1000}
                async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                    if resp.status == 418:
                        self._banned = True
                        return []
                    if resp.status == 429:
                        self._banned = True
                        return []
                    if resp.status == 200:
                        return await resp.json()
        except Exception:
            pass
        return []

    async def _process_trades(self, symbol, trades):
        try:
            from modules.cvd_tracker import cvd_tracker
            for t in trades:
                price = float(t.get("price", 0))
                qty = float(t.get("qty", 0))
                is_buy = not t.get("isBuyerMaker", False)
                trade_time = t.get("time", int(time.time() * 1000))
                trade_data = {
                    "symbol": symbol,
                    "price": price,
                    "qty": qty,
                    "is_buy": is_buy,
                    "time": trade_time,
                    "exchange": "binance",
                    "market_type": "futures"
                }
                await cvd_tracker.process_trade(trade_data)
        except Exception:
            pass


data_seeder = DataSeeder()
