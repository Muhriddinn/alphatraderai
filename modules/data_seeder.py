"""
Data Seeder — Barcha symbollar uchun 30 kunlik tarixiy data
Har 24 soat yangilanadi (rolling window)
"""
import asyncio
import aiohttp
import time
from loguru import logger
from core.rate_limiter import rate_limiter, retry_handler

REFRESH_INTERVAL = 24 * 60 * 60  # 24 soat


class DataSeeder:
    def __init__(self):
        self.running = False
        self._last_seed = 0

    async def start(self):
        logger.info("📥 Data Seeder ishga tushdi...")
        self.running = True
        asyncio.create_task(self._seed_loop())

    async def _seed_loop(self):
        while self.running:
            try:
                await asyncio.sleep(30)
                await self._seed_all()
                while self.running:
                    await asyncio.sleep(60)
                    if time.time() - self._last_seed >= REFRESH_INTERVAL:
                        await self._seed_all()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"DataSeeder loop xato: {e}")
                await asyncio.sleep(60)

    async def _seed_all(self):
        try:
            symbols = await self._get_all_symbols()
            if not symbols:
                try:
                    from core.binance_connector import connector
                    symbols = getattr(connector, "symbols", [])
                    if symbols:
                        logger.info(f"📥 DataSeeder: connector dan {len(symbols)} symbol olindi")
                except Exception:
                    pass
            if not symbols:
                logger.warning("DataSeeder: symbol topilmadi")
                return

            total = len(symbols)
            logger.info(f"📥 DataSeeder: {total} symbol uchun 30 kunlik data yuklanmoqda...")

            total_trades = 0
            batch_size = 20
            for start in range(0, total, batch_size):
                batch = symbols[start:start + batch_size]
                tasks = [self._seed_one_symbol(s) for s in batch]
                results = await asyncio.gather(*tasks, return_exceptions=True)
                for r in results:
                    if isinstance(r, int):
                        total_trades += r
                done = min(start + batch_size, total)
                if done % 100 == 0 or done == total:
                    logger.info(f"📥 DataSeeder: {done}/{total} — {total_trades} trade")
                await asyncio.sleep(0.5)

            self._last_seed = time.time()
            logger.info(f"✅ DataSeeder tugadi: {total} coin, {total_trades} trade")

        except Exception as e:
            logger.error(f"DataSeeder xatolik: {e}")

    async def _seed_one_symbol(self, symbol: str) -> int:
        trade_count = 0
        try:
            trades = await self._fetch_recent_trades(symbol)
            if trades:
                trade_count = len(trades)
                await self._process_trades(symbol, trades)
        except Exception:
            pass
        return trade_count

    async def _get_all_symbols(self):
        try:
            await rate_limiter.acquire()
            async with aiohttp.ClientSession() as session:
                url = "https://fapi.binance.com/fapi/v1/ticker/24hr"
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                    if resp.status == 418:
                        logger.error("⛔ DataSeeder: IP BAN (418) — 120 soniya kutish")
                        await asyncio.sleep(120)
                        return await self._get_all_symbols()
                    if resp.status == 429:
                        await retry_handler.handle_429(resp)
                        return await self._get_all_symbols()
                    if resp.status == 200:
                        data = await resp.json()
                        usdt_pairs = [d for d in data if d["symbol"].endswith("USDT")]
                        usdt_pairs.sort(key=lambda x: float(x.get("quoteVolume", 0)), reverse=True)
                        return [d["symbol"] for d in usdt_pairs]
        except Exception as e:
            logger.error(f"DataSeeder symbol xatolik: {e}")
        return []

    async def _fetch_recent_trades(self, symbol):
        try:
            await rate_limiter.acquire()
            async with aiohttp.ClientSession() as session:
                url = "https://fapi.binance.com/fapi/v1/trades"
                params = {"symbol": symbol, "limit": 1000}
                async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                    if resp.status == 418:
                        logger.warning(f"418 ban on trades for {symbol}")
                        await asyncio.sleep(120)
                        return await self._fetch_recent_trades(symbol)
                    if resp.status == 429:
                        await retry_handler.handle_429(resp)
                        return await self._fetch_recent_trades(symbol)
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
