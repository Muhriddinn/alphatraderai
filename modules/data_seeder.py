"""
Data Seeder — Bot ishga tushganda tarixiy ma'lumotlarni yuklaydi
CVD, Liquidation, Volume uchun 24 soatlik data
"""
import asyncio
import aiohttp
import time
from loguru import logger
from datetime import datetime


class DataSeeder:
    def __init__(self):
        self.running = False

    async def start(self):
        logger.info("📥 Data Seeder: tarixiy ma'lumot yuklanmoqda...")
        self.running = True
        asyncio.create_task(self._seed_data())

    async def _seed_data(self):
        try:
            await asyncio.sleep(30)

            symbols = await self._get_top_symbols()
            if not symbols:
                logger.warning("DataSeeder: symbol topilmadi")
                return

            logger.info(f"📥 DataSeeder: {len(symbols)} symbol uchun data yuklanmoqda...")

            total_trades = 0
            for i, symbol in enumerate(symbols[:50]):
                try:
                    trades = await self._fetch_recent_trades(symbol)
                    if trades:
                        total_trades += len(trades)
                        await self._process_trades(symbol, trades)
                    if (i + 1) % 10 == 0:
                        logger.info(f"📥 DataSeeder: {i+1}/50 symbol — {total_trades} trade")
                    await asyncio.sleep(0.1)
                except Exception as e:
                    continue

            logger.info(f"✅ DataSeeder: tayyor! {total_trades} trade yuklandi")

        except Exception as e:
            logger.error(f"DataSeeder xatolik: {e}")

    async def _get_top_symbols(self):
        try:
            async with aiohttp.ClientSession() as session:
                url = "https://fapi.binance.com/fapi/v1/ticker/24hr"
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
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
            async with aiohttp.ClientSession() as session:
                url = f"https://fapi.binance.com/fapi/v1/trades"
                params = {"symbol": symbol, "limit": 1000}
                async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                    if resp.status == 200:
                        return await resp.json()
        except Exception:
            pass
        return []

    async def _process_trades(self, symbol, trades):
        try:
            from modules.cvd_tracker import cvd_tracker
            from modules.price_tracker import price_tracker

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
