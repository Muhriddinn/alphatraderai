"""
CRYPTO MONITOR PRO — Listing/Delisting Detector
Yangi listing va delisting avtomatik aniqlanadi
"""
import asyncio
import aiohttp
from datetime import datetime
from loguru import logger
from core.models import Exchange, MarketType


BINANCE_FUTURES_REST = "https://fapi.binance.com"
BINANCE_SPOT_REST = "https://api.binance.com"


class ListingDetector:
    """
    Har 60 soniyada Binance API dan symbollar ro'yxatini oladi.
    Yangi symbol = LISTING
    O'chgan symbol = DELISTING
    """

    def __init__(self, alert_callback):
        self.alert_callback = alert_callback
        self._known_futures: set[str] = set()
        self._known_spot: set[str] = set()
        self._session = None
        self._running = False
        self._initialized = False

    async def start(self):
        self._running = True
        self._session = aiohttp.ClientSession()
        # Birinchi marta yuklash — baseline
        await self._load_initial()
        asyncio.create_task(self._monitor_loop())
        logger.info("✅ Listing Detector started")

    async def _load_initial(self):
        """Dastlabki symbollar ro'yxatini yuklash"""
        try:
            # Futures
            async with self._session.get(
                f"{BINANCE_FUTURES_REST}/fapi/v1/exchangeInfo"
            ) as resp:
                data = await resp.json()
                self._known_futures = {
                    s["symbol"] for s in data["symbols"]
                    if s["status"] == "TRADING"
                    and s["contractType"] == "PERPETUAL"
                }

            # Spot
            async with self._session.get(
                f"{BINANCE_SPOT_REST}/api/v3/exchangeInfo"
            ) as resp:
                data = await resp.json()
                self._known_spot = {
                    s["symbol"] for s in data["symbols"]
                    if s["status"] == "TRADING"
                    and s["symbol"].endswith("USDT")
                }

            self._initialized = True
            logger.info(
                f"📋 Listing detector: "
                f"{len(self._known_futures)} futures, "
                f"{len(self._known_spot)} spot kuzatilmoqda"
            )
        except Exception as e:
            logger.error(f"Listing init error: {e}")

    async def _monitor_loop(self):
        """Har 60 soniyada tekshiradi"""
        while self._running:
            await asyncio.sleep(60)
            if not self._initialized:
                continue
            try:
                await self._check_futures()
                await self._check_spot()
            except Exception as e:
                logger.debug(f"Listing check error: {e}")

    async def _check_futures(self):
        async with self._session.get(
            f"{BINANCE_FUTURES_REST}/fapi/v1/exchangeInfo"
        ) as resp:
            data = await resp.json()
            current = {
                s["symbol"] for s in data["symbols"]
                if s["status"] == "TRADING"
                and s["contractType"] == "PERPETUAL"
            }

        # Yangi listing
        new_symbols = current - self._known_futures
        for symbol in new_symbols:
            logger.info(f"🚀 YANGI FUTURES LISTING: {symbol}")
            await self.alert_callback({
                "symbol": symbol,
                "exchange": "Binance",
                "market_type": "Futures",
                "is_listing": True,
                "time": datetime.utcnow().strftime("%H:%M UTC")
            })

        # Delisting
        removed = self._known_futures - current
        for symbol in removed:
            logger.info(f"⚠️ FUTURES DELISTING: {symbol}")
            await self.alert_callback({
                "symbol": symbol,
                "exchange": "Binance",
                "market_type": "Futures",
                "is_listing": False,
                "time": datetime.utcnow().strftime("%H:%M UTC")
            })

        self._known_futures = current

    async def _check_spot(self):
        async with self._session.get(
            f"{BINANCE_SPOT_REST}/api/v3/exchangeInfo"
        ) as resp:
            data = await resp.json()
            current = {
                s["symbol"] for s in data["symbols"]
                if s["status"] == "TRADING"
                and s["symbol"].endswith("USDT")
            }

        new_symbols = current - self._known_spot
        for symbol in new_symbols:
            logger.info(f"🚀 YANGI SPOT LISTING: {symbol}")
            await self.alert_callback({
                "symbol": symbol,
                "exchange": "Binance",
                "market_type": "Spot",
                "is_listing": True,
                "time": datetime.utcnow().strftime("%H:%M UTC")
            })

        removed = self._known_spot - current
        for symbol in removed:
            logger.info(f"⚠️ SPOT DELISTING: {symbol}")
            await self.alert_callback({
                "symbol": symbol,
                "exchange": "Binance",
                "market_type": "Spot",
                "is_listing": False,
                "time": datetime.utcnow().strftime("%H:%M UTC")
            })

        self._known_spot = current

    async def stop(self):
        self._running = False
        if self._session:
            await self._session.close()
