"""
CRYPTO MONITOR PRO — Main Entry Point (Yangilangan)

Yangiliklar:
- DigestScheduler qo'shildi (H1 + H4 hisobot)
- OrderBookWallTracker qo'shildi (ob_tracker.py)
- Volume scanner eski OBScanner o'rniga yangi ob_tracker ishlatadi
"""
import asyncio
import sys
import os
import uvicorn
from loguru import logger

from config.settings import settings
from core.state_manager import state_manager
from core.binance_connector import BinanceFuturesConnector
from core.alert_engine import AlertEngine
from core.models import MarketAlert

from modules.volume_scanner import VolumeScanner
from modules.oi_scanner import OIScanner
from modules.liquidation_scanner import LiquidationScanner, liq_aggregator
from modules.scanners import WhaleScanner, FundingScanner
from modules.top_report import TopReportModule
from modules.price_tracker import price_tracker, PriceChangeScanner
from modules.cvd_tracker import cvd_tracker
from modules.listing_detector import ListingDetector

# YANGI MODULLAR
from modules.ob_tracker import OrderBookWallTracker
from modules.digest_scheduler import DigestScheduler

# 7 TA YANGI MODULE
from modules.fear_greed import fear_greed_index
from modules.long_short_ratio import long_short_ratio
from modules.funding_history import funding_rate_history
from modules.liquidation_heatmap import liquidation_heatmap
from modules.correlation_matrix import correlation_matrix
from modules.volume_profile import volume_profile
from modules.top_trader_sentiment import top_trader_sentiment

# ML PREDICTION
from modules.data_collector import data_collector
from modules.prediction_engine import prediction_engine
from modules.bookmap_engine import bookmap_engine
from modules.pre_signal_detector import pre_signal_detector
from modules.data_seeder import data_seeder

from bot.telegram_bot import bot
from db.models import init_db
from admin.dashboard import app as admin_app

logger.remove()
logger.add(
    sys.stdout,
    format="<green>{time:HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan> | {message}",
    level=settings.log_level,
    colorize=True,
    serialize=False,
    enqueue=True,
)
os.makedirs("logs", exist_ok=True)
logger.add("logs/crypto_monitor.log", rotation="50 MB", retention="7 days", level="DEBUG", enqueue=True)


class CryptoMonitorApp:
    def __init__(self):
        self.running = False

        self.alert_engine = AlertEngine(alert_callback=self._on_alert)

        self.volume_scanner = VolumeScanner(event_callback=self.alert_engine.on_volume_event)
        self.oi_scanner = OIScanner(event_callback=self.alert_engine.on_oi_event)
        self.liq_scanner = LiquidationScanner(event_callback=self.alert_engine.on_liq_event)
        self.whale_scanner = WhaleScanner(event_callback=self.alert_engine.on_whale_event)
        self.funding_scanner = FundingScanner(event_callback=self.alert_engine.on_funding_event)

        self.connector = BinanceFuturesConnector(callbacks={
            "trade": self._on_trade,
            "whale_trade": self._on_whale_trade,
            "liquidation": self._on_liquidation,
            "oi_update": self._on_oi_update,
            "funding_update": self._on_funding_update,
            # Symbollar ro'yxati tayyor bo'lganda ob_tracker ga beramiz
            "symbols_ready": self._on_symbols_ready,
        })

        # YANGI: Order Book Wall Tracker (eski OrderBookScanner o'rniga)
        self.ob_tracker = OrderBookWallTracker(
            event_callback=self.alert_engine.on_orderbook_event
        )

        # MUHIM: alert_engine har bir signalga hajm va order-book
        # kontekstini qo'shishi uchun shu ikki instansga ishora kerak.
        # Bu ulanmasdan extra hajm/OB blok signalda hech qachon
        # ko'rinmaydi (avval shu joy yo'q edi).
        self.alert_engine.volume_scanner = self.volume_scanner
        self.alert_engine.ob_tracker = self.ob_tracker

        # Real likvidatsiya klasterlari uchun aggregator
        self.alert_engine.liq_aggregator = liq_aggregator

        # YANGI: Digest Scheduler
        self.digest_scheduler = DigestScheduler(
            broadcast_callback=self._send_broadcast,
            symbols=[]  # start() da to'ldiriladi
        )

        self.top_report = TopReportModule(bot_send_callback=self._send_broadcast)
        self.price_change_scanner = PriceChangeScanner(
            event_callback=self._on_price_change
        )
        self.listing_detector = ListingDetector(
            alert_callback=self._on_listing
        )

    async def _on_symbols_ready(self, symbols: list[str]):
        """Connector symbollar ro'yxatini olganda ob_tracker, digest va bookmap ga beradi"""
        logger.info(f"📋 {len(symbols)} symbol tayyor → OB tracker, Digest va Bookmap yangilanmoqda")
        self.ob_tracker.update_symbols(symbols)
        self.digest_scheduler.update_symbols(symbols)
        # Bookmap WebSocket — top 20 symbol
        bookmap_engine.start_ws_for_symbols(symbols[:20])

    async def _on_trade(self, trade):
        await self.volume_scanner.process_trade(trade)
        await cvd_tracker.process_trade(trade)
        # Barcha trade'larni whale scanner'ga yuborish — burst aniqlash uchun
        await self.whale_scanner.process_whale_trade(trade)

    async def _on_whale_trade(self, trade):
        await self.whale_scanner.process_whale_trade(trade)

    async def _on_liquidation(self, liq):
        await self.liq_scanner.process_liquidation(liq)

    async def _on_oi_update(self, oi_data):
        await self.oi_scanner.process_oi_update(oi_data)

    async def _on_funding_update(self, funding):
        await self.funding_scanner.process_funding_update(funding)

    async def _on_price_change(self, symbol: str, changes: dict, label: str):
        from telegram.constants import ParseMode
        from db.models import AsyncSessionFactory, User
        from sqlalchemy import select
        c1m = changes.get("change_1m", 0)
        c5m = changes.get("change_5m", 0)
        c1h = changes.get("change_1h", 0)
        price = changes.get("current", 0)
        direction = "📈" if c1m > 0 else "📉"
        price_str = "{:,.4f}".format(price) if price < 100 else "{:,.2f}".format(price)
        text = (
            direction + " <b>KESKIN NARX O'ZGARISH</b>\n\n"
            + "📌 <b>" + symbol + "</b>\n"
            + "💵 Narx: <b>" + price_str + "</b>\n"
            + "⚡ " + label + "\n"
            + "  1m:  {:+.2f}%\n".format(c1m)
            + "  5m:  {:+.2f}%\n".format(c5m)
            + "  1h:  {:+.2f}%".format(c1h)
        )
        async with AsyncSessionFactory() as db:
            result = await db.execute(
                select(User.telegram_id).where(User.is_active == True)
            )
            user_ids = [row[0] for row in result.fetchall()]
        tasks = [
            bot.app.bot.send_message(
                chat_id=uid, text=text, parse_mode=ParseMode.HTML
            )
            for uid in user_ids
        ]
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _on_listing(self, listing: dict):
        from telegram.constants import ParseMode
        from db.models import AsyncSessionFactory, User
        from sqlalchemy import select
        is_listing = listing["is_listing"]
        symbol = listing["symbol"]
        exchange = listing["exchange"]
        market_type = listing["market_type"]
        time_str = listing["time"]
        if is_listing:
            emoji = "🚀"
            action = "YANGI LISTING"
        else:
            emoji = "⚠️"
            action = "DELISTING"
        text = (
            emoji + " <b>" + action + "</b>\n\n"
            + "📌 <b>" + symbol + "</b> (" + exchange + " " + market_type + ")\n"
            + "⏰ Vaqt: <b>" + time_str + "</b>"
        )
        async with AsyncSessionFactory() as db:
            result = await db.execute(
                select(User.telegram_id).where(User.is_active == True)
            )
            user_ids = [row[0] for row in result.fetchall()]
        tasks = [
            bot.app.bot.send_message(
                chat_id=uid, text=text, parse_mode=ParseMode.HTML
            )
            for uid in user_ids
        ]
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _on_alert(self, alert: MarketAlert):
        await bot.send_alert(alert)

    async def _send_broadcast(self, message: str):
        from db.models import AsyncSessionFactory, User
        from sqlalchemy import select
        from telegram.constants import ParseMode
        async with AsyncSessionFactory() as db:
            result = await db.execute(select(User.telegram_id).where(User.is_active == True))
            user_ids = [row[0] for row in result.fetchall()]
        tasks = [
            bot.app.bot.send_message(chat_id=uid, text=message, parse_mode=ParseMode.HTML)
            for uid in user_ids
        ]
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def start(self):
        logger.info("🚀 Starting ALPHATRADERAI...")

        await init_db()
        await state_manager.connect()

        # 1-QADAM: Telegram bot
        asyncio.create_task(bot.start())

        # 2-QADAM: Connector birinchi — symbols topadi, WS ulaydi
        await self.connector.start()

        # 3-QADAM: KUTISH — bootstrap va WS o'rnashsin (5 daqiqa)
        logger.info("⏳ 5 daqiqa kutish — bootstrap va WS o'rnashsin...")
        await asyncio.sleep(300)

        # 4-QADAM: Scannerlar (REST kam ishlatadi)
        await self.volume_scanner.start()
        await self.oi_scanner.start()
        await self.liq_scanner.start()
        await self.whale_scanner.start()
        await self.funding_scanner.start()
        await self.alert_engine.start()

        await price_tracker.start()
        await self.price_change_scanner.start()
        await cvd_tracker.start()
        await self.listing_detector.start()

        # 5-QADAM: REST module-lar (har biri orasida 60+ soniya)
        import random as _rnd
        await self.ob_tracker.start()
        await asyncio.sleep(_rnd.uniform(30, 60))

        await self.digest_scheduler.start()
        await asyncio.sleep(_rnd.uniform(60, 120))

        await fear_greed_index.start()
        await asyncio.sleep(_rnd.uniform(60, 120))
        await long_short_ratio.start()
        await asyncio.sleep(_rnd.uniform(60, 120))
        await funding_rate_history.start()
        await asyncio.sleep(_rnd.uniform(60, 120))
        await liquidation_heatmap.start()
        await asyncio.sleep(_rnd.uniform(60, 120))
        await correlation_matrix.start()
        await asyncio.sleep(_rnd.uniform(60, 120))
        await volume_profile.start()
        await asyncio.sleep(_rnd.uniform(60, 120))
        await top_trader_sentiment.start()

        # ML PREDICTION
        await data_collector.start()
        await asyncio.sleep(_rnd.uniform(30, 60))
        await prediction_engine.start()
        await asyncio.sleep(_rnd.uniform(30, 60))
        await bookmap_engine.start()
        await pre_signal_detector.start()

        # Real-time PnL yangilash (har 10 sekunda)
        asyncio.create_task(bot.start_pnl_updater())

        # Premium expiry tekshirish (har 5 daqiqada)
        asyncio.create_task(bot.start_premium_checker())

        # Signal tarixini yuklash
        from signal_tracker import signal_tracker
        signal_tracker.load_from_file()

        await self.top_report.start()

        # Data Seeder — tarixiy ma'lumotlarni yuklaydi (CVD, Volume, Liq)
        await data_seeder.start()

        self.running = True
        logger.info("✅ Barcha modullar ishga tushdi!")
        logger.info(f"🌐 Admin panel: http://localhost:{settings.admin_port}")
        logger.info("📱 Telegramda botingizga /start yozing!")

        # Heartbeat — har 5 daqiqada bot tirik ekanligini log qiladi
        asyncio.create_task(self._heartbeat_loop())

    async def _heartbeat_loop(self):
        """Har 5 daqiqada bot holatini log qiladi"""
        while self.running:
            await asyncio.sleep(300)
            try:
                import psutil
                proc = psutil.Process()
                mem_mb = proc.memory_info().rss / 1024 / 1024
                cpu = proc.cpu_percent()
                threads = proc.num_threads()
                from signal_tracker import signal_tracker
                active = len(signal_tracker.active_signals)
                logger.info(f"💓 Heartbeat: CPU={cpu:.1f}% MEM={mem_mb:.0f}MB Threads={threads} Active={active}")
            except Exception:
                logger.info("💓 Heartbeat: OK")

    async def stop(self):
        logger.info("⏹ To'xtatilmoqda...")
        self.running = False
        try:
            # Signal tarixini saqlash
            from signal_tracker import signal_tracker
            signal_tracker.save_to_file()

            await price_tracker.stop()
            await self.price_change_scanner.stop()
            await cvd_tracker.stop()
            await self.listing_detector.stop()
            await self.connector.stop()
            await self.volume_scanner.stop()
            await self.oi_scanner.stop()
            await self.liq_scanner.stop()
            await self.whale_scanner.stop()
            await self.funding_scanner.stop()
            await self.ob_tracker.stop()           # YANGI
            await self.digest_scheduler.stop()     # YANGI

            # 7 TA YANGI MODULE
            await fear_greed_index.stop()
            await long_short_ratio.stop()
            await funding_rate_history.stop()
            await liquidation_heatmap.stop()
            await correlation_matrix.stop()
            await volume_profile.stop()
            await top_trader_sentiment.stop()

            # ML PREDICTION
            await data_collector.stop()
            await prediction_engine.stop()
            await bookmap_engine.stop()

            await self.alert_engine.stop()
            await self.top_report.stop()
            await bot.stop()
            await state_manager.disconnect()
        except Exception as e:
            logger.error(f"Stop error: {e}")

    async def run_forever(self):
        await self.start()
        try:
            while self.running:
                await asyncio.sleep(1)
        except asyncio.CancelledError:
            pass
        finally:
            await self.stop()


async def run_admin():
    config = uvicorn.Config(admin_app, host="0.0.0.0", port=settings.admin_port, log_level="warning")
    server = uvicorn.Server(config)
    await server.serve()


async def main():
    app = CryptoMonitorApp()
    try:
        await asyncio.gather(
            app.run_forever(),
            run_admin(),
            return_exceptions=True
        )
    except (KeyboardInterrupt, SystemExit):
        await app.stop()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bot to'xtatildi.")
