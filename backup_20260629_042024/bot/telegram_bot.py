"""
CRYPTO MONITOR PRO — Telegram Bot v3
- Show More / Qisqartirish callback buttons
- edit_message_text for live PNL updates
- Signal performance stats in /stats
- Consolidated signal format
"""
import asyncio
import uuid
from io import BytesIO
from datetime import datetime
from typing import Optional
from loguru import logger

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, BotCommand, InputMediaPhoto
from telegram.ext import (
    Application, CommandHandler, CallbackQueryHandler,
    ContextTypes
)
from telegram.constants import ParseMode

from config.settings import settings
from core.models import MarketAlert, AlertLevel, Direction
from core.state_manager import state_manager
from bot.formatter import (
    build_short_signal, build_full_signal, build_alert_keyboard,
    build_signal_keyboard, build_showmore_keyboard,
    build_bookmap_message, build_live_pnl_update,
    build_cextrack_activity,
)
from db.models import AsyncSessionFactory, User, UserSettings, Watchlist, AlertLog
from sqlalchemy import select


async def get_or_create_user(telegram_id: int, username: str = "", first_name: str = "") -> User:
    async with AsyncSessionFactory() as db:
        result = await db.execute(select(User).where(User.telegram_id == telegram_id))
        user = result.scalar_one_or_none()
        if not user:
            user = User(telegram_id=telegram_id, username=username, first_name=first_name)
            db.add(user)
            await db.flush()
            s = UserSettings(user_id=user.id)
            db.add(s)
            await db.commit()
            await db.refresh(user)
            logger.info(f"New user: {telegram_id} ({first_name})")
        else:
            user.last_seen = datetime.utcnow()
            await db.commit()
        return user


async def get_user_settings_db(telegram_id: int) -> dict:
    cached = await state_manager.get_cached_user_settings(telegram_id)
    if cached:
        return cached

    async with AsyncSessionFactory() as db:
        result = await db.execute(select(User).where(User.telegram_id == telegram_id))
        user = result.scalar_one_or_none()
        if not user:
            return _default_settings()

        result2 = await db.execute(select(UserSettings).where(UserSettings.user_id == user.id))
        s = result2.scalar_one_or_none()
        if not s:
            return _default_settings()

        data = {
            "volume_enabled": s.volume_enabled,
            "oi_enabled": s.oi_enabled,
            "liquidation_enabled": s.liquidation_enabled,
            "whale_enabled": s.whale_enabled,
            "orderbook_enabled": s.orderbook_enabled,
            "funding_enabled": s.funding_enabled,
            "listing_alerts": s.listing_alerts,
            "top_report_enabled": s.top_report_enabled,
            "only_binance": s.only_binance,
            "min_alert_level": s.min_alert_level,
            "coin_filter": s.coin_filter,
            "alerts_paused": s.alerts_paused,
            "timezone_offset": getattr(s, "timezone_offset", 0) or 0,
        }
        await state_manager.cache_user_settings(telegram_id, data)
        return data


def _fmt_tz(offset: int) -> str:
    offset = offset or 0
    if offset == 0:
        return "UTC"
    sign = "+" if offset > 0 else ""
    return f"UTC{sign}{offset}"


def _default_settings() -> dict:
    return {
        "volume_enabled": True,
        "oi_enabled": True,
        "liquidation_enabled": True,
        "whale_enabled": True,
        "orderbook_enabled": True,
        "funding_enabled": True,
        "listing_alerts": True,
        "top_report_enabled": True,
        "only_binance": True,
        "min_alert_level": "notice",
        "coin_filter": "all",
        "alerts_paused": False,
        "timezone_offset": 0,
    }


async def toggle_setting_db(telegram_id: int, field: str):
    field_map = {
        "volume": "volume_enabled",
        "oi": "oi_enabled",
        "liquidation": "liquidation_enabled",
        "whale": "whale_enabled",
        "orderbook": "orderbook_enabled",
        "funding": "funding_enabled",
        "listing": "listing_alerts",
        "top_report": "top_report_enabled",
    }
    db_field = field_map.get(field)
    if not db_field:
        return

    async with AsyncSessionFactory() as db:
        result = await db.execute(select(User).where(User.telegram_id == telegram_id))
        user = result.scalar_one_or_none()
        if not user:
            return
        result2 = await db.execute(select(UserSettings).where(UserSettings.user_id == user.id))
        s = result2.scalar_one_or_none()
        if s:
            current = getattr(s, db_field)
            setattr(s, db_field, not current)
            await db.commit()

    await state_manager.invalidate_user_cache(telegram_id)


async def set_setting_db(telegram_id: int, field: str, value):
    async with AsyncSessionFactory() as db:
        result = await db.execute(select(User).where(User.telegram_id == telegram_id))
        user = result.scalar_one_or_none()
        if not user:
            return
        result2 = await db.execute(select(UserSettings).where(UserSettings.user_id == user.id))
        s = result2.scalar_one_or_none()
        if s:
            setattr(s, field, value)
            await db.commit()
    await state_manager.invalidate_user_cache(telegram_id)


class CryptoMonitorBot:
    def __init__(self):
        self.app: Optional[Application] = None
        self._running = False

    async def start(self):
        self.app = Application.builder().token(settings.telegram_bot_token).build()

        self.app.add_handler(CommandHandler("start", self._cmd_start))
        self.app.add_handler(CommandHandler("settings", self._cmd_settings))
        self.app.add_handler(CommandHandler("stats", self._cmd_stats))
        self.app.add_handler(CommandHandler("watchlist", self._cmd_watchlist))
        self.app.add_handler(CommandHandler("pause", self._cmd_pause))
        self.app.add_handler(CommandHandler("resume", self._cmd_resume))
        self.app.add_handler(CommandHandler("help", self._cmd_help))

        # 7 TA YANGI MODULE COMMANDS
        self.app.add_handler(CommandHandler("fear", self._cmd_fear))
        self.app.add_handler(CommandHandler("lsr", self._cmd_lsr))
        self.app.add_handler(CommandHandler("frh", self._cmd_frh))
        self.app.add_handler(CommandHandler("liqmap", self._cmd_liqmap))
        self.app.add_handler(CommandHandler("corr", self._cmd_corr))
        self.app.add_handler(CommandHandler("vp", self._cmd_vp))
        self.app.add_handler(CommandHandler("toptrader", self._cmd_toptrader))

        self.app.add_handler(CallbackQueryHandler(self._handle_callback))

        try:
            await self.app.bot.set_my_commands([
                BotCommand("start", "Botni ishga tushirish"),
                BotCommand("settings", "Sozlamalar paneli"),
                BotCommand("stats", "Tizim statistikasi + Signallar"),
                BotCommand("watchlist", "Kuzatuv ro'yxati"),
                BotCommand("pause", "Alertlarni to'xtatish"),
                BotCommand("resume", "Alertlarni yoqish"),
                BotCommand("help", "Yordam"),
                BotCommand("fear", "🧠 Fear & Greed Index"),
                BotCommand("lsr", "📊 Long/Short Ratio"),
                BotCommand("frh", "💰 Funding Rate History"),
                BotCommand("liqmap", "🔥 Liquidation Heatmap"),
                BotCommand("corr", "🔗 Correlation Matrix"),
                BotCommand("vp", "📊 Volume Profile"),
                BotCommand("toptrader", "🐋 Top Trader Sentiment"),
            ])
        except Exception as e:
            logger.warning(f"set_my_commands error: {e}")

        self._running = True
        await self.app.initialize()
        await self.app.start()
        await self.app.updater.start_polling(drop_pending_updates=False)
        logger.info("✅ Telegram Bot v3 started")

    async def stop(self):
        self._running = False
        if self.app:
            await self.app.updater.stop()
            await self.app.stop()
            await self.app.shutdown()

    # ════════════════════════════════════════════
    # ALERT DELIVERY
    # ════════════════════════════════════════════

    async def send_alert(self, alert: MarketAlert):
        if not self._running:
            return
        try:
            from modules.price_tracker import price_tracker
            from modules.cvd_tracker import cvd_tracker
            from signal_tracker import signal_tracker, Signal

            # Price from price_tracker
            price_changes = price_tracker.get_price_changes(alert.symbol)
            if not price_changes or price_changes.get("current", 0) <= 0:
                ticker = await state_manager.get_ticker(
                    alert.exchange.value, alert.symbol
                )
                if ticker and ticker.get("price", 0) > 0:
                    real_price = ticker["price"]
                    price_changes = {
                        "current": real_price,
                        "change_1s": 0, "change_1m": 0, "change_5m": 0,
                        "change_15m": 0, "change_1h": 0,
                    }
                    alert.current_price = real_price

            if alert.current_price <= 0:
                ticker = await state_manager.get_ticker(
                    alert.exchange.value, alert.symbol
                )
                if ticker and ticker.get("price", 0) > 0:
                    alert.current_price = ticker["price"]

            # Fill price_changes from extra if all zeros
            extra = getattr(alert, "extra", {}) or {}
            has_real_data = any(
                price_changes.get(k, 0) != 0
                for k in ("change_1m", "change_5m", "change_1h")
            )
            if not has_real_data and extra:
                for key in ("change_1m", "change_5m", "change_15m", "change_1h", "change_4h", "change_24h"):
                    xkey = f"price_{key}" if key != "change_24h" else "price_change_24h"
                    if key == "change_15m":
                        xkey = "price_change_15m"
                    price_changes[key] = extra.get(xkey, 0)

            # CVD fallback
            cvd_data = cvd_tracker.get_cvd_data(alert.symbol)
            if cvd_data and cvd_data.get("cvd_1m", 0) == 0 and cvd_data.get("cvd_5m", 0) == 0 and extra:
                cvd_1m = extra.get("cvd_1m", 0)
                cvd_5m = extra.get("cvd_5m", 0)
                if cvd_1m != 0 or cvd_5m != 0:
                    cvd_data = {
                        "cvd_1m": cvd_1m, "cvd_5m": cvd_5m,
                        "cvd_direction": extra.get("cvd_direction", "neutral"),
                        "cvd_current": 0,
                    }

            users = await self._get_eligible_users(alert)

            if not users:
                logger.debug(f"⚠️ {alert.symbol}: hech qanday eligible user topilmadi")
                return

            # Generate unique signal_id
            signal_id = str(uuid.uuid4())[:8]

            # Register signal for tracking
            if alert.strategy_direction and alert.strategy_entry > 0:
                sig = Signal(
                    symbol=alert.symbol,
                    direction=alert.strategy_direction,
                    entry_price=alert.strategy_entry,
                    sl_price=alert.strategy_sl,
                    tp_price=alert.strategy_tp,
                    strategy=alert.strategy_reason,
                    extra_data=extra,
                    signal_id=signal_id,
                )
                signal_tracker.add_signal(sig)

            tasks = [
                self._send_to_user(
                    u, alert, price_changes, cvd_data, signal_id
                ) for u in users
            ]
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)

        except Exception as e:
            logger.error(f"Alert delivery error: {e}")

    async def _send_to_user(
        self, telegram_id: int, alert: MarketAlert,
        price_changes: dict, cvd_data: dict,
        signal_id: str,
    ) -> bool:
        try:
            from signal_tracker import signal_tracker

            s = await get_user_settings_db(telegram_id)
            tz_offset = s.get("timezone_offset", 0) or 0

            # ─── SHORT signal first (with Show More button) ───
            short_text = build_short_signal(alert, price_changes, cvd_data, tz_offset)
            keyboard = build_signal_keyboard(
                alert.symbol, alert.exchange.value, alert.market_type.value, signal_id
            )
            reply_markup = InlineKeyboardMarkup([
                [InlineKeyboardButton(
                    btn.get("text", ""),
                    url=btn.get("url"),
                    callback_data=btn.get("callback_data"),
                ) for btn in row]
                for row in keyboard
            ])

            # ─── CHART ───
            chart_bytes = None
            chart_markup = None
            try:
                from modules.chart_generator import generate_price_chart, build_timeframe_keyboard, DEFAULT_TIMEFRAME
                chart_bytes = await generate_price_chart(
                    alert.symbol,
                    timeframe=DEFAULT_TIMEFRAME,
                    title_extra=alert.level.value.upper(),
                )
                if chart_bytes:
                    tf_rows = build_timeframe_keyboard(alert.symbol, alert.exchange.value, active=DEFAULT_TIMEFRAME)
                    chart_markup = InlineKeyboardMarkup([
                        [InlineKeyboardButton(b["text"], callback_data=b["callback_data"]) for b in row]
                        for row in tf_rows
                    ])
            except Exception as e:
                logger.debug(f"Chart error {alert.symbol}: {e}")

            # Send chart first
            if chart_bytes:
                try:
                    await self.app.bot.send_photo(
                        chat_id=telegram_id,
                        photo=BytesIO(chart_bytes),
                        reply_markup=chart_markup,
                    )
                except Exception as e:
                    logger.debug(f"Chart send error {telegram_id}: {e}")

            # Send short signal with Show More button
            msg = await self.app.bot.send_message(
                chat_id=telegram_id,
                text=short_text,
                parse_mode=ParseMode.HTML,
                reply_markup=reply_markup,
                disable_web_page_preview=True,
            )

            # Track message_id for live updates
            if msg and msg.message_id:
                sig = signal_tracker.get_active_signal(alert.symbol)
                if sig:
                    sig.message_id = msg.message_id
                    sig.chat_id = telegram_id
                    # Add to message_map for Show More callback
                    signal_tracker._message_map[(telegram_id, msg.message_id)] = sig

            logger.info(f"✅ Signal yuborildi: {alert.symbol} → user {telegram_id}")
            return True
        except Exception as e:
            logger.warning(f"❌ Send failed to {telegram_id}: {e}")
            return False

    async def _get_eligible_users(self, alert: MarketAlert) -> list[int]:
        async with AsyncSessionFactory() as db:
            result = await db.execute(
                select(User.telegram_id).where(User.is_active == True)
            )
            user_ids = [row[0] for row in result.fetchall()]

        eligible = []
        for uid in user_ids:
            s = await get_user_settings_db(uid)
            if self._user_wants_alert(s, alert):
                eligible.append(uid)
        return eligible

    def _user_wants_alert(self, s: dict, alert: MarketAlert) -> bool:
        if s.get("alerts_paused"):
            return False
        level_order = {AlertLevel.NOTICE: 1, AlertLevel.STRONG: 2, AlertLevel.EXTREME: 3}
        min_level = {"notice": 1, "strong": 2, "extreme": 3}.get(
            s.get("min_alert_level", "notice"), 1
        )
        if level_order.get(alert.level, 0) < min_level:
            return False
        if s.get("only_binance") and alert.exchange.value != "binance":
            return False
        return True

    # ════════════════════════════════════════════
    # LIVE PNL UPDATE TASK
    # ════════════════════════════════════════════

    async def start_pnl_updater(self):
        """Background task — only check TP/SL, no auto message updates"""
        while self._running:
            try:
                from signal_tracker import signal_tracker
                active = signal_tracker.get_all_active()
                for sig in active:
                    if sig.message_id and sig.chat_id:
                        ticker = await state_manager.get_ticker("binance", sig.symbol)
                        if ticker and ticker.get("price", 0) > 0:
                            current_price = ticker["price"]
                            # Only check TP/SL — no message editing
                            signal_tracker.check_price(sig.symbol, current_price)
            except Exception as e:
                logger.debug(f"PnL updater error: {e}")
            await asyncio.sleep(30)

    # ════════════════════════════════════════════
    # COMMANDS
    # ════════════════════════════════════════════

    async def _cmd_start(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        user = update.effective_user
        await get_or_create_user(user.id, user.username or "", user.first_name or "")

        text = (
            f"🎉 Salom, <b>{user.first_name}</b>!\n\n"
            f"🌟 <b>ALPHATRADERAI — Smart Signal Bot</b>\n\n"
            f"🎯 <b>REAL SIGNALLAR</b>\n"
            f"  • Binance Futures real-time tahlil\n"
            f"  • Whale, OI, CVD, Volume indikatorlari\n"
            f"  • Show More → Bookmap + Liq zonalar\n\n"
            f"📊 <b>7 TA MODUL</b>\n"
            f"  🔥 Fear & Greed     📊 Long/Short Ratio\n"
            f"  💰 Funding History   🔥 Liquidation Heatmap\n"
            f"  📈 Volume Profile    🔗 Correlation Matrix\n"
            f"  🐋 Top Trader Sentiment\n\n"
            f"⚙️ <b>BUYRUGLAR</b>\n"
            f"  /settings — Sozlamalar\n"
            f"  /stats — Statistika\n"
            f"  /liqmap — Likvidatsiya xaritasi\n"
            f"  /fear — Fear & Greed Index\n"
            f"  /lsr — Long/Short Ratio\n"
            f"  /funding — Funding history\n"
            f"  /corr — Korrelyatsiya matritsasi\n"
            f"  /vp — Volume Profile\n"
            f"  /toptrader — Top trader sentiment\n\n"
            f"📲 <b>Signallar avtomatik keladi!</b>"
        )
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("⚙️ Sozlamalar", callback_data="settings_main")],
            [InlineKeyboardButton("📊 Statistika", callback_data="stats_view")],
        ])
        await update.message.reply_text(text, parse_mode=ParseMode.HTML, reply_markup=keyboard)

    async def _cmd_settings(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        user = update.effective_user
        await get_or_create_user(user.id, user.username or "", user.first_name or "")
        await self._show_settings(user.id, update.message)

    async def _cmd_stats(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id
        if user_id != 5571433323:
            await update.message.reply_text("⚠️ Bu bo'lim faqat admin uchun.")
            return
        await self._send_stats(update.message)

    async def _cmd_watchlist(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        user_id = update.effective_user.id
        async with AsyncSessionFactory() as db:
            result = await db.execute(select(User).where(User.telegram_id == user_id))
            user = result.scalar_one_or_none()
            if not user:
                await update.message.reply_text("Avval /start bosing")
                return
            result2 = await db.execute(
                select(Watchlist).where(Watchlist.user_id == user.id)
            )
            items = result2.scalars().all()

        if not items:
            text = "⭐ Kuzatuv ro'yxatingiz bo'sh.\n\nAlert xabarlaridagi ⭐ tugmasini bosing."
        else:
            lines = ["<b>⭐ KUZATUV RO'YXATI</b>\n"]
            for item in items:
                lines.append(f"• {item.symbol} ({item.exchange.upper()})")
            text = "\n".join(lines)

        await update.message.reply_text(text, parse_mode=ParseMode.HTML)

    async def _cmd_pause(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        await set_setting_db(update.effective_user.id, "alerts_paused", True)
        await update.message.reply_text("⏸ Alertlar to'xtatildi. Yoqish: /resume")

    async def _cmd_resume(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        await set_setting_db(update.effective_user.id, "alerts_paused", False)
        await update.message.reply_text("▶️ Alertlar yoqildi!")

    async def _cmd_help(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        text = (
            "<b>📖 YORDAM</b>\n\n"
            "/start — Botni ishga tushirish\n"
            "/settings — Sozlamalar\n"
            "/stats — Statistika + Signallar\n"
            "/watchlist — Kuzatuv ro'yxati\n"
            "/pause — Alertlarni to'xtatish\n"
            "/resume — Alertlarni yoqish\n\n"
            "<b>📊 Market Modullari:</b>\n"
            "/fear — 🧠 Fear & Greed Index\n"
            "/lsr — 📊 Long/Short Ratio\n"
            "/frh — 💰 Funding Rate History\n"
            "/liqmap — 🔥 Liquidation Heatmap\n"
            "/corr — 🔗 Correlation Matrix\n"
            "/vp — 📊 Volume Profile (POC/VAH/VAL)\n"
            "/toptrader — 🐋 Top Trader Sentiment"
        )
        await update.message.reply_text(text, parse_mode=ParseMode.HTML)

    # ════════════════════════════════════════════
    # 7 TA YANGI MODULE COMMANDS
    # ════════════════════════════════════════════

    async def _cmd_fear(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        from modules.fear_greed import fear_greed_index
        text = fear_greed_index.format_text()
        await update.message.reply_text(text, parse_mode=ParseMode.HTML)

    async def _cmd_lsr(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        from modules.long_short_ratio import long_short_ratio
        text = long_short_ratio.format_text()
        await update.message.reply_text(text, parse_mode=ParseMode.HTML)

    async def _cmd_frh(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        from modules.funding_history import funding_rate_history
        text = funding_rate_history.format_text()
        await update.message.reply_text(text, parse_mode=ParseMode.HTML)

    async def _cmd_liqmap(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        import time as _time
        from modules.liquidation_heatmap import liquidation_heatmap
        await update.message.reply_text("⏳ Yuklanmoqda...")
        try:
            if not liquidation_heatmap.data:
                await liquidation_heatmap._fetch_all()
            elif _time.time() - liquidation_heatmap.data.get(list(liquidation_heatmap.data.keys())[0], {}).get("timestamp", 0) > 600:
                await liquidation_heatmap._fetch_all()
        except Exception as e:
            logger.warning(f"/liqmap fetch error: {e}")
        text = liquidation_heatmap.format_text()
        await update.message.reply_text(text, parse_mode=ParseMode.HTML)

    async def _cmd_corr(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        from modules.correlation_matrix import correlation_matrix
        text = correlation_matrix.format_text()
        await update.message.reply_text(text, parse_mode=ParseMode.HTML)

    async def _cmd_vp(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        from modules.volume_profile import volume_profile
        text = volume_profile.format_text()
        await update.message.reply_text(text, parse_mode=ParseMode.HTML)

    async def _cmd_toptrader(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        from modules.top_trader_sentiment import top_trader_sentiment
        text = top_trader_sentiment.format_text()
        await update.message.reply_text(text, parse_mode=ParseMode.HTML)

    # ════════════════════════════════════════════
    # SETTINGS UI
    # ════════════════════════════════════════════

    async def _show_settings(self, user_id: int, target):
        s = await get_user_settings_db(user_id)

        def ico(val): return "✅" if val else "❌"

        text = "<b>⚙️ SOZLAMALAR PANELI</b>\n\nSkanerlarni yoqish/o'chirish:"
        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton(
                f"{ico(s['volume_enabled'])} Volume Scanner",
                callback_data="toggle:volume"
            )],
            [InlineKeyboardButton(
                f"{ico(s['oi_enabled'])} OI Scanner",
                callback_data="toggle:oi"
            )],
            [InlineKeyboardButton(
                f"{ico(s['liquidation_enabled'])} Likvidatsiya",
                callback_data="toggle:liquidation"
            )],
            [InlineKeyboardButton(
                f"{ico(s['whale_enabled'])} Whale Scanner",
                callback_data="toggle:whale"
            )],
            [InlineKeyboardButton(
                f"{ico(s['orderbook_enabled'])} Order Book",
                callback_data="toggle:orderbook"
            )],
            [InlineKeyboardButton(
                f"{ico(s['funding_enabled'])} Funding Rate",
                callback_data="toggle:funding"
            )],
            [InlineKeyboardButton(
                f"{ico(s['listing_alerts'])} Listing Alerts",
                callback_data="toggle:listing"
            )],
            [InlineKeyboardButton(
                f"{ico(s['top_report_enabled'])} Top Coin Report",
                callback_data="toggle:top_report"
            )],
            [
                InlineKeyboardButton(
                    "🔔 Alert Darajasi",
                    callback_data="settings:alert_level"
                ),
                InlineKeyboardButton(
                    "🪙 Coinlar",
                    callback_data="settings:coins"
                ),
            ],
            [InlineKeyboardButton(
                f"🌍 Vaqt zonasi: {_fmt_tz(s.get('timezone_offset', 0))}",
                callback_data="settings:timezone"
            )],
            [InlineKeyboardButton("❌ Yopish", callback_data="settings:close")],
        ])

        if hasattr(target, 'reply_text'):
            await target.reply_text(
                text, parse_mode=ParseMode.HTML, reply_markup=keyboard
            )
        else:
            await target.edit_message_text(
                text, parse_mode=ParseMode.HTML, reply_markup=keyboard
            )

    # ════════════════════════════════════════════
    # CALLBACK HANDLER
    # ════════════════════════════════════════════

    async def _handle_callback(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        query = update.callback_query
        try:
            await query.answer()
        except Exception:
            return
        data = query.data
        user_id = update.effective_user.id

        try:
            await self._process_callback(query, data, user_id)
        except Exception as e:
            if "Message is not modified" in str(e):
                pass  # Matn o'zgarmagan — xavfsiz e'tiborsiz qoldiramiz
            else:
                logger.debug(f"Callback error: {e}")

    async def _process_callback(self, query, data: str, user_id: int):

        # ─── SHOW MORE / COLLAPSE ──────────────────────
        if data.startswith("showmore:"):
            signal_id = data.split(":", 1)[1]
            # Find the signal from message
            from signal_tracker import signal_tracker
            sig = signal_tracker.get_signal_by_message(query.message.chat_id, query.message.message_id)

            if sig:
                # Build full signal
                from modules.price_tracker import price_tracker
                price_changes = price_tracker.get_price_changes(sig.symbol)
                extra = getattr(sig, "extra_data", {})
                full_text = build_full_signal_text(sig, extra, price_changes)
                keyboard = build_showmore_keyboard(signal_id)
                reply_markup = InlineKeyboardMarkup([
                    [InlineKeyboardButton(btn["text"], callback_data=btn["callback_data"])]
                    for btn in keyboard[0]
                ])
                await query.edit_message_text(
                    full_text,
                    parse_mode=ParseMode.HTML,
                    reply_markup=reply_markup,
                )
            else:
                # Fallback: just show a generic expanded view
                await query.edit_message_text(
                    f"📖 <b>Signal #{signal_id}</b>\n\nBatafsil ma'lumot mavjud emas.\nSignal yangilangan bo'lishi mumkin.",
                    parse_mode=ParseMode.HTML,
                )
            return

        if data.startswith("collapse:"):
            signal_id = data.split(":", 1)[1]
            from signal_tracker import signal_tracker
            sig = signal_tracker.get_signal_by_message(query.message.chat_id, query.message.message_id)
            if sig:
                short_text = build_short_signal_text(sig)
                keyboard = build_signal_keyboard(
                    sig.symbol, "binance", "futures", signal_id
                )
                reply_markup = InlineKeyboardMarkup([
                    [InlineKeyboardButton(
                        btn.get("text", ""),
                        url=btn.get("url"),
                        callback_data=btn.get("callback_data"),
                    ) for btn in row]
                    for row in keyboard
                ])
                await query.edit_message_text(
                    short_text,
                    parse_mode=ParseMode.HTML,
                    reply_markup=reply_markup,
                )
            return

        if data.startswith("refresh:"):
            signal_id = data.split(":", 1)[1]
            from signal_tracker import signal_tracker
            sig = signal_tracker.get_signal_by_message(query.message.chat_id, query.message.message_id)
            if sig:
                # Fetch latest price — max/min ni yangilash, entry NI O'ZGARTIRMASLIK
                ticker = await state_manager.get_ticker("binance", sig.symbol)
                if ticker and ticker.get("price", 0) > 0:
                    current_price = ticker["price"]
                    sig.check_tp_sl(current_price)
                short_text = build_short_signal_text(sig)
                keyboard = build_signal_keyboard(
                    sig.symbol, "binance", "futures", signal_id
                )
                reply_markup = InlineKeyboardMarkup([
                    [InlineKeyboardButton(
                        btn.get("text", ""),
                        url=btn.get("url"),
                        callback_data=btn.get("callback_data"),
                    ) for btn in row]
                    for row in keyboard
                ])
                await query.edit_message_text(
                    short_text,
                    parse_mode=ParseMode.HTML,
                    reply_markup=reply_markup,
                )
            await query.answer("🔄 Yangilandi!")
            return

        # ─── EXISTING CALLBACKS ───────────────────────
        if data.startswith("toggle:"):
            field = data.split(":", 1)[1]
            await toggle_setting_db(user_id, field)
            await self._show_settings(user_id, query)

        elif data == "settings_main":
            await self._show_settings(user_id, query)

        elif data == "settings:alert_level":
            s = await get_user_settings_db(user_id)
            current = s.get("min_alert_level", "notice")

            def a(l): return " ✅" if current == l else ""

            keyboard = InlineKeyboardMarkup([
                [InlineKeyboardButton(f"🟡 Notice{a('notice')}", callback_data="set_level:notice")],
                [InlineKeyboardButton(f"🟢 Strong{a('strong')}", callback_data="set_level:strong")],
                [InlineKeyboardButton(f"🔥 Extreme{a('extreme')}", callback_data="set_level:extreme")],
                [InlineKeyboardButton("◀️ Orqaga", callback_data="settings_main")],
            ])
            await query.edit_message_text(
                "<b>🔔 Minimal Alert Darajasi</b>\n\n"
                "Faqat tanlangan darajadan yuqori alertlar keladi:",
                parse_mode=ParseMode.HTML,
                reply_markup=keyboard
            )

        elif data.startswith("set_level:"):
            level = data.split(":", 1)[1]
            await set_setting_db(user_id, "min_alert_level", level)
            await self._show_settings(user_id, query)

        elif data == "settings:coins":
            s = await get_user_settings_db(user_id)
            current = s.get("coin_filter", "all")

            def a(f): return " ✅" if current == f else ""

            keyboard = InlineKeyboardMarkup([
                [InlineKeyboardButton(f"Top 50{a('top50')}", callback_data="set_coins:top50")],
                [InlineKeyboardButton(f"Top 100{a('top100')}", callback_data="set_coins:top100")],
                [InlineKeyboardButton(f"Barchasi{a('all')}", callback_data="set_coins:all")],
                [InlineKeyboardButton("◀️ Orqaga", callback_data="settings_main")],
            ])
            await query.edit_message_text(
                "<b>🪙 Coin Filtri</b>",
                parse_mode=ParseMode.HTML,
                reply_markup=keyboard
            )

        elif data.startswith("set_coins:"):
            coin_filter = data.split(":", 1)[1]
            await set_setting_db(user_id, "coin_filter", coin_filter)
            await self._show_settings(user_id, query)

        elif data == "settings:timezone":
            s = await get_user_settings_db(user_id)
            current = s.get("timezone_offset", 0) or 0

            def a(o): return " ✅" if current == o else ""

            offsets = [-12, -8, -5, -4, -3, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 12]
            rows = []
            row = []
            for o in offsets:
                row.append(InlineKeyboardButton(f"{_fmt_tz(o)}{a(o)}", callback_data=f"set_tz:{o}"))
                if len(row) == 3:
                    rows.append(row)
                    row = []
            if row:
                rows.append(row)
            rows.append([InlineKeyboardButton("◀️ Orqaga", callback_data="settings_main")])

            await query.edit_message_text(
                "<b>🌍 Vaqt zonasi</b>\n\n"
                "Signal xabarlaridagi vaqt shu zonada ko'rsatiladi.\n"
                "Masalan, Toshkent — UTC+5.",
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup(rows)
            )

        elif data.startswith("set_tz:"):
            offset = int(data.split(":", 1)[1])
            await set_setting_db(user_id, "timezone_offset", offset)
            await self._show_settings(user_id, query)

        elif data.startswith("watchlist_add:"):
            symbol = data.split(":", 1)[1]
            async with AsyncSessionFactory() as db:
                result = await db.execute(
                    select(User).where(User.telegram_id == user_id)
                )
                user = result.scalar_one_or_none()
                if user:
                    existing = await db.execute(
                        select(Watchlist).where(
                            Watchlist.user_id == user.id,
                            Watchlist.symbol == symbol
                        )
                    )
                    if not existing.scalar_one_or_none():
                        db.add(Watchlist(user_id=user.id, symbol=symbol))
                        await db.commit()
            await query.answer(
                f"⭐ {symbol} kuzatuv ro'yxatiga qo'shildi!",
                show_alert=True
            )

        elif data == "settings:close":
            await query.edit_message_text("✅ Sozlamalar saqlandi")

        elif data == "stats_view":
            if user_id != 5571433323:
                await query.answer("⚠️ Bu bo'lim faqat admin uchun.", show_alert=True)
                return
            await self._send_stats(query)

        elif data.startswith("chart:"):
            try:
                _, exchange, symbol, timeframe = data.split(":", 3)
            except ValueError:
                return
            await query.answer("📊 Grafik yangilanmoqda...")
            try:
                from modules.chart_generator import generate_price_chart, build_timeframe_keyboard
                chart_bytes = await generate_price_chart(
                    symbol, timeframe=timeframe, title_extra=timeframe.upper()
                )
                if not chart_bytes:
                    return
                tf_rows = build_timeframe_keyboard(symbol, exchange, active=timeframe)
                chart_markup = InlineKeyboardMarkup([
                    [InlineKeyboardButton(b["text"], callback_data=b["callback_data"]) for b in row]
                    for row in tf_rows
                ])
                media = InputMediaPhoto(media=BytesIO(chart_bytes))
                await query.edit_message_media(media=media, reply_markup=chart_markup)
            except Exception as e:
                logger.debug(f"Chart switch error {symbol}: {e}")

    async def _send_stats(self, target):
        stats = await state_manager.get_all_stats()
        symbols_count = len(await state_manager.get_symbols("binance", "futures"))

        async with AsyncSessionFactory() as db:
            from sqlalchemy import func
            result = await db.execute(
                select(func.count(User.id)).where(User.is_active == True)
            )
            user_count = result.scalar() or 0

        # Real signal hisoblagich — signal_tracker dan
        from signal_tracker import signal_tracker
        active_count = len(signal_tracker.get_all_active())
        history_count = len(signal_tracker._history)

        lines = [
            "<b>📊 ALPHATRADERAI STATISTIKASI</b>\n",
            f"🔮 Kuzatilayotgan coinlar: <b>{symbols_count}</b>",
            f"👤 Foydalanuvchilar: <b>{user_count}</b>",
            f"📨 Yuborilgan alertlar: <b>{stats.get('alerts_sent', 0)}</b>",
        ]

        text = "\n".join(lines)

        if hasattr(target, 'reply_text'):
            await target.reply_text(text, parse_mode=ParseMode.HTML)
        else:
            await target.edit_message_text(text, parse_mode=ParseMode.HTML)


# ─── HELP FUNCTIONS ──────────────────────────────────────────

def build_short_signal_text(sig) -> str:
    """Build short signal text from Signal object — same format as initial"""
    from bot.formatter import fmt_price, fmt_usdt, fmt_token, fmt_time
    from datetime import datetime

    extra = getattr(sig, "extra_data", {}) or {}
    now = datetime.utcnow()

    lines = []
    lines.append("━━━━━━━━━━━━━━━━━━━━━━━━━━")
    lines.append(f"🪙 <b>{sig.symbol}</b> — {fmt_price(sig.entry_price)}$")
    lines.append("━━━━━━━━━━━━━━━━━━━━━━━━━━")
    lines.append("")

    # ── WHALE ────────────────────────────────────────
    whale_usdt = extra.get("whale_liquidity_usdt", 0) or 0
    last_whale_usdt = extra.get("last_whale_usdt", 0) or 0
    last_whale_side = extra.get("last_whale_side", "")
    if whale_usdt > 0:
        buy_v = extra.get("whale_buy_volume", 0) or 0
        sell_v = extra.get("whale_sell_volume", 0) or 0
        bias = "BUY 🟢" if buy_v >= sell_v else "SELL 🔴"
        token_qty = extra.get("whale_token_qty", 0) or 0
        token_str = f" ({fmt_token(token_qty)})" if token_qty > 0 else ""
        lines.append(f"🐋 <b>WHALE — {bias}</b>")
        lines.append(f"  {fmt_usdt(whale_usdt)}{token_str}")
        vol_24h = extra.get("volume_24h", 0) or 0
        if vol_24h > 0:
            vol_pct = whale_usdt / vol_24h * 100
            lines.append(f"  24h: {fmt_usdt(vol_24h)} | {vol_pct:.2f}%")
    elif last_whale_usdt > 0:
        dir_word = "BUY 🟢" if last_whale_side == "buy" else "SELL 🔴"
        lines.append(f"🐋 <b>WHALE — {dir_word}</b>")
        lines.append(f"  {fmt_usdt(last_whale_usdt)}")
    else:
        # Global oxirgi whale — har qanday coin uchun
        import asyncio
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                gw = state_manager._data.get("global:last_whale")
                if gw and gw.get("usdt", 0) > 0:
                    ago_s = time.time() - gw.get("ts", 0)
                    if ago_s < 60:
                        ago_str = f"{int(ago_s)}s oldin"
                    elif ago_s < 3600:
                        ago_str = f"{int(ago_s // 60)}m oldin"
                    elif ago_s < 86400:
                        ago_str = f"{int(ago_s // 3600)}h oldin"
                    else:
                        ago_str = f"{int(ago_s // 86400)}d oldin"
                    dir_w = "BUY 🟢" if gw["direction"] == "buy" else "SELL 🔴"
                    lines.append(f"🐋 <b>Oxirgi whale:</b> {gw['symbol']} {dir_w} {fmt_usdt(gw['usdt'])} — {ago_str}")
                else:
                    lines.append("🐋 <b>Whale:</b> Hali aniqlanmagan")
        except Exception:
            lines.append("🐋 <b>Whale:</b> Hali aniqlanmagan")
    lines.append("")

    # ── OI + FUNDING ─────────────────────────────────
    oi_usdt = extra.get("oi_usdt", 0) or 0
    oi_change = extra.get("oi_change_pct", 0) or 0
    funding = extra.get("funding_rate", 0) or 0
    oi_f_parts = []
    if oi_usdt > 0 or oi_change != 0:
        oi_ico = "▲" if oi_change > 0 else "▼"
        oi_f_parts.append(f"📊 OI: {fmt_usdt(oi_usdt)} {oi_ico} {oi_change:+.1f}%")
    if funding != 0:
        fr_ico = "🔴" if funding > 0 else "🟢"
        oi_f_parts.append(f"{fr_ico} Funding: {funding:+.4f}%")
    if oi_f_parts:
        lines.append(" | ".join(oi_f_parts))
        lines.append("")

    # ── ORDERBOOK ────────────────────────────────────
    ob_buy = extra.get("ob_buy_walls", [])
    ob_sell = extra.get("ob_sell_walls", [])
    imbalance = extra.get("ob_imbalance", 1.0)
    buy_price = fmt_price(ob_buy[0]["price"]) if ob_buy else "—"
    buy_usdt = fmt_usdt(ob_buy[0]["usdt"]) if ob_buy else "—"
    sell_price = fmt_price(ob_sell[0]["price"]) if ob_sell else "—"
    sell_usdt = fmt_usdt(ob_sell[0]["usdt"]) if ob_sell else "—"
    ob_bias = "BUY 🟢" if imbalance >= 1 else "SELL 🔴"
    lines.append("📖 <b>ORDERBOOK</b>")
    lines.append(f"  Buy: {buy_price} ({buy_usdt}) | Sell: {sell_price} ({sell_usdt})")
    lines.append(f"  Imbalance: {imbalance:.1f}x {ob_bias}")
    lines.append("")

    # ── VOLUME ───────────────────────────────────────
    vol_5m = extra.get("vol_5m", 0) or 0
    vol_1h = extra.get("vol_1h", 0) or 0
    cvd_5m = extra.get("cvd_5m", 0) or 0
    taker = extra.get("taker_ratio", 0) or 0
    c5m = extra.get("price_change_5m", 0) or 0
    c1h = extra.get("price_change_1h", 0) or 0
    c24h = extra.get("price_change_24h", 0) or 0

    vol_parts = []
    if vol_5m > 0:
        vol_parts.append(f"5m: {fmt_usdt(vol_5m)}")
    if vol_1h > 0:
        vol_parts.append(f"1h: {fmt_usdt(vol_1h)}")

    extra_parts = []
    if abs(cvd_5m) > 0:
        cvd_ico = "🟢" if cvd_5m > 0 else "🔴"
        extra_parts.append(f"CVD: {cvd_ico} {fmt_usdt(abs(cvd_5m))}")
    if taker > 0:
        tb = "BUY 🟢" if taker > 1.1 else "SELL 🔴" if taker < 0.9 else "⚪"
        extra_parts.append(f"Taker: {taker:.2f}x {tb}")
    if c5m != 0:
        ico = "🟢" if c5m > 0 else "🔴" if c5m < 0 else "⚪"
        extra_parts.append(f"5m {ico} {c5m:+.2f}%")
    if c1h != 0:
        ico = "🟢" if c1h > 0 else "🔴" if c1h < 0 else "⚪"
        extra_parts.append(f"1h {ico} {c1h:+.2f}%")
    if c24h != 0:
        ico = "🟢" if c24h > 0 else "🔴" if c24h < 0 else "⚪"
        extra_parts.append(f"24h {ico} {c24h:+.2f}%")

    if vol_parts or extra_parts:
        lines.append("💹 <b>VOLUME</b>")
        if vol_parts:
            lines.append(f"  {' | '.join(vol_parts)}")
        if extra_parts:
            lines.append(f"  {' | '.join(extra_parts)}")
        lines.append("")

    # ── PnL + TIME ───────────────────────────────────
    pnl = sig.get_max_pnl_pct()
    pnl_ico = "🟢" if pnl > 0 else "⚪"
    time_str = fmt_time(now)
    lines.append(f"{pnl_ico} <b>Eng yuqori foyda:</b> {pnl:+.2f}%")
    lines.append(f"⏱ {sig.get_duration_str()} | {time_str} UTC")
    lines.append("━━━━━━━━━━━━━━━━━━━━━━━━━━")

    if sig.status in ("tp_hit", "sl_hit"):
        result = "✅ TP HIT!" if sig.status == "tp_hit" else "❌ SL HIT!"
        lines.append(f"\n{result} — Yakuniy PnL: {sig.pnl_pct:+.2f}%")

    return "\n".join(lines)


def build_full_signal_text(sig, extra: dict, price_changes: dict) -> str:
    """Build full Bookmap-style signal text"""
    from bot.formatter import fmt_price, fmt_usdt

    pnl = sig.get_pnl_pct(sig.entry_price)
    pnl_ico = "🟢" if pnl > 0 else "🔴" if pnl < 0 else "⚪"
    status_ico = "🎯" if sig.status == "tp_hit" else "🛑" if sig.status == "sl_hit" else "📍"

    if sig.direction == "LONG":
        sl_pct = ((sig.sl_price - sig.entry_price) / sig.entry_price * 100) if sig.entry_price > 0 else 0
        tp_pct = ((sig.tp_price - sig.entry_price) / sig.entry_price * 100) if sig.entry_price > 0 else 0
    else:
        sl_pct = ((sig.entry_price - sig.sl_price) / sig.entry_price * 100) if sig.entry_price > 0 else 0
        tp_pct = ((sig.entry_price - sig.tp_price) / sig.entry_price * 100) if sig.entry_price > 0 else 0

    lines = [
        f"📍 <b>#{sig.symbol} — {sig.direction} SIGNAL</b>",
        "",
        f"💵 <b>Entry:</b> {fmt_price(sig.entry_price)}$",
        f"🛑 <b>SL:</b> {fmt_price(sig.sl_price)}$ ({sl_pct:+.1f}%)",
        f"🎯 <b>TP:</b> {fmt_price(sig.tp_price)}$ ({tp_pct:+.1f}%)",
        f"⏱ <b>Davomiylik:</b> {sig.get_duration_str()}",
    ]

    if sig.status in ("tp_hit", "sl_hit"):
        result = "✅ TP HIT!" if sig.status == "tp_hit" else "❌ SL HIT!"
        lines.append(f"\n{result} — Yakuniy PnL: {sig.pnl_pct:+.2f}%")

    # ─── 📊 BOOKMAP — ORDER BOOK DEPTH ──────────────
    buy_walls = extra.get("ob_buy_walls", [])
    sell_walls = extra.get("ob_sell_walls", [])
    ob_imbalance = extra.get("ob_imbalance", 1.0)

    if buy_walls or sell_walls:
        lines.append("")
        lines.append("━━━━━━━━━━━━━━━━━━━━━━━━━━")
        lines.append("📊 <b>BOOKMAP — ORDER BOOK DEPTH</b>")
        lines.append("━━━━━━━━━━━━━━━━━━━━━━━━━━")

        # Imbalance visual
        if ob_imbalance > 1:
            bid_bar_len = min(10, int(ob_imbalance * 5))
            ask_bar_len = 10 - bid_bar_len
        else:
            ask_bar_len = min(10, int((1 / max(ob_imbalance, 0.1)) * 5))
            bid_bar_len = 10 - ask_bar_len

        bid_bar = "█" * bid_bar_len + "░" * (10 - bid_bar_len)
        ask_bar = "█" * ask_bar_len + "░" * (10 - ask_bar_len)

        if ob_imbalance > 1.2:
            ob_note = "🟢 BUY devor kuchli"
        elif ob_imbalance < 0.8:
            ob_note = "🔴 SELL devor kuchli"
        else:
            ob_note = "⚪ Balanslangan"

        lines.append(f"  🟢 [{bid_bar}] 🔴 [{ask_bar}]")
        lines.append(f"  📊 Imbalance: {ob_imbalance:.2f} — {ob_note}")

        # Top buy walls
        if buy_walls:
            lines.append(f"\n  🟢 <b>BUY Devor (Top {min(3, len(buy_walls))}):</b>")
            for w in buy_walls[:3]:
                dist = w.get("dist_pct", 0)
                lines.append(f"    • ${w['usdt']:,.0f} @ {fmt_price(w['price'])}$ ({dist:+.2f}%)")

        # Top sell walls
        if sell_walls:
            lines.append(f"\n  🔴 <b>SELL Devor (Top {min(3, len(sell_walls))}):</b>")
            for w in sell_walls[:3]:
                dist = w.get("dist_pct", 0)
                lines.append(f"    • ${w['usdt']:,.0f} @ {fmt_price(w['price'])}$ ({dist:+.2f}%)")

    # ─── 💥 LIKVIDATSIYA ZONALARI ──────────────────
    price = price_changes.get("current", sig.entry_price)
    liq_real_long = extra.get("liq_real_long", 0)
    liq_real_short = extra.get("liq_real_short", 0)
    liq_real_count = extra.get("liq_real_count", 0)

    if liq_real_count > 0:
        lines.append("")
        lines.append("━━━━━━━━━━━━━━━━━━━━━━━━━━")
        lines.append("💥 <b>LIKVIDATSIYA ZONALARI</b>")
        lines.append("━━━━━━━━━━━━━━━━━━━━━━━━━━")
        lines.append(f"  🔥 Real likvidatsiya: {liq_real_count} ta")
        if liq_real_long > 0:
            lines.append(f"    🔴 LONG liq: ${liq_real_long:,.0f}")
        if liq_real_short > 0:
            lines.append(f"    🟢 SHORT liq: ${liq_real_short:,.0f}")

    # ─── 🎯 NARX YO'NALISHI ──────────────────────
    c1m = price_changes.get("change_1m", 0)
    c5m = price_changes.get("change_5m", 0)
    c15m = price_changes.get("change_15m", 0)
    c1h = price_changes.get("change_1h", 0)
    c4h = price_changes.get("change_4h", 0)
    c24h = extra.get("price_change_24h", 0)

    cvd_1m = extra.get("cvd_1m", 0)
    cvd_5m = extra.get("cvd_5m", 0)
    cvd_dir = extra.get("cvd_direction", "neutral")
    taker_ratio = extra.get("taker_ratio", 0)
    volume_24h = extra.get("volume_24h", 0)

    has_price_data = any(v != 0 for v in [c1m, c5m, c1h, c24h])
    has_cvd = cvd_1m != 0 or cvd_5m != 0

    if has_price_data or has_cvd:
        lines.append("")
        lines.append("━━━━━━━━━━━━━━━━━━━━━━━━━━")
        lines.append("🎯 <b>NARX YO'NALISHI</b>")
        lines.append("━━━━━━━━━━━━━━━━━━━━━━━━━━")

        # Price changes
        if has_price_data:
            lines.append("  📈 <b>Narx o'zgarishlari:</b>")
            if c1m != 0:
                lines.append(f"    1m:  {c1m:+.2f}%")
            if c5m != 0:
                lines.append(f"    5m:  {c5m:+.2f}%")
            if c15m != 0:
                lines.append(f"    15m: {c15m:+.2f}%")
            if c1h != 0:
                lines.append(f"    1h:  {c1h:+.2f}%")
            if c4h != 0:
                lines.append(f"    4h:  {c4h:+.2f}%")
            if c24h != 0:
                lines.append(f"    24h: {c24h:+.2f}%")

        # CVD
        if has_cvd:
            cvd_ico = "🟢" if cvd_dir == "bullish" else "🔴" if cvd_dir == "bearish" else "⚪"
            lines.append(f"\n  {cvd_ico} <b>CVD ({cvd_dir.upper()}):</b>")
            if cvd_1m != 0:
                lines.append(f"    1m: {fmt_usdt(abs(cvd_1m))} ({'BUY' if cvd_1m > 0 else 'SELL'})")
            if cvd_5m != 0:
                lines.append(f"    5m: {fmt_usdt(abs(cvd_5m))} ({'BUY' if cvd_5m > 0 else 'SELL'})")

        # Taker
        if taker_ratio != 0:
            taker_pct = taker_ratio * 100
            sell_pct = 100 - taker_pct
            if taker_ratio > 1.1:
                taker_ico = "🟢"
                taker_note = "BUY bosim"
            elif taker_ratio < 0.9:
                taker_ico = "🔴"
                taker_note = "SELL bosim"
            else:
                taker_ico = "⚪"
                taker_note = "Neutral"
            lines.append(f"\n  {taker_ico} <b>Taker Buy/Sell:</b> {taker_pct:.1f}% / {sell_pct:.1f}% — {taker_note}")

        # Volume
        if volume_24h > 0:
            lines.append(f"\n  📊 <b>24h Hajm:</b> {fmt_usdt(volume_24h)}")

        # ─── ⚠️ G'AYRIODDIY HARAKAT (Volume Anomaly) ──────
        spike_1m = extra.get("spike_1m", 0)
        spike_5m = extra.get("spike_5m", 0)
        spike_15m = extra.get("spike_15m", 0)
        vol_baseline = extra.get("vol_baseline_1m", 0)
        vol_trend = extra.get("vol_trend", "neutral")

        if spike_5m > 100 or spike_1m > 200:
            lines.append("")
            lines.append("━━━━━━━━━━━━━━━━━━━━━━━━━━")
            lines.append("⚠️ <b>G'AYRIODDIY HARAKAT</b>")
            lines.append("━━━━━━━━━━━━━━━━━━━━━━━━━━")

            if spike_1m > 200:
                actual_1m = vol_baseline * (1 + spike_1m / 100) if vol_baseline > 0 else 0
                lines.append(f"  ┗ 1m: {fmt_usdt(actual_1m)} — o'rtacha {fmt_usdt(vol_baseline)} ({spike_1m:.1f}x) 🟢")

            if spike_5m > 100:
                actual_5m = vol_baseline * 5 * (1 + spike_5m / 100) if vol_baseline > 0 else 0
                baseline_5m = vol_baseline * 5 if vol_baseline > 0 else 0
                lines.append(f"  ┗ 5m: {fmt_usdt(actual_5m)} — o'rtacha {fmt_usdt(baseline_5m)} ({spike_5m:.1f}x) 🟢")

            if spike_15m > 50:
                actual_15m = vol_baseline * 15 * (1 + spike_15m / 100) if vol_baseline > 0 else 0
                baseline_15m = vol_baseline * 15 if vol_baseline > 0 else 0
                lines.append(f"  ┗ 15m: {fmt_usdt(actual_15m)} — o'rtacha {fmt_usdt(baseline_15m)} ({spike_15m:.1f}x) 🟡")

            if vol_trend == "up":
                lines.append(f"  ┗ Yo'nalish: 📈 O'smoqda")
            elif vol_trend == "down":
                lines.append(f"  ┗ Yo'nalish: 📉 Pasaymoqda")

    return "\n".join(lines)


def fmt_price(p: float) -> str:
    if p >= 10000:
        return f"{p:,.0f}"
    elif p >= 100:
        return f"{p:,.2f}"
    elif p >= 1:
        return f"{p:.4f}"
    elif p > 0:
        return f"{p:.6f}"
    return "—"


bot = CryptoMonitorBot()
