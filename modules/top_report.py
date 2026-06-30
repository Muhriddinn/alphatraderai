"""
ALPHATRADERAI — Top Coin Report (M5/H1/H4 Top Movers)
"""
import asyncio
from datetime import datetime
from loguru import logger
from core.state_manager import state_manager
from bot.formatter import build_top_report


class TopReportModule:
    def __init__(self, bot_send_callback):
        self.send_callback = bot_send_callback
        self._running = False

    async def start(self):
        self._running = True
        asyncio.create_task(self._report_loop())
        logger.info("✅ Top Report module started")

    async def _report_loop(self):
        while self._running:
            await asyncio.sleep(300)
            try:
                await self._generate_and_send()
            except Exception as e:
                logger.error(f"Top report error: {e}")

    async def _generate_and_send(self):
        from modules.price_tracker import price_tracker

        symbols = await state_manager.get_symbols("binance", "futures")
        symbol_list = list(symbols)

        oi_up_data = []
        oi_down_data = []
        liq_data = []
        m5_data = []
        h1_data = []
        h4_data = []

        for symbol in symbol_list[:200]:
            try:
                # OI o'zgarish
                oi_history = await state_manager.get_oi_history("binance", symbol, count=30)
                if len(oi_history) >= 2:
                    newest = oi_history[0].get("oi_usdt", 0)
                    oldest = oi_history[-1].get("oi_usdt", 0)
                    if oldest > 0 and newest > 0:
                        change = ((newest - oldest) / oldest) * 100
                        entry = {"symbol": symbol, "change": change}
                        if change > 1:
                            oi_up_data.append(entry)
                        elif change < -1:
                            oi_down_data.append(entry)
            except Exception:
                pass

            try:
                # Likvidatsiyalar
                liqs = await state_manager.get_liquidations_window("binance", symbol, seconds=300)
                if liqs:
                    total = sum(l["usdt"] for l in liqs)
                    if total > 10000:
                        liq_data.append({"symbol": symbol, "total": total})
            except Exception:
                pass

            # M5/H1/H4 price changes
            try:
                pc = price_tracker.get_price_changes(symbol)
                if pc:
                    c5m = pc.get("change_5m", 0) or 0
                    c1h = pc.get("change_1h", 0) or 0
                    c4h = pc.get("change_4h", 0) or 0
                    if c5m != 0:
                        m5_data.append({"symbol": symbol, "change": c5m})
                    if c1h != 0:
                        h1_data.append({"symbol": symbol, "change": c1h})
                    if c4h != 0:
                        h4_data.append({"symbol": symbol, "change": c4h})
            except Exception:
                pass

        # Sort: biggest gainers and losers
        m5_data.sort(key=lambda x: abs(x["change"]), reverse=True)
        h1_data.sort(key=lambda x: abs(x["change"]), reverse=True)
        h4_data.sort(key=lambda x: abs(x["change"]), reverse=True)
        oi_up_data.sort(key=lambda x: x["change"], reverse=True)
        oi_down_data.sort(key=lambda x: x["change"])
        liq_data.sort(key=lambda x: x["total"], reverse=True)

        if not (oi_up_data or oi_down_data or liq_data or m5_data or h1_data or h4_data):
            logger.debug("Top report: ma'lumot yo'q, skip")
            return

        report = build_top_report(
            top_volume=[],
            top_oi_up=oi_up_data[:5],
            top_oi_down=oi_down_data[:5],
            top_liq=liq_data[:5],
            top_m5=m5_data[:5],
            top_h1=h1_data[:5],
            top_h4=h4_data[:5],
        )

        await self.send_callback(report)
        logger.info("📊 Top report yuborildi")

    async def stop(self):
        self._running = False
