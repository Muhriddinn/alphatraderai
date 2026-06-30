"""
Top Trader Sentiment — Binance Futures API dan real data
Top trader'lar qanday pozitsiyalarda (long/short)
"""
import asyncio
import time
import aiohttp
from loguru import logger


class TopTraderSentiment:
    """Binance Top Trader Long/Short Account Ratio + Position Ratio"""

    TOP_SYMBOLS = [
        "BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT",
        "DOGEUSDT", "ADAUSDT", "AVAXUSDT", "LINKUSDT", "DOTUSDT",
    ]

    def __init__(self, update_interval: int = 600):
        self.update_interval = update_interval
        self.account_data = {}  # {symbol: {"long": %, "short": %, "ratio": float}}
        self.position_data = {}  # {symbol: {"long": %, "short": %, "ratio": float}}
        self._running = False
        self._task = None

    async def start(self):
        self._running = True
        self._task = asyncio.create_task(self._loop())
        logger.info("🟢 Top Trader Sentiment moduli ishga tushdi")

    async def stop(self):
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):
                pass
        logger.info("🔴 Top Trader Sentiment to'xtatildi")

    async def _loop(self):
        while self._running:
            try:
                await self._fetch_all()
            except Exception as e:
                logger.debug(f"Top Trader Sentiment xato: {e}")
            await asyncio.sleep(self.update_interval)

    async def _fetch_all(self):
        async with aiohttp.ClientSession() as session:
            tasks = []
            for sym in self.TOP_SYMBOLS:
                tasks.append(self._fetch_account_ratio(session, sym))
                tasks.append(self._fetch_position_ratio(session, sym))
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _fetch_account_ratio(self, session, symbol: str):
        url = (
            f"https://fapi.binance.com/futures/data/topLongShortAccountRatio"
            f"?symbol={symbol}&period=5m&limit=1"
        )
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=5)) as r:
                if r.status == 200:
                    data = await r.json()
                    if data and len(data) > 0:
                        entry = data[0]
                        self.account_data[symbol] = {
                            "long": float(entry.get("longAccount", 0)) * 100,
                            "short": float(entry.get("shortAccount", 0)) * 100,
                            "ratio": float(entry.get("longShortRatio", 0)),
                        }
        except Exception as e:
            logger.debug(f"Top Account Ratio {symbol} xato: {e}")

    async def _fetch_position_ratio(self, session, symbol: str):
        url = (
            f"https://fapi.binance.com/futures/data/topLongShortPositionRatio"
            f"?symbol={symbol}&period=5m&limit=1"
        )
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=5)) as r:
                if r.status == 200:
                    data = await r.json()
                    if data and len(data) > 0:
                        entry = data[0]
                        self.position_data[symbol] = {
                            "long": float(entry.get("longAccount", 0)) * 100,
                            "short": float(entry.get("shortAccount", 0)) * 100,
                            "ratio": float(entry.get("longShortRatio", 0)),
                        }
        except Exception as e:
            logger.debug(f"Top Position Ratio {symbol} xato: {e}")

    def format_text(self) -> str:
        if not self.account_data and not self.position_data:
            return "⚠️ Top Trader Sentiment mavjud emas"

        lines = [
            "🐋 <b>TOP TRADER SENTIMENT</b>",
            "🐋 Binance — Top trader pozitsiyalari",
            "",
            "📊 <b>Account Ratio</b> (necha % long/short hisob):",
            "",
        ]

        for sym in self.TOP_SYMBOLS:
            acct = self.account_data.get(sym)
            pos = self.position_data.get(sym)
            if not acct:
                continue

            ar = acct["ratio"]
            pr = pos["ratio"] if pos else 0

            if ar > 1.5:
                emoji = "🟢"
                bias = "LONG dominant"
            elif ar > 1.1:
                emoji = "🟡"
                bias = "Biroz LONG"
            elif ar < 0.67:
                emoji = "🔴"
                bias = "SHORT dominant"
            elif ar < 0.9:
                emoji = "🟠"
                bias = "Biroz SHORT"
            else:
                emoji = "⚪"
                bias = "Balans"

            lines.append(f"{emoji} <b>{sym}</b>")
            lines.append(
                f"  📊 Account: {acct['long']:.1f}% L / {acct['short']:.1f}% S "
                f"(Ratio: {ar:.3f})"
            )
            if pos:
                lines.append(
                    f"  📈 Position: {pos['long']:.1f}% L / {pos['short']:.1f}% S "
                    f"(Ratio: {pr:.3f})"
                )
            lines.append(f"  💡 {bias}")
            lines.append("")

        # Summary
        all_acct_ratios = [d["ratio"] for d in self.account_data.values()]
        if all_acct_ratios:
            avg_ratio = sum(all_acct_ratios) / len(all_acct_ratios)
            if avg_ratio > 1.2:
                summary = "🟢 Top trader'lar umuman LONG tarafda"
            elif avg_ratio < 0.8:
                summary = "🔴 Top trader'lar umuman SHORT tarafda"
            else:
                summary = "⚪ Top trader'lar taqsimlangan"
            lines.append(f"📋 <b>Umumiy xulosa:</b> {summary}")
            lines.append(f"📊 O'rtacha ratio: {avg_ratio:.3f}")

        return "\n".join(lines)


top_trader_sentiment = TopTraderSentiment()
