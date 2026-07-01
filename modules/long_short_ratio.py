"""
Long/Short Ratio — Binance Futures API dan real data
Trader sentiment: necha % long, necha % short
"""
import asyncio
import time
import aiohttp
from loguru import logger


class LongShortRatio:
    """Binance Long/Short Ratio — top symbols uchun"""

    TOP_SYMBOLS = [
        "BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT",
        "DOGEUSDT", "ADAUSDT", "AVAXUSDT", "LINKUSDT", "DOTUSDT",
    ]

    def __init__(self, update_interval: int = 300):
        self.update_interval = update_interval
        self.data = {}  # {symbol: {"long_pct": float, "short_pct": float, "ratio": float, "timestamp": float}}
        self._running = False
        self._task = None

    async def start(self):
        self._running = True
        self._task = asyncio.create_task(self._loop())
        logger.info("🟢 Long/Short Ratio moduli ishga tushdi")

    async def stop(self):
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):
                pass
        logger.info("🔴 Long/Short Ratio to'xtatildi")

    async def _loop(self):
        await asyncio.sleep(60)
        while self._running:
            try:
                await self._fetch_all()
            except Exception as e:
                logger.debug(f"Long/Short Ratio xato: {e}")
            await asyncio.sleep(self.update_interval)

    async def _fetch_all(self):
        async with aiohttp.ClientSession() as session:
            tasks = [self._fetch_symbol(session, sym) for sym in self.TOP_SYMBOLS]
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _fetch_symbol(self, session, symbol: str):
        """Binance futuresLongShortRatioChart API"""
        url = (
            f"https://fapi.binance.com/futures/data/globalLongShortAccountRatio"
            f"?symbol={symbol}&period=5m&limit=1"
        )
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=5)) as r:
                if r.status == 200:
                    data = await r.json()
                    if data and len(data) > 0:
                        entry = data[0]
                        long_pct = float(entry.get("longAccount", 0)) * 100
                        short_pct = float(entry.get("shortAccount", 0)) * 100
                        ratio = float(entry.get("longShortRatio", 0))
                        self.data[symbol] = {
                            "long_pct": long_pct,
                            "short_pct": short_pct,
                            "ratio": ratio,
                            "timestamp": time.time(),
                        }
        except Exception as e:
            logger.debug(f"Long/Short Ratio {symbol} xato: {e}")

    def format_text(self) -> str:
        if not self.data:
            return "⚠️ Long/Short Ratio mavjud emas"

        lines = [
            "📊 <b>LONG / SHORT RATIO</b>",
            "📊 Binance Futures — Trader Sentiment",
            "",
        ]

        sorted_data = sorted(self.data.items(), key=lambda x: x[1]["ratio"], reverse=True)

        for sym, d in sorted_data:
            lp = d["long_pct"]
            sp = d["short_pct"]
            ratio = d["ratio"]
            if ratio > 1.5:
                emoji = "🟢"
                bias = "LONG dominant"
            elif ratio > 1.1:
                emoji = "🟡"
                bias = "Biroz LONG"
            elif ratio < 0.67:
                emoji = "🔴"
                bias = "SHORT dominant"
            elif ratio < 0.9:
                emoji = "🟠"
                bias = "Biroz SHORT"
            else:
                emoji = "⚪"
                bias = "Balanslangan"

            long_bar = "█" * int(lp / 10) + "░" * (10 - int(lp / 10))
            short_bar = "█" * int(sp / 10) + "░" * (10 - int(sp / 10))

            lines.append(f"{emoji} <b>{sym}</b>")
            lines.append(f"  📈 LONG:  [{long_bar}] {lp:.1f}%")
            lines.append(f"  📉 SHORT: [{short_bar}] {sp:.1f}%")
            lines.append(f"  ⚖️ Ratio: {ratio:.3f} — {bias}")
            lines.append("")

        return "\n".join(lines)

    def format_symbol(self, symbol: str) -> str:
        """Bitta symbol uchun format"""
        d = self.data.get(symbol)
        if not d:
            return f"⚠️ {symbol} uchun Long/Short Ratio mavjud emas"
        lp = d["long_pct"]
        sp = d["short_pct"]
        ratio = d["ratio"]
        return (
            f"📊 <b>{symbol}</b> L/S: {lp:.1f}% / {sp:.1f}% "
            f"(Ratio: {ratio:.3f})"
        )


long_short_ratio = LongShortRatio()
