"""
Funding Rate History — Binance Futures API dan real data
Oxirgi 8 ta funding rate va tendensiya
"""
import asyncio
import time
import aiohttp
from loguru import logger


class FundingRateHistory:
    """Binance Funding Rate History — top symbols uchun"""

    TOP_SYMBOLS = [
        "BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT",
        "DOGEUSDT", "ADAUSDT", "AVAXUSDT", "LINKUSDT", "DOTUSDT",
    ]

    def __init__(self, update_interval: int = 300):
        self.update_interval = update_interval
        self.data = {}  # {symbol: [{"rate": float, "time": int}, ...]}
        self._running = False
        self._task = None

    async def start(self):
        self._running = True
        self._task = asyncio.create_task(self._loop())
        logger.info("🟢 Funding Rate History moduli ishga tushdi")

    async def stop(self):
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):
                pass
        logger.info("🔴 Funding Rate History to'xtatildi")

    async def _loop(self):
        await asyncio.sleep(60)
        while self._running:
            try:
                await self._fetch_all()
            except Exception as e:
                logger.debug(f"Funding Rate History xato: {e}")
            await asyncio.sleep(self.update_interval)

    async def _fetch_all(self):
        async with aiohttp.ClientSession() as session:
            tasks = [self._fetch_symbol(session, sym) for sym in self.TOP_SYMBOLS]
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _fetch_symbol(self, session, symbol: str):
        url = (
            f"https://fapi.binance.com/fapi/v1/fundingRate"
            f"?symbol={symbol}&limit=8"
        )
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=5)) as r:
                if r.status == 200:
                    data = await r.json()
                    if data:
                        rates = []
                        for entry in data:
                            rates.append({
                                "rate": float(entry.get("fundingRate", 0)) * 100,
                                "time": int(entry.get("fundingTime", 0)),
                            })
                        self.data[symbol] = rates
        except Exception as e:
            logger.debug(f"Funding Rate History {symbol} xato: {e}")

    def _get_trend(self, symbol: str) -> str:
        rates = self.data.get(symbol, [])
        if len(rates) < 2:
            return "N/A"
        recent = rates[0]["rate"]
        avg = sum(r["rate"] for r in rates) / len(rates)
        if recent > avg * 1.5:
            return "📈 Yuqoriga"
        elif recent < avg * 0.5:
            return "📉 Pastga"
        else:
            return "➡️ Barqaror"

    def _get_avg(self, symbol: str) -> float:
        rates = self.data.get(symbol, [])
        if not rates:
            return 0
        return sum(r["rate"] for r in rates) / len(rates)

    def format_text(self) -> str:
        if not self.data:
            return "⚠️ Funding Rate History mavjud emas"

        lines = [
            "💰 <b>FUNDING RATE HISTORY</b>",
            "💰 Binance Futures — Oxirgi 8 ta funding",
            "",
        ]

        sorted_data = sorted(
            self.data.items(),
            key=lambda x: abs(x[1][0]["rate"]) if x[1] else 0,
            reverse=True,
        )

        for sym, rates in sorted_data:
            if not rates:
                continue
            current = rates[0]["rate"]
            avg = self._get_avg(sym)
            trend = self._get_trend(sym)

            if current > 0.05:
                emoji = "🔴"
                note = "LONG to'laydi"
            elif current < -0.05:
                emoji = "🟢"
                note = "SHORT to'laydi"
            else:
                emoji = "⚪"
                note = "NeutraL"

            lines.append(f"{emoji} <b>{sym}</b>")
            lines.append(f"  💵 Joriy: <b>{current:+.4f}%</b> — {note}")
            lines.append(f"  📊 O'rtacha: {avg:+.4f}%")
            lines.append(f"  📈 Tendensiya: {trend}")

            history_str = " → ".join([f"{r['rate']:+.3f}%" for r in rates[:6]])
            lines.append(f"  📜 Tarix: {history_str}")
            lines.append("")

        return "\n".join(lines)


funding_rate_history = FundingRateHistory()
