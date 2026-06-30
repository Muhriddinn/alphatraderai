"""
Correlation Matrix — Top crypto'lar orasidagi korrelyatsiya
Binance kline API dan narx ma'lumotlarini olib, korrelyatsiya hisoblaydi
"""
import asyncio
import time
import math
import aiohttp
from loguru import logger


class CorrelationMatrix:
    """Crypto Correlation Matrix — top pairs uchun"""

    SYMBOLS = ["BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT", "DOGEUSDT"]

    def __init__(self, update_interval: int = 600):
        self.update_interval = update_interval
        self.prices = {}  # {symbol: [price1, price2, ...]}
        self.matrix = {}  # {(sym1, sym2): correlation}
        self._running = False
        self._task = None

    async def start(self):
        self._running = True
        self._task = asyncio.create_task(self._loop())
        logger.info("🟢 Correlation Matrix moduli ishga tushdi")

    async def stop(self):
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):
                pass
        logger.info("🔴 Correlation Matrix to'xtatildi")

    async def _loop(self):
        while self._running:
            try:
                await self._fetch_all()
                self._calculate_matrix()
            except Exception as e:
                logger.debug(f"Correlation Matrix xato: {e}")
            await asyncio.sleep(self.update_interval)

    async def _fetch_all(self):
        async with aiohttp.ClientSession() as session:
            tasks = [self._fetch_symbol(session, sym) for sym in self.SYMBOLS]
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _fetch_symbol(self, session, symbol: str):
        url = f"https://fapi.binance.com/fapi/v1/klines?symbol={symbol}&interval=1h&limit=168"
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=5)) as r:
                if r.status == 200:
                    data = await r.json()
                    if data:
                        closes = [float(k[4]) for k in data]
                        self.prices[symbol] = closes
        except Exception as e:
            logger.debug(f"Correlation {symbol} xato: {e}")

    @staticmethod
    def _pearson(x: list, y: list) -> float:
        n = min(len(x), len(y))
        if n < 3:
            return 0.0
        x, y = x[-n:], y[-n:]
        mx = sum(x) / n
        my = sum(y) / n
        num = sum((xi - mx) * (yi - my) for xi, yi in zip(x, y))
        dx = math.sqrt(sum((xi - mx) ** 2 for xi in x))
        dy = math.sqrt(sum((yi - my) ** 2 for yi in y))
        if dx * dy == 0:
            return 0.0
        return num / (dx * dy)

    def _calculate_matrix(self):
        self.matrix = {}
        for i, s1 in enumerate(self.SYMBOLS):
            for s2 in self.SYMBOLS[i + 1:]:
                p1 = self.prices.get(s1, [])
                p2 = self.prices.get(s2, [])
                if p1 and p2:
                    # Use percentage changes for correlation
                    r1 = [(p1[j] - p1[j - 1]) / p1[j - 1] for j in range(1, len(p1)) if p1[j - 1] != 0]
                    r2 = [(p2[j] - p2[j - 1]) / p2[j - 1] for j in range(1, len(p2)) if p2[j - 1] != 0]
                    corr = self._pearson(r1, r2)
                    self.matrix[(s1, s2)] = round(corr, 3)
                    self.matrix[(s2, s1)] = round(corr, 3)

    def _corr_emoji(self, val: float) -> str:
        if val > 0.7:
            return "🟢"
        elif val > 0.3:
            return "🟡"
        elif val > -0.3:
            return "⚪"
        elif val > -0.7:
            return "🟠"
        else:
            return "🔴"

    def _corr_bar(self, val: float) -> str:
        filled = int(abs(val) * 5)
        if val > 0:
            return "+" + "█" * filled + "░" * (5 - filled)
        else:
            return "-" + "█" * filled + "░" * (5 - filled)

    def format_text(self) -> str:
        if not self.matrix:
            return "⚠️ Correlation Matrix mavjud emas"

        lines = [
            "🔗 <b>CORRELATION MATRIX</b>",
            "🔗 1H narx o'zgarishlari asosida (so'nggi 7 kun)",
            "",
        ]

        short_names = [s.replace("USDT", "") for s in self.SYMBOLS]
        header = "      " + "  ".join(f"{n:>6}" for n in short_names)
        lines.append(f"<code>{header}</code>")

        for s1 in self.SYMBOLS:
            n1 = s1.replace("USDT", "")
            row = f"{n1:>5} "
            for s2 in self.SYMBOLS:
                if s1 == s2:
                    row += "  1.00 "
                else:
                    corr = self.matrix.get((s1, s2), 0)
                    row += f" {corr:+.2f} "
            lines.append(f"<code>{row}</code>")

        lines.append("")
        lines.append("📊 <b>Eng kuchli korrelyatsiyalar:</b>")

        sorted_pairs = sorted(
            [(k, v) for k, v in self.matrix.items() if k[0] < k[1]],
            key=lambda x: abs(x[1]),
            reverse=True,
        )

        for (s1, s2), corr in sorted_pairs[:5]:
            n1 = s1.replace("USDT", "")
            n2 = s2.replace("USDT", "")
            e = self._corr_emoji(corr)
            bar = self._corr_bar(corr)
            lines.append(f"  {e} {n1}/{n2}: {corr:+.3f} [{bar}]")

        return "\n".join(lines)


correlation_matrix = CorrelationMatrix()
