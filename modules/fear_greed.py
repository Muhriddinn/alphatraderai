"""
Fear & Greed Index — alternative.me API dan real data
"""
import asyncio
import time
import aiohttp
from loguru import logger


class FearGreedIndex:
    """Crypto Fear & Greed Index — real-time"""

    def __init__(self, update_interval: int = 300):
        self.update_interval = update_interval
        self.current_value = None
        self.current_label = "N/A"
        self.history = []  # [(timestamp, value, label), ...]
        self._running = False
        self._task = None

    async def start(self):
        self._running = True
        self._task = asyncio.create_task(self._loop())
        logger.info("🟢 Fear & Greed Index moduli ishga tushdi")

    async def stop(self):
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):
                pass
        logger.info("🔴 Fear & Greed Index to'xtatildi")

    async def _loop(self):
        while self._running:
            try:
                await self._fetch()
            except Exception as e:
                logger.debug(f"Fear & Greed xato: {e}")
            await asyncio.sleep(self.update_interval)

    async def _fetch(self):
        url = "https://api.alternative.me/fng/?limit=30&format=json"
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as r:
                if r.status == 200:
                    data = await r.json()
                    entries = data.get("data", [])
                    if entries:
                        latest = entries[0]
                        self.current_value = int(latest.get("value", 0))
                        self.current_label = latest.get("value_classification", "N/A")
                        self.history = []
                        for e in entries:
                            self.history.append((
                                int(e.get("timestamp", 0)),
                                int(e.get("value", 0)),
                                e.get("value_classification", "N/A"),
                            ))
                        logger.debug(f"Fear & Greed: {self.current_value} ({self.current_label})")

    def get_emoji(self, value: int | None = None) -> str:
        v = value if value is not None else self.current_value
        if v is None:
            return "⚪"
        if v <= 20:
            return "😱"
        elif v <= 40:
            return "😰"
        elif v <= 60:
            return "😐"
        elif v <= 80:
            return "😊"
        else:
            return "🤑"

    def get_bar(self, value: int | None = None, length: int = 10) -> str:
        v = value if value is not None else self.current_value
        if v is None:
            return "░" * length
        filled = int(v / 100 * length)
        return "█" * filled + "░" * (length - filled)

    def get_trend(self) -> str:
        if len(self.history) < 7:
            return "N/A"
        recent_avg = sum(h[1] for h in self.history[:7]) / 7
        older_avg = sum(h[1] for h in self.history[7:14]) / max(len(self.history[7:14]), 1)
        diff = recent_avg - older_avg
        if diff > 5:
            return f"📈 Oshmoqda (+{diff:.1f})"
        elif diff < -5:
            return f"📉 Pasaymoqda ({diff:.1f})"
        else:
            return f"➡️ Barqaror ({diff:+.1f})"

    def format_text(self) -> str:
        if self.current_value is None:
            return "⚠️ Fear & Greed Index mavjud emas"

        emoji = self.get_emoji()
        bar = self.get_bar()
        trend = self.get_trend()
        last_7 = self.history[:7]

        lines = [
            f"🧠 <b>FEAR & GREED INDEX</b>",
            f"",
            f"{emoji} <b>Joriy: {self.current_value}/100</b> — {self.current_label}",
            f"📊 [{bar}] {self.current_value}%",
            f"📈 Tendensiya: {trend}",
            f"",
            f"📅 <b>So'nggi 7 kun:</b>",
        ]

        for ts, val, lbl in last_7:
            from datetime import datetime
            dt = datetime.utcfromtimestamp(ts).strftime("%d.%m")
            e = self.get_emoji(val)
            b = self.get_bar(val, 8)
            lines.append(f"  {dt}: {e} {val} [{b}] {lbl}")

        return "\n".join(lines)


fear_greed_index = FearGreedIndex()
