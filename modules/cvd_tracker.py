"""
CRYPTO MONITOR PRO — CVD (Cumulative Volume Delta) Tracker
Buy volume - Sell volume = Delta
"""
import asyncio
import time
from datetime import datetime
from collections import defaultdict
from loguru import logger
from core.models import TradeData, Direction


class CVDTracker:
    """
    CVD = Cumulative Volume Delta
    Buy USDT - Sell USDT = Delta

    Agar CVD o'sib, narx tushsa → divergence (kuchli signal)
    Agar CVD tushib, narx o'ssa → divergence (kuchli signal)
    """

    def __init__(self):
        # {symbol: [(delta, timestamp), ...]}
        self._cvd_history: dict[str, list] = defaultdict(list)
        self._current_delta: dict[str, float] = defaultdict(float)
        self._running = False

    async def start(self):
        self._running = True
        asyncio.create_task(self._cleanup_loop())
        logger.info("✅ CVD Tracker started")

    def process_trade(self, trade: TradeData):
        """Har bir trade uchun delta yangilanadi"""
        symbol = trade.symbol
        now = time.time()

        if trade.side == Direction.BUY:
            delta = trade.usdt_value
        else:
            delta = -trade.usdt_value

        self._current_delta[symbol] += delta
        self._cvd_history[symbol].append((self._current_delta[symbol], now))

        # Faqat so'nggi 1 soat
        if len(self._cvd_history[symbol]) > 10000:
            self._cvd_history[symbol] = self._cvd_history[symbol][-10000:]

    def get_cvd_data(self, symbol: str) -> dict:
        """
        Qaytaradi:
        {
            "cvd_current": 1234567.0,
            "cvd_1m": +234567.0,
            "cvd_5m": -567890.0,
            "cvd_15m": +1200000.0,
            "cvd_direction": "bullish",
            "cvd_trend": "accelerating",
        }
        """
        history = self._cvd_history.get(symbol, [])
        if not history:
            return {
                "cvd_current": 0, "cvd_1m": 0, "cvd_5m": 0, "cvd_15m": 0,
                "cvd_direction": "neutral", "cvd_trend": "flat"
            }

        now = time.time()
        current_cvd = history[-1][0]

        def get_past_cvd(seconds_ago: int) -> float:
            cutoff = now - seconds_ago
            for cvd, ts in history:
                if ts <= cutoff:
                    return cvd
            return history[0][0] if history else 0

        cvd_1m_ago = get_past_cvd(60)
        cvd_5m_ago = get_past_cvd(300)
        cvd_15m_ago = get_past_cvd(900)

        cvd_change_1m = current_cvd - cvd_1m_ago
        cvd_change_5m = current_cvd - cvd_5m_ago
        cvd_change_15m = current_cvd - cvd_15m_ago

        direction = "bullish" if cvd_change_1m > 0 else "bearish"

        # CVD trend — tezlashmoqda yoki sekinlashmoqda
        if abs(cvd_change_1m) > abs(cvd_change_5m) / 5 * 1.2:
            trend = "accelerating"
        elif abs(cvd_change_1m) < abs(cvd_change_5m) / 5 * 0.8:
            trend = "decelerating"
        else:
            trend = "steady"

        return {
            "cvd_current": current_cvd,
            "cvd_1m": cvd_change_1m,
            "cvd_5m": cvd_change_5m,
            "cvd_15m": cvd_change_15m,
            "cvd_direction": direction,
            "cvd_trend": trend,
        }

    def check_divergence(self, symbol: str, price_change_1m: float) -> str | None:
        """
        Divergence tekshiradi:
        - CVD o'sadi + narx tushadi = BULLISH DIVERGENCE
        - CVD tushadi + narx o'sadi = BEARISH DIVERGENCE
        """
        cvd = self.get_cvd_data(symbol)
        cvd_1m = cvd["cvd_1m"]

        if abs(cvd_1m) < 10000:  # Juda kichik — skip
            return None
        if abs(price_change_1m) < 0.1:  # Narx deyarli o'zgarmagan
            return None

        if cvd_1m > 0 and price_change_1m < -0.2:
            return "BULLISH_DIVERGENCE"
        elif cvd_1m < 0 and price_change_1m > 0.2:
            return "BEARISH_DIVERGENCE"

        return None

    async def _cleanup_loop(self):
        while self._running:
            now = time.time()
            cutoff = now - 3600
            for symbol in list(self._cvd_history.keys()):
                self._cvd_history[symbol] = [
                    (c, t) for c, t in self._cvd_history[symbol]
                    if t >= cutoff
                ]
            await asyncio.sleep(300)

    async def stop(self):
        self._running = False


# Global instance
cvd_tracker = CVDTracker()
