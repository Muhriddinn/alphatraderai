"""
CRYPTO MONITOR PRO — Price Change Tracker (To'liq tuzatilgan)

Narx o'zgarishi: 1s / 1m / 5m / 1h

Tuzatishlar:
1. Threshold pasaytirildi: 1m da 0.5%, 5m da 1.5%
2. Cooldown kamaytirildi: 300s → 120s
3. Scan tezligi oshirildi: 10s → 5s
4. Ko'proq symbol skanerlaydi: 300 → 500
"""
import asyncio
from datetime import datetime
from collections import defaultdict
from loguru import logger


class PriceTracker:
    """
    Har bir coin uchun narx tarixini saqlaydi.
    O'zgarish % ni hisoblaydi: 1s, 1m, 5m, 1h
    """

    def __init__(self):
        self._history: dict[str, list] = defaultdict(list)
        self._running = False
        # Trend kuzatuvi: symbol -> {"direction": "up"/"down", "since_ts": float, "since_price": float}
        self._trend: dict[str, dict] = {}

    async def start(self):
        self._running = True
        asyncio.create_task(self._cleanup_loop())
        logger.info("✅ Price Tracker started")

    def update_price(self, symbol: str, price: float):
        """Mark price stream dan har yangilanishda chaqiriladi"""
        if price <= 0:
            return
        now = datetime.utcnow().timestamp()
        self._history[symbol].append((price, now))
        # Faqat so'nggi 1 soatni saqlash (max 7200 ta narq nuqtasi)
        if len(self._history[symbol]) > 7200:
            self._history[symbol] = self._history[symbol][-7200:]
        self._update_trend(symbol, price, now)

    def _update_trend(self, symbol: str, price: float, now: float):
        """
        Narx tendensiyasini kuzatadi: qachondan beri ko'tarilmoqda yoki
        tushmoqda. Yo'nalish 0.05% dan kam o'zgarsa "neutral" hisoblanadi
        va hisoblagich qayta boshlanmaydi (mikro shovqinni e'tiborsiz
        qoldirish uchun).
        """
        state = self._trend.get(symbol)
        if state is None:
            self._trend[symbol] = {"direction": "neutral", "since_ts": now, "since_price": price, "last_price": price}
            return

        last_price = state["last_price"]
        if last_price <= 0:
            state["last_price"] = price
            return

        change_pct = (price - last_price) / last_price * 100
        if change_pct > 0.02:
            new_dir = "up"
        elif change_pct < -0.02:
            new_dir = "down"
        else:
            new_dir = state["direction"]

        if new_dir != state["direction"] and new_dir != "neutral":
            state["direction"] = new_dir
            state["since_ts"] = now
            state["since_price"] = last_price

        state["last_price"] = price

    def get_trend(self, symbol: str) -> dict:
        """{'direction': 'up'/'down'/'neutral', 'since_seconds': int, 'change_pct': float}"""
        state = self._trend.get(symbol)
        if not state:
            return {"direction": "neutral", "since_seconds": 0, "change_pct": 0.0}
        now = datetime.utcnow().timestamp()
        since_seconds = int(now - state["since_ts"])
        current = self._history[symbol][-1][0] if self._history.get(symbol) else state["last_price"]
        change_pct = ((current - state["since_price"]) / state["since_price"] * 100) if state["since_price"] > 0 else 0.0
        return {"direction": state["direction"], "since_seconds": since_seconds, "change_pct": change_pct}

    def get_price_changes(self, symbol: str) -> dict:
        """
        Qaytaradi:
        {
            "current": 105240.0,
            "change_1s": +0.12,
            "change_1m": -0.34,
            "change_5m": +1.23,
            "change_1h": +2.45
        }
        """
        history = self._history.get(symbol, [])
        if not history:
            return {"current": 0, "change_1s": 0, "change_1m": 0, "change_5m": 0, "change_15m": 0, "change_1h": 0, "change_4h": 0}

        now = datetime.utcnow().timestamp()
        current_price = history[-1][0]

        def get_change(seconds_ago: int) -> float:
            cutoff = now - seconds_ago
            past = None
            for price, ts in history:
                if ts <= cutoff:
                    past = price
                else:
                    break
            if past and past > 0:
                return ((current_price - past) / past) * 100
            return 0.0

        return {
            "current": current_price,
            "change_1s": get_change(1),
            "change_1m": get_change(60),
            "change_5m": get_change(300),
            "change_15m": get_change(900),
            "change_1h": get_change(3600),
            "change_4h": get_change(14400),
        }

    async def _cleanup_loop(self):
        while self._running:
            now = datetime.utcnow().timestamp()
            cutoff = now - 14400  # Keep 4 hours for change_4h
            for symbol in list(self._history.keys()):
                self._history[symbol] = [
                    (p, t) for p, t in self._history[symbol]
                    if t >= cutoff
                ]
            await asyncio.sleep(300)

    async def stop(self):
        self._running = False


# Global instance
price_tracker = PriceTracker()


class PriceChangeScanner:
    """
    Narx keskin o'zgarishini aniqlaydi.

    Tuzatilgan thresholdlar:
    - 1 daqiqada 0.5% → signal (oldin 1.5% edi)
    - 5 daqiqada 1.5% → signal (oldin 3.0% edi)
    """

    def __init__(self, event_callback):
        self.event_callback = event_callback
        self._running = False

    async def start(self):
        self._running = True
        asyncio.create_task(self._scan_loop())
        logger.info("✅ Price Change Scanner started")

    async def _scan_loop(self):
        """Har 5 soniyada barcha coinlarni tekshiradi (oldin 10s edi)"""
        while self._running:
            await asyncio.sleep(5)
            try:
                await self._check_all()
            except Exception as e:
                logger.debug(f"Price scan error: {e}")

    async def _check_all(self):
        from core.state_manager import state_manager
        symbols = await state_manager.get_symbols("binance", "futures")

        checked = 0
        triggered = 0

        for symbol in list(symbols)[:500]:  # 500 ta symbol (oldin 300 edi)
            changes = price_tracker.get_price_changes(symbol)
            if changes["current"] <= 0:
                continue

            checked += 1
            c1m = changes["change_1m"]
            c5m = changes["change_5m"]
            abs_1m = abs(c1m)
            abs_5m = abs(c5m)

            fire = False
            label = ""

            # Threshold: 1m da 1.0%
            if abs_1m >= 1.0:
                fire = True
                label = f"1 daqiqada {c1m:+.2f}%"
            # Threshold: 5m da 2.5%
            elif abs_5m >= 2.5:
                fire = True
                label = f"5 daqiqada {c5m:+.2f}%"

            if fire:
                if not await state_manager.is_event_sent("binance", symbol, "price_change"):
                    # Cooldown: 120s (oldin 300s edi)
                    await state_manager.mark_event_sent(
                        "binance", symbol, "price_change", cooldown=120
                    )
                    triggered += 1
                    logger.info(f"💹 Keskin narx o'zgarish: {symbol} {label}")
                    await self.event_callback(symbol, changes, label)

        if checked > 0 and triggered > 0:
            logger.info(f"💹 Price scan: {checked} symbol tekshirildi, {triggered} ta signal")

    async def stop(self):
        self._running = False
