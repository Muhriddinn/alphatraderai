"""
CRYPTO MONITOR PRO — Order Book Wall Tracker (Yangi modul)

Nima qiladi:
- Katta buy/sell devorlarni kuzatadi
- Har bir devor qancha vaqtdan beri turganligi
- Devor yo'qolsa — SPOOFING signal
- Real devor vs qisqa muddatli devor farqi
- TOP-3 buy va sell devorlarini ko'rsatadi
"""
import asyncio
from datetime import datetime
from collections import defaultdict
from loguru import logger
import aiohttp

from config.settings import settings
from core.models import OrderBookEvent, Exchange
from core.state_manager import state_manager
from core.rate_limiter import rate_limiter


# Devor kamida shu miqdorda bo'lishi kerak (USDT)
WALL_MIN_USDT = 500_000   # 500K$

# Spoofing: devor kamida 60s turib, keyin yo'qolsa = shubhali
# Avvalgi 30s edi — bu juda kichik, OB tabiiy o'zgarishlarini ham flag qilardi
SPOOF_MIN_DURATION = 120   # kamida 120s turishi kerak
SPOOF_MAX_SECONDS = 600   # 10 daqiqadan keyin yo'qolsa — normal (spam emas)

# OB refresh interval — 30 soniya (weight limit uchun)
OB_REFRESH_SECONDS = 30


class OrderBookWallTracker:
    """
    Order book devorlarini real vaqtda kuzatadi.
    REST polling (5 soniyada bir) ishlatiladi — WebSocket ga o'tish keyingi bosqich.
    """

    def __init__(self, event_callback):
        self.event_callback = event_callback
        self._running = False

        # { symbol: { "price_level": { "side": "buy/sell", "usdt": float, "first_seen": ts, "last_seen": ts } } }
        self._walls: dict[str, dict] = defaultdict(dict)

        # Kuzatiladigan symbollar (top coinlar)
        self._watch_symbols: list[str] = []

        # Spoofing log (xabar takror ketmasin)
        self._spoof_sent: dict[str, float] = {}

    async def start(self, symbols: list[str] = None):
        self._running = True
        if symbols:
            self._watch_symbols = symbols[:30]
        if not self._watch_symbols:
            logger.info("⏳ OB Wall Tracker — symbollar kutmoqda (update_symbols kutmoqda)")
            return
        asyncio.create_task(self._poll_loop())
        logger.info(f"✅ OB Wall Tracker started ({len(self._watch_symbols)} symbol)")

    def update_symbols(self, symbols: list[str]):
        """Dinamik ravishda symbollar ro'yxatini yangilash"""
        old_count = len(self._watch_symbols)
        self._watch_symbols = symbols[:30]
        if old_count == 0 and self._watch_symbols and self._running:
            # Poll loop hali start bo'lmagan — endi boshlash
            asyncio.create_task(self._poll_loop())
            logger.info(f"✅ OB Wall Tracker started ({len(self._watch_symbols)} symbol)")
        else:
            logger.debug(f"OB tracker symbols yangilandi: {len(self._watch_symbols)} symbol")

    async def _poll_loop(self):
        """Har 5 soniyada order book yangilaydi"""
        session = aiohttp.ClientSession()
        try:
            while self._running:
                for symbol in self._watch_symbols:
                    try:
                        await self._fetch_and_analyze(session, symbol)
                    except Exception as e:
                        logger.debug(f"OB error {symbol}: {e}")
                    await asyncio.sleep(1.0)  # Rate limit: 1 soniya oraliq

                await asyncio.sleep(OB_REFRESH_SECONDS)
        finally:
            await session.close()

    async def _fetch_and_analyze(self, session: aiohttp.ClientSession, symbol: str):
        """OB snapshot olish va devorlarni tahlil qilish"""
        url = "https://fapi.binance.com/fapi/v1/depth"
        params = {"symbol": symbol, "limit": 20}

        await rate_limiter.acquire(weight=5)
        async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=5)) as resp:
            if resp.status != 200:
                return
            data = await resp.json()

        bids = data.get("bids", [])  # [[price, qty], ...]
        asks = data.get("asks", [])
        now = datetime.utcnow().timestamp()

        # Ticker narxini olish
        ticker = await state_manager.get_ticker("binance", symbol)
        current_price = ticker["price"] if ticker else 0
        if current_price <= 0:
            return

        # Hozirgi devorlarni hisoblash
        current_walls = {}

        for price_str, qty_str in bids:
            price = float(price_str)
            qty = float(qty_str)
            usdt = price * qty
            if usdt >= WALL_MIN_USDT:
                key = f"buy_{price_str}"
                current_walls[key] = {"side": "buy", "price": price, "usdt": usdt}

        for price_str, qty_str in asks:
            price = float(price_str)
            qty = float(qty_str)
            usdt = price * qty
            if usdt >= WALL_MIN_USDT:
                key = f"sell_{price_str}"
                current_walls[key] = {"side": "sell", "price": price, "usdt": usdt}

        existing = self._walls[symbol]

        # Yangi devorlarni qo'shish yoki yangilash
        for key, wall_data in current_walls.items():
            if key in existing:
                existing[key]["last_seen"] = now
                existing[key]["usdt"] = wall_data["usdt"]  # Yangilash
            else:
                existing[key] = {
                    **wall_data,
                    "first_seen": now,
                    "last_seen": now,
                }

        # Yo'qolgan devorlarni tekshirish (spoofing)
        disappeared = []
        for key in list(existing.keys()):
            if key not in current_walls:
                wall = existing[key]
                duration = wall["last_seen"] - wall["first_seen"]
                # Faqat kamida 60s turib yo'qolgan devorlar = shubhali
                # 5 daqiqadan oshsa — normal OB o'zgarishi (spam emas)
                if SPOOF_MIN_DURATION <= duration <= SPOOF_MAX_SECONDS and wall["usdt"] >= settings.orderbook_wall_min_usdt:
                    disappeared.append({**wall, "duration": duration, "key": key})
                del existing[key]

        for spoof_wall in disappeared:
            await self._on_spoof_detected(symbol, spoof_wall, current_price)

        # Signal: katta devorlar nisbati
        await self._check_imbalance(symbol, existing, current_price)

    async def _on_spoof_detected(self, symbol: str, wall: dict, current_price: float):
        """Spoofing aniqlandi — signal yuborish"""
        spoof_key = f"{symbol}_{wall['key']}"
        now = datetime.utcnow().timestamp()

        # Cooldown: bir devor uchun 5 daqiqada bir signal
        if spoof_key in self._spoof_sent and now - self._spoof_sent[spoof_key] < 300:
            return

        self._spoof_sent[spoof_key] = now
        dist_pct = abs(wall["price"] - current_price) / current_price * 100

        logger.warning(
            f"⚠️ SPOOFING: {symbol} | "
            f"{wall['side'].upper()} {wall['usdt']/1e6:.1f}M$ "
            f"@ {wall['price']:,.2f} | "
            f"faqat {wall['duration']:.0f}s turdi"
        )

        # State managerga spoofing ma'lumotini saqlash
        await state_manager.mark_event_sent("binance", symbol, "spoofing", cooldown=300)

    async def _check_imbalance(self, symbol: str, walls: dict, current_price: float):
        """Buy/Sell devorlar nisbatini tekshirish va signal berish"""
        if not walls:
            return

        # Top-3 buy va sell devorlarni ajratish
        buy_walls = sorted(
            [w for w in walls.values() if w["side"] == "buy"],
            key=lambda x: x["usdt"], reverse=True
        )[:3]

        sell_walls = sorted(
            [w for w in walls.values() if w["side"] == "sell"],
            key=lambda x: x["usdt"], reverse=True
        )[:3]

        if not buy_walls and not sell_walls:
            return

        total_buy = sum(w["usdt"] for w in buy_walls)
        total_sell = sum(w["usdt"] for w in sell_walls)

        if total_buy == 0 or total_sell == 0:
            return

        ratio = total_buy / total_sell if total_sell > 0 else 99

        # Signal faqat katta imbalance bo'lganda
        if ratio < settings.orderbook_imbalance_ratio and ratio > (1 / settings.orderbook_imbalance_ratio):
            return

        if await state_manager.is_event_sent("binance", symbol, "orderbook_wall"):
            return

        now = datetime.utcnow().timestamp()

        # Eng katta buy devori
        top_buy = buy_walls[0] if buy_walls else None
        top_sell = sell_walls[0] if sell_walls else None

        # Vaqt hisoblash
        def wall_age(wall: dict) -> int:
            return int(now - wall["first_seen"])

        # Devor uzoqligini hisoblash
        def wall_dist(wall: dict) -> float:
            return (wall["price"] - current_price) / current_price * 100

        event = OrderBookEvent(
            symbol=symbol,
            exchange=Exchange.BINANCE,
            buy_wall_usdt=total_buy,
            sell_wall_usdt=total_sell,
            buy_wall_price=top_buy["price"] if top_buy else 0,
            sell_wall_price=top_sell["price"] if top_sell else 0,
            buy_wall_distance_pct=wall_dist(top_buy) if top_buy else 0,
            sell_wall_distance_pct=wall_dist(top_sell) if top_sell else 0,
            imbalance_ratio=round(ratio, 2),
            current_price=current_price,
            timestamp=datetime.utcnow(),
        )

        # TOP-3 devorlar ro'yxati
        event.extra_walls = {
            "buy_walls": [
                {
                    "usdt": w["usdt"],
                    "price": w["price"],
                    "dist_pct": wall_dist(w),
                    "age_seconds": wall_age(w),
                }
                for w in buy_walls
            ],
            "sell_walls": [
                {
                    "usdt": w["usdt"],
                    "price": w["price"],
                    "dist_pct": wall_dist(w),
                    "age_seconds": wall_age(w),
                }
                for w in sell_walls
            ],
        }

        await state_manager.mark_event_sent("binance", symbol, "orderbook_wall", cooldown=180)
        logger.info(
            f"📚 OB Wall: {symbol} | "
            f"Buy: {total_buy/1e6:.1f}M$ | "
            f"Sell: {total_sell/1e6:.1f}M$ | "
            f"Nisbat: {ratio:.1f}x"
        )
        await self.event_callback(event)

    def get_walls(self, symbol: str) -> dict:
        """Boshqa modullar uchun: hozirgi devorlar"""
        walls = self._walls.get(symbol, {})
        now = datetime.utcnow().timestamp()

        buy_walls = sorted(
            [w for w in walls.values() if w["side"] == "buy"],
            key=lambda x: x["usdt"], reverse=True
        )[:3]

        sell_walls = sorted(
            [w for w in walls.values() if w["side"] == "sell"],
            key=lambda x: x["usdt"], reverse=True
        )[:3]

        def wall_age(wall):
            return int(now - wall["first_seen"])

        return {
            "buy_walls": [
                {"usdt": w["usdt"], "price": w["price"], "age_seconds": wall_age(w)}
                for w in buy_walls
            ],
            "sell_walls": [
                {"usdt": w["usdt"], "price": w["price"], "age_seconds": wall_age(w)}
                for w in sell_walls
            ],
        }

    def get_walls_with_price(self, symbol: str, current_price: float) -> dict | None:
        """
        Har doim (imbalance signal bo'lmasa ham) ishlatish uchun:
        TOP-3 buy/sell devor + masofa% + imbalance ratio.
        AlertEngine har bir signalga Order Book blokini qo'shishi uchun ishlatadi.
        """
        if current_price <= 0:
            return None

        raw = self.get_walls(symbol)
        buy_walls, sell_walls = raw["buy_walls"], raw["sell_walls"]
        if not buy_walls and not sell_walls:
            return None

        def dist(w):
            return (w["price"] - current_price) / current_price * 100

        for w in buy_walls:
            w["dist_pct"] = dist(w)
        for w in sell_walls:
            w["dist_pct"] = dist(w)

        total_buy = sum(w["usdt"] for w in buy_walls)
        total_sell = sum(w["usdt"] for w in sell_walls)
        if total_buy <= 0 or total_sell <= 0:
            ratio = 1.0
        else:
            ratio = total_buy / total_sell

        return {
            "buy_walls": buy_walls,
            "sell_walls": sell_walls,
            "imbalance_ratio": round(ratio, 2),
        }

    async def stop(self):
        self._running = False