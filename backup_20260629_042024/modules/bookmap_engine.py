"""
ALPHATRADERAI — Bookmap Engine
Bookmap uslubidagi order book tahlili.

QILADIGANLARI:
1. Order Book Heatmap — har 2s da har bir price level ni saqlash
2. Volume Profile — har bir narx darajasida qancha hajm
3. Absorption — katta order sotish bosimini yutib olayaptimi
4. Iceberg — katta order kichik qismlarga bo'linganmi
5. Sweep — devor buzildi (support/resistance sindi)
6. Liquidity Heatmap — qayerda katta likvidatsiya to'plangan
7. Support/Resistance — avtomatik aniqlash
"""
import asyncio
import time
import aiosqlite
from datetime import datetime
from collections import defaultdict
from loguru import logger
import aiohttp


DB_PATH = "data/bookmap.db"


# ═══════════════════════════════════════════════════════
# DATABASE SCHEMA
# ═══════════════════════════════════════════════════════

CREATE_TABLES = """
-- Order Book Snapshots (har 2s da)
CREATE TABLE IF NOT EXISTS ob_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol TEXT NOT NULL,
    timestamp REAL NOT NULL,
    current_price REAL NOT NULL,

    -- Top-5 buy/sell levels
    bid1_price REAL, bid1_usdt REAL,
    bid2_price REAL, bid2_usdt REAL,
    bid3_price REAL, bid3_usdt REAL,
    bid4_price REAL, bid4_usdt REAL,
    bid5_price REAL, bid5_usdt REAL,

    ask1_price REAL, ask1_usdt REAL,
    ask2_price REAL, ask2_usdt REAL,
    ask3_price REAL, ask3_usdt REAL,
    ask4_price REAL, ask4_usdt REAL,
    ask5_price REAL, ask5_usdt REAL,

    -- Aggregate
    total_bid_usdt REAL DEFAULT 0,
    total_ask_usdt REAL DEFAULT 0,
    imbalance_ratio REAL DEFAULT 1.0,
    spread_pct REAL DEFAULT 0,

    -- Imbalance levels (qaysi narxda katta devor bor)
    wall_bid_price REAL DEFAULT 0,
    wall_bid_usdt REAL DEFAULT 0,
    wall_ask_price REAL DEFAULT 0,
    wall_ask_usdt REAL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_ob_symbol_ts ON ob_snapshots(symbol, timestamp);
CREATE INDEX IF NOT EXISTS idx_ob_timestamp ON ob_snapshots(timestamp);

-- Volume Profile (har 1 daqiqada yangilanadi)
CREATE TABLE IF NOT EXISTS volume_profile (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol TEXT NOT NULL,
    timestamp REAL NOT NULL,
    price_level REAL NOT NULL,
    volume_usdt REAL DEFAULT 0,
    buy_volume REAL DEFAULT 0,
    sell_volume REAL DEFAULT 0,
    trade_count INTEGER DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_vp_symbol_ts ON volume_profile(symbol, timestamp);
CREATE INDEX IF NOT EXISTS idx_vp_symbol_price ON volume_profile(symbol, price_level);

-- Absorption Events
CREATE TABLE IF NOT EXISTS absorption_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol TEXT NOT NULL,
    timestamp REAL NOT NULL,
    side TEXT NOT NULL,  -- 'buy' yoki 'sell'
    price REAL NOT NULL,
    absorbed_usdt REAL NOT NULL,
    duration_seconds INTEGER DEFAULT 0,
    strength REAL DEFAULT 0  -- qancha kuchli (0-100)
);

-- Iceberg Events
CREATE TABLE IF NOT EXISTS iceberg_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol TEXT NOT NULL,
    timestamp REAL NOT NULL,
    side TEXT NOT NULL,
    price REAL NOT NULL,
    total_usdt REAL NOT NULL,
    chunks_count INTEGER DEFAULT 0,
    avg_chunk_usdt REAL DEFAULT 0
);

-- Sweep Events (support/resistance sindi)
CREATE TABLE IF NOT EXISTS sweep_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol TEXT NOT NULL,
    timestamp REAL NOT NULL,
    side TEXT NOT NULL,  -- 'sweep_up' yoki 'sweep_down'
    level_price REAL NOT NULL,
    swept_usdt REAL NOT NULL,
    volume_usdt REAL DEFAULT 0
);

-- Support/Resistance Levels
CREATE TABLE IF NOT EXISTS sr_levels (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol TEXT NOT NULL,
    price REAL NOT NULL,
    level_type TEXT NOT NULL,  -- 'support' yoki 'resistance'
    strength REAL DEFAULT 0,   -- necha marta test qilingan
    last_test_ts REAL DEFAULT 0,
    volume_at_level REAL DEFAULT 0,
    UNIQUE(symbol, price, level_type)
);

CREATE INDEX IF NOT EXISTS idx_sr_symbol ON sr_levels(symbol);
"""


class BookmapEngine:
    """
    Bookmap uslubidagi order book tahlili.
    Har 2s da order book snapshot oladi va tahlil qiladi.
    """

    def __init__(self):
        self._running = False
        self._db_path = DB_PATH
        self._last_walls: dict[str, dict] = {}  # symbol -> {price: {side, usdt, first_seen}}
        self._absorption_track: dict[str, dict] = {}  # symbol -> {price, usdt, start_ts}
        self._iceberg_track: dict[str, dict] = {}  # symbol -> {price, chunks, total_usdt}
        self._sr_levels: dict[str, list] = defaultdict(list)  # symbol -> [{price, type, strength}]
        self._volume_profile: dict[str, dict] = defaultdict(dict)  # symbol -> {price_level: {vol, buy, sell}}

    async def start(self):
        self._running = True
        import os
        os.makedirs("data", exist_ok=True)
        await self._init_db()
        asyncio.create_task(self._snapshot_loop())
        asyncio.create_task(self._sr_detection_loop())
        asyncio.create_task(self._volume_profile_loop())
        logger.info("✅ Bookmap Engine started")

    async def _init_db(self):
        async with aiosqlite.connect(self._db_path) as db:
            await db.executescript(CREATE_TABLES)
            await db.commit()
        logger.info(f"✅ Bookmap DB ready: {self._db_path}")

    # ═══════════════════════════════════════════════════════
    # ORDER BOOK SNAPSHOT (har 2s)
    # ═══════════════════════════════════════════════════════

    async def _snapshot_loop(self):
        """Har 2 soniyada order book snapshot oladi"""
        while self._running:
            try:
                await self._take_snapshots()
            except Exception as e:
                logger.debug(f"Bookmap snapshot error: {e}")
            await asyncio.sleep(2)

    async def _take_snapshots(self):
        """Barcha tracked symbollar uchun OB snapshot oladi"""
        from core.state_manager import state_manager
        symbols = await state_manager.get_symbols("binance", "futures")

        session = aiohttp.ClientSession()
        try:
            for symbol in list(symbols)[:50]:  # Top 50 symbol
                try:
                    await self._fetch_ob_snapshot(session, symbol)
                except Exception as e:
                    logger.debug(f"OB snapshot error {symbol}: {e}")
                await asyncio.sleep(0.5)  # Rate limit
        finally:
            await session.close()

    async def _fetch_ob_snapshot(self, session: aiohttp.ClientSession, symbol: str):
        """Bitta symbol uchun OB snapshot olish va tahlil qilish"""
        url = "https://fapi.binance.com/fapi/v1/depth"
        params = {"symbol": symbol, "limit": 20}

        async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=5)) as resp:
            if resp.status != 200:
                return
            data = await resp.json()

        bids = data.get("bids", [])
        asks = data.get("asks", [])
        now = time.time()

        # Ticker narxini olish
        from core.state_manager import state_manager
        ticker = await state_manager.get_ticker("binance", symbol)
        current_price = ticker["price"] if ticker else 0
        if current_price <= 0:
            return

        # Top-5 bid/ask levels
        bid_levels = []
        ask_levels = []

        for i, (price_str, qty_str) in enumerate(bids[:5]):
            price = float(price_str)
            qty = float(qty_str)
            usdt = price * qty
            bid_levels.append({"price": price, "usdt": usdt, "qty": qty})

        for i, (price_str, qty_str) in enumerate(asks[:5]):
            price = float(price_str)
            qty = float(qty_str)
            usdt = price * qty
            ask_levels.append({"price": price, "usdt": usdt, "qty": qty})

        # Aggregate
        total_bid = sum(b["usdt"] for b in bid_levels)
        total_ask = sum(a["usdt"] for a in ask_levels)
        imbalance = total_bid / total_ask if total_ask > 0 else 1.0

        # Spread
        best_bid = bid_levels[0]["price"] if bid_levels else 0
        best_ask = ask_levels[0]["price"] if ask_levels else 0
        spread_pct = ((best_ask - best_bid) / best_bid * 100) if best_bid > 0 else 0

        # Eng katta devorlar
        wall_bid = max(bid_levels, key=lambda x: x["usdt"]) if bid_levels else {"price": 0, "usdt": 0}
        wall_ask = max(ask_levels, key=lambda x: x["usdt"]) if ask_levels else {"price": 0, "usdt": 0}

        # Database ga saqlash
        async with aiosqlite.connect(self._db_path) as db:
            await db.execute("""
                INSERT INTO ob_snapshots (
                    symbol, timestamp, current_price,
                    bid1_price, bid1_usdt, bid2_price, bid2_usdt,
                    bid3_price, bid3_usdt, bid4_price, bid4_usdt,
                    bid5_price, bid5_usdt,
                    ask1_price, ask1_usdt, ask2_price, ask2_usdt,
                    ask3_price, ask3_usdt, ask4_price, ask4_usdt,
                    ask5_price, ask5_usdt,
                    total_bid_usdt, total_ask_usdt, imbalance_ratio, spread_pct,
                    wall_bid_price, wall_bid_usdt, wall_ask_price, wall_ask_usdt
                ) VALUES (?, ?, ?,
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                    ?, ?, ?, ?, ?, ?, ?, ?, ?,
                    ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                symbol, now, current_price,
                bid_levels[0]["price"] if len(bid_levels) > 0 else 0,
                bid_levels[0]["usdt"] if len(bid_levels) > 0 else 0,
                bid_levels[1]["price"] if len(bid_levels) > 1 else 0,
                bid_levels[1]["usdt"] if len(bid_levels) > 1 else 0,
                bid_levels[2]["price"] if len(bid_levels) > 2 else 0,
                bid_levels[2]["usdt"] if len(bid_levels) > 2 else 0,
                bid_levels[3]["price"] if len(bid_levels) > 3 else 0,
                bid_levels[3]["usdt"] if len(bid_levels) > 3 else 0,
                bid_levels[4]["price"] if len(bid_levels) > 4 else 0,
                bid_levels[4]["usdt"] if len(bid_levels) > 4 else 0,
                ask_levels[0]["price"] if len(ask_levels) > 0 else 0,
                ask_levels[0]["usdt"] if len(ask_levels) > 0 else 0,
                ask_levels[1]["price"] if len(ask_levels) > 1 else 0,
                ask_levels[1]["usdt"] if len(ask_levels) > 1 else 0,
                ask_levels[2]["price"] if len(ask_levels) > 2 else 0,
                ask_levels[2]["usdt"] if len(ask_levels) > 2 else 0,
                ask_levels[3]["price"] if len(ask_levels) > 3 else 0,
                ask_levels[3]["usdt"] if len(ask_levels) > 3 else 0,
                ask_levels[4]["price"] if len(ask_levels) > 4 else 0,
                ask_levels[4]["usdt"] if len(ask_levels) > 4 else 0,
                total_bid, total_ask, imbalance, spread_pct,
                wall_bid["price"], wall_bid["usdt"], wall_ask["price"], wall_ask["usdt"]
            ))
            await db.commit()

        # Absorption tekshirish
        await self._check_absorption(symbol, bid_levels, ask_levels, current_price, now)

        # Iceberg tekshirish
        await self._check_iceberg(symbol, bid_levels, ask_levels, current_price, now)

        # Sweep tekshirish
        await self._check_sweep(symbol, bid_levels, ask_levels, current_price, now)

    # ═══════════════════════════════════════════════════════
    # ABSORPTION DETECTION
    # ═══════════════════════════════════════════════════════

    async def _check_absorption(self, symbol: str, bids: list, asks: list, price: float, now: float):
        """
        Absorption aniqlash:
        Agar katta buy devori turib, narx tushmasa → absorption (kuchli support)
        Agar katta sell devori turib, narx ko'tarilmasa → absorption (kuchli resistance)
        """
        # Buy absorption: katta bid devori + narx yaqinida
        for bid in bids:
            dist_pct = abs(bid["price"] - price) / price * 100
            if bid["usdt"] >= 100_000 and dist_pct < 1.0:
                key = f"{symbol}:bid:{bid['price']}"
                if key not in self._absorption_track:
                    self._absorption_track[key] = {
                        "price": bid["price"],
                        "side": "buy",
                        "start_usdt": bid["usdt"],
                        "start_ts": now,
                        "max_usdt": bid["usdt"],
                    }
                else:
                    track = self._absorption_track[key]
                    track["max_usdt"] = max(track["max_usdt"], bid["usdt"])
                    duration = now - track["start_ts"]

                    # 30s dan beri turibdi va kamaymagan → absorption
                    if duration >= 30 and bid["usdt"] >= track["start_usdt"] * 0.8:
                        strength = min(100, int(duration / 60 * 10 + bid["usdt"] / 100_000 * 5))
                        await self._log_absorption(symbol, "buy", bid["price"], bid["usdt"], int(duration), strength)
                        del self._absorption_track[key]

        # Sell absorption: katta ask devori + narx yaqinida
        for ask in asks:
            dist_pct = abs(ask["price"] - price) / price * 100
            if ask["usdt"] >= 100_000 and dist_pct < 1.0:
                key = f"{symbol}:ask:{ask['price']}"
                if key not in self._absorption_track:
                    self._absorption_track[key] = {
                        "price": ask["price"],
                        "side": "sell",
                        "start_usdt": ask["usdt"],
                        "start_ts": now,
                        "max_usdt": ask["usdt"],
                    }
                else:
                    track = self._absorption_track[key]
                    track["max_usdt"] = max(track["max_usdt"], ask["usdt"])
                    duration = now - track["start_ts"]

                    if duration >= 30 and ask["usdt"] >= track["start_usdt"] * 0.8:
                        strength = min(100, int(duration / 60 * 10 + ask["usdt"] / 100_000 * 5))
                        await self._log_absorption(symbol, "sell", ask["price"], ask["usdt"], int(duration), strength)
                        del self._absorption_track[key]

    async def _log_absorption(self, symbol: str, side: str, price: float, usdt: float, duration: int, strength: int):
        """Absorption event ni log qilish"""
        async with aiosqlite.connect(self._db_path) as db:
            await db.execute("""
                INSERT INTO absorption_events (symbol, timestamp, side, price, absorbed_usdt, duration_seconds, strength)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (symbol, time.time(), side, price, usdt, duration, strength))
            await db.commit()

        emoji = "🟢" if side == "buy" else "🔴"
        logger.info(f"🧱 ABSORPTION: {symbol} {emoji} {side.upper()} ${usdt/1e6:.1f}M @ {price:.2f} | {duration}s | strength={strength}")

    # ═══════════════════════════════════════════════════════
    # ICEBERG DETECTION
    # ═══════════════════════════════════════════════════════

    async def _check_iceberg(self, symbol: str, bids: list, asks: list, price: float, now: float):
        """
        Iceberg aniqlash:
        Agar bir xil narxda doimiy katta order paydo bo'lsa → iceberg
        (katta order kichik qismlarga bo'lingan)
        """
        for bid in bids:
            key = f"{symbol}:bid:{bid['price']}"
            if bid["usdt"] >= 50_000:
                if key not in self._iceberg_track:
                    self._iceberg_track[key] = {
                        "price": bid["price"],
                        "side": "buy",
                        "chunks": 1,
                        "total_usdt": bid["usdt"],
                        "last_seen": now,
                        "first_seen": now,
                    }
                else:
                    track = self._iceberg_track[key]
                    time_diff = now - track["last_seen"]
                    if time_diff < 10:  # 10s ichida qayta paydo bo'ldi
                        track["chunks"] += 1
                        track["total_usdt"] += bid["usdt"]
                        track["last_seen"] = now

                        # 3+ chunk va umumiy $200K+ → iceberg
                        if track["chunks"] >= 3 and track["total_usdt"] >= 200_000:
                            await self._log_iceberg(
                                symbol, "buy", bid["price"],
                                track["total_usdt"], track["chunks"],
                                track["total_usdt"] / track["chunks"]
                            )
                            del self._iceberg_track[key]
                    else:
                        # Vaqt o'tdi, qayta boshlash
                        self._iceberg_track[key] = {
                            "price": bid["price"],
                            "side": "buy",
                            "chunks": 1,
                            "total_usdt": bid["usdt"],
                            "last_seen": now,
                            "first_seen": now,
                        }

        for ask in asks:
            key = f"{symbol}:ask:{ask['price']}"
            if ask["usdt"] >= 50_000:
                if key not in self._iceberg_track:
                    self._iceberg_track[key] = {
                        "price": ask["price"],
                        "side": "sell",
                        "chunks": 1,
                        "total_usdt": ask["usdt"],
                        "last_seen": now,
                        "first_seen": now,
                    }
                else:
                    track = self._iceberg_track[key]
                    time_diff = now - track["last_seen"]
                    if time_diff < 10:
                        track["chunks"] += 1
                        track["total_usdt"] += ask["usdt"]
                        track["last_seen"] = now

                        if track["chunks"] >= 3 and track["total_usdt"] >= 200_000:
                            await self._log_iceberg(
                                symbol, "sell", ask["price"],
                                track["total_usdt"], track["chunks"],
                                track["total_usdt"] / track["chunks"]
                            )
                            del self._iceberg_track[key]
                    else:
                        self._iceberg_track[key] = {
                            "price": ask["price"],
                            "side": "sell",
                            "chunks": 1,
                            "total_usdt": ask["usdt"],
                            "last_seen": now,
                            "first_seen": now,
                        }

    async def _log_iceberg(self, symbol: str, side: str, price: float, total_usdt: float, chunks: int, avg_chunk: float):
        """Iceberg event ni log qilish"""
        async with aiosqlite.connect(self._db_path) as db:
            await db.execute("""
                INSERT INTO iceberg_events (symbol, timestamp, side, price, total_usdt, chunks_count, avg_chunk_usdt)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (symbol, time.time(), side, price, total_usdt, chunks, avg_chunk))
            await db.commit()

        emoji = "🟢" if side == "buy" else "🔴"
        logger.info(f"🧊 ICEBERG: {symbol} {emoji} {side.upper()} ${total_usdt/1e6:.1f}M @ {price:.2f} | {chunks} chunks")

    # ═══════════════════════════════════════════════════════
    # SWEEP DETECTION (Support/Resistance sindi)
    # ═══════════════════════════════════════════════════════

    async def _check_sweep(self, symbol: str, bids: list, asks: list, price: float, now: float):
        """
        Sweep aniqlash:
        Agar katta devor buzilib, narx o'tib ketsa → sweep
        (support/resistance sindi)
        """
        prev_walls = self._last_walls.get(symbol, {})
        current_walls = {}

        # Hozirgi devorlarni saqlash
        for bid in bids:
            if bid["usdt"] >= 100_000:
                current_walls[bid["price"]] = {"side": "bid", "usdt": bid["usdt"]}

        for ask in asks:
            if ask["usdt"] >= 100_000:
                current_walls[ask["price"]] = {"side": "ask", "usdt": ask["usdt"]}

        # Avvalgi devorlar bilan solishtirish
        for prev_price, prev_data in prev_walls.items():
            if prev_price not in current_walls:
                # Devor yo'qoldi
                if prev_data["side"] == "ask" and price > prev_price:
                    # Sell devori buzildi → sweep up
                    await self._log_sweep(symbol, "sweep_up", prev_price, prev_data["usdt"], price)
                elif prev_data["side"] == "bid" and price < prev_price:
                    # Buy devori buzildi → sweep down
                    await self._log_sweep(symbol, "sweep_down", prev_price, prev_data["usdt"], price)

        self._last_walls[symbol] = current_walls

    async def _log_sweep(self, symbol: str, side: str, level_price: float, swept_usdt: float, current_price: float):
        """Sweep event ni log qilish"""
        async with aiosqlite.connect(self._db_path) as db:
            await db.execute("""
                INSERT INTO sweep_events (symbol, timestamp, side, level_price, swept_usdt, volume_usdt)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (symbol, time.time(), side, level_price, swept_usdt, swept_usdt))
            await db.commit()

        emoji = "📈" if side == "sweep_up" else "📉"
        logger.info(f"💨 SWEEP: {symbol} {emoji} {side} | ${swept_usdt/1e6:.1f}M @ {level_price:.2f} sindi → {current_price:.2f}")

    # ═══════════════════════════════════════════════════════
    # SUPPORT/RESISTANCE DETECTION
    # ═══════════════════════════════════════════════════════

    async def _sr_detection_loop(self):
        """Har 5 daqiqada support/resistance level'larni yangilaydi"""
        while self._running:
            try:
                await self._update_sr_levels()
            except Exception as e:
                logger.debug(f"SR detection error: {e}")
            await asyncio.sleep(300)

    async def _update_sr_levels(self):
        """Order book tarixidan support/resistance level'larni aniqlash"""
        async with aiosqlite.connect(self._db_path) as db:
            # Oxirgi 1 soatlik OB snapshotlar
            cutoff = time.time() - 3600
            cursor = await db.execute("""
                SELECT symbol, wall_bid_price, wall_bid_usdt, wall_ask_price, wall_ask_usdt
                FROM ob_snapshots WHERE timestamp > ? AND symbol IS NOT NULL
            """, (cutoff,))
            rows = await cursor.fetchall()

        # Har bir symbol uchun devorlar darajasini hisoblash
        wall_counts: dict[str, dict] = defaultdict(lambda: defaultdict(float))

        for symbol, bid_price, bid_usdt, ask_price, ask_usdt in rows:
            if bid_price > 0 and bid_usdt >= 50_000:
                # Narxni 0.5% ga round qilish
                level = round(bid_price * 20) / 20
                wall_counts[symbol][level] += bid_usdt

            if ask_price > 0 and ask_usdt >= 50_000:
                level = round(ask_price * 20) / 20
                wall_counts[symbol][level] += ask_usdt

        # Kuchli level'larni aniqlash (3+ marta devor bo'lgan)
        async with aiosqlite.connect(self._db_path) as db:
            for symbol, levels in wall_counts.items():
                for level, total_usdt in levels.items():
                    count = int(total_usdt / 100_000)  # Taxminiy devor soni
                    if count >= 3:
                        # Support yoki Resistance?
                        cursor = await db.execute("""
                            SELECT current_price FROM ob_snapshots
                            WHERE symbol = ? ORDER BY timestamp DESC LIMIT 1
                        """, (symbol,))
                        row = await cursor.fetchone()
                        if row:
                            current = row[0]
                            if level < current:
                                level_type = "support"
                            else:
                                level_type = "resistance"

                            await db.execute("""
                                INSERT OR REPLACE INTO sr_levels
                                (symbol, price, level_type, strength, last_test_ts, volume_at_level)
                                VALUES (?, ?, ?, ?, ?, ?)
                            """, (symbol, level, level_type, count, time.time(), total_usdt))

            await db.commit()

    # ═══════════════════════════════════════════════════════
    # VOLUME PROFILE
    # ═══════════════════════════════════════════════════════

    async def _volume_profile_loop(self):
        """Har 1 daqiqada volume profile ni yangilaydi"""
        while self._running:
            try:
                await self._update_volume_profile()
            except Exception as e:
                logger.debug(f"Volume profile error: {e}")
            await asyncio.sleep(60)

    async def _update_volume_profile(self):
        """Har bir symbol uchun volume profile hisoblaydi"""
        async with aiosqlite.connect(self._db_path) as db:
            # Oxirgi 5 daqiqadagi savdolar (trade data kerak)
            # Hozircha faqat order book volume profile
            cutoff = time.time() - 300
            cursor = await db.execute("""
                SELECT symbol, wall_bid_price, wall_bid_usdt, wall_ask_price, wall_ask_usdt
                FROM ob_snapshots WHERE timestamp > ?
            """, (cutoff,))
            rows = await cursor.fetchall()

        profile_data: dict[str, dict] = defaultdict(lambda: defaultdict(lambda: {"vol": 0, "buy": 0, "sell": 0}))

        for symbol, bid_price, bid_usdt, ask_price, ask_usdt in rows:
            if bid_price > 0:
                level = round(bid_price * 20) / 20
                profile_data[symbol][level]["vol"] += bid_usdt
                profile_data[symbol][level]["buy"] += bid_usdt

            if ask_price > 0:
                level = round(ask_price * 20) / 20
                profile_data[symbol][level]["vol"] += ask_usdt
                profile_data[symbol][level]["sell"] += ask_usdt

        async with aiosqlite.connect(self._db_path) as db:
            for symbol, levels in profile_data.items():
                for level, data in levels.items():
                    await db.execute("""
                        INSERT INTO volume_profile (symbol, timestamp, price_level, volume_usdt, buy_volume, sell_volume)
                        VALUES (?, ?, ?, ?, ?, ?)
                    """, (symbol, time.time(), level, data["vol"], data["buy"], data["sell"]))

            await db.commit()

    # ═══════════════════════════════════════════════════════
    # PUBLIC METHODS (boshqa modullar uchun)
    # ═══════════════════════════════════════════════════════

    async def get_heatmap(self, symbol: str, minutes: int = 60) -> list[dict]:
        """Bookmap heatmap — oxirgi N daqiqadagi order book"""
        cutoff = time.time() - minutes * 60
        async with aiosqlite.connect(self._db_path) as db:
            cursor = await db.execute("""
                SELECT timestamp, current_price,
                       bid1_price, bid1_usdt, bid2_price, bid2_usdt,
                       bid3_price, bid3_usdt, bid4_price, bid4_usdt,
                       bid5_price, bid5_usdt,
                       ask1_price, ask1_usdt, ask2_price, ask2_usdt,
                       ask3_price, ask3_usdt, ask4_price, ask4_usdt,
                       ask5_price, ask5_usdt,
                       total_bid_usdt, total_ask_usdt, imbalance_ratio
                FROM ob_snapshots
                WHERE symbol = ? AND timestamp > ?
                ORDER BY timestamp ASC
            """, (symbol, cutoff))
            rows = await cursor.fetchall()

        result = []
        for row in rows:
            result.append({
                "timestamp": row[0],
                "price": row[1],
                "bids": [
                    {"price": row[2], "usdt": row[3]},
                    {"price": row[4], "usdt": row[5]},
                    {"price": row[6], "usdt": row[7]},
                    {"price": row[8], "usdt": row[9]},
                    {"price": row[10], "usdt": row[11]},
                ],
                "asks": [
                    {"price": row[12], "usdt": row[13]},
                    {"price": row[14], "usdt": row[15]},
                    {"price": row[16], "usdt": row[17]},
                    {"price": row[18], "usdt": row[19]},
                    {"price": row[20], "usdt": row[21]},
                ],
                "total_bid": row[22],
                "total_ask": row[23],
                "imbalance": row[24],
            })

        return result

    async def get_sr_levels(self, symbol: str) -> dict:
        """Support/Resistance level'larni qaytaradi"""
        async with aiosqlite.connect(self._db_path) as db:
            cursor = await db.execute("""
                SELECT price, level_type, strength, volume_at_level
                FROM sr_levels WHERE symbol = ? ORDER BY strength DESC LIMIT 20
            """, (symbol,))
            rows = await cursor.fetchall()

        supports = []
        resistances = []

        for price, level_type, strength, volume in rows:
            entry = {"price": price, "strength": strength, "volume": volume}
            if level_type == "support":
                supports.append(entry)
            else:
                resistances.append(entry)

        return {"supports": supports, "resistances": resistances}

    async def get_absorption_events(self, symbol: str, hours: int = 24) -> list[dict]:
        """So'nggi N soatdagi absorption eventlarni qaytaradi"""
        cutoff = time.time() - hours * 3600
        async with aiosqlite.connect(self._db_path) as db:
            cursor = await db.execute("""
                SELECT timestamp, side, price, absorbed_usdt, duration_seconds, strength
                FROM absorption_events
                WHERE symbol = ? AND timestamp > ?
                ORDER BY timestamp DESC LIMIT 50
            """, (symbol, cutoff))
            rows = await cursor.fetchall()

        return [
            {"timestamp": r[0], "side": r[1], "price": r[2], "usdt": r[3], "duration": r[4], "strength": r[5]}
            for r in rows
        ]

    async def get_iceberg_events(self, symbol: str, hours: int = 24) -> list[dict]:
        """So'nggi N soatdagi iceberg eventlarni qaytaradi"""
        cutoff = time.time() - hours * 3600
        async with aiosqlite.connect(self._db_path) as db:
            cursor = await db.execute("""
                SELECT timestamp, side, price, total_usdt, chunks_count, avg_chunk_usdt
                FROM iceberg_events
                WHERE symbol = ? AND timestamp > ?
                ORDER BY timestamp DESC LIMIT 50
            """, (symbol, cutoff))
            rows = await cursor.fetchall()

        return [
            {"timestamp": r[0], "side": r[1], "price": r[2], "usdt": r[3], "chunks": r[4], "avg_chunk": r[5]}
            for r in rows
        ]

    async def get_sweep_events(self, symbol: str, hours: int = 24) -> list[dict]:
        """So'nggi N soatdagi sweep eventlarni qaytaradi"""
        cutoff = time.time() - hours * 3600
        async with aiosqlite.connect(self._db_path) as db:
            cursor = await db.execute("""
                SELECT timestamp, side, level_price, swept_usdt
                FROM sweep_events
                WHERE symbol = ? AND timestamp > ?
                ORDER BY timestamp DESC LIMIT 50
            """, (symbol, cutoff))
            rows = await cursor.fetchall()

        return [
            {"timestamp": r[0], "side": r[1], "level": r[2], "usdt": r[3]}
            for r in rows
        ]

    async def get_stats(self, symbol: str) -> dict:
        """Bookmap statistikasi"""
        async with aiosqlite.connect(self._db_path) as db:
            cursor = await db.execute("""
                SELECT COUNT(*) FROM ob_snapshots WHERE symbol = ?
            """, (symbol,))
            snapshots = (await cursor.fetchone())[0]

            cursor = await db.execute("""
                SELECT COUNT(*) FROM absorption_events WHERE symbol = ?
            """, (symbol,))
            absorptions = (await cursor.fetchone())[0]

            cursor = await db.execute("""
                SELECT COUNT(*) FROM iceberg_events WHERE symbol = ?
            ""), (symbol,)
            icebergs = (await cursor.fetchone())[0]

            cursor = await db.execute("""
                SELECT COUNT(*) FROM sweep_events WHERE symbol = ?
            """, (symbol,))
            sweeps = (await cursor.fetchone())[0]

            cursor = await db.execute("""
                SELECT COUNT(*) FROM sr_levels WHERE symbol = ?
            """, (symbol,))
            sr_levels = (await cursor.fetchone())[0]

        return {
            "snapshots": snapshots,
            "absorptions": absorptions,
            "icebergs": icebergs,
            "sweeps": sweeps,
            "sr_levels": sr_levels,
        }

    async def stop(self):
        self._running = False


# Global instance
bookmap_engine = BookmapEngine()
