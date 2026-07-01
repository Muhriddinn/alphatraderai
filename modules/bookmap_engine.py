"""
ALPHATRADERAI — Bookmap Engine (WebSocket)
Bookmap uslubidagi order book tahlili.

WebSocket orqali — REST emas! Ban yo'q, tez, real-time.

QILADIGANLARI:
1. Order Book Heatmap — har 100ms da yangilanadi
2. Absorption — katta order sotish bosimini yutib olayaptimi
3. Iceberg — katta order kichik qismlarga bo'linganmi
4. Sweep — devor buzildi (support/resistance sindi)
5. Support/Resistance — avtomatik aniqlash
"""
import asyncio
import time
import json
import aiohttp
import aiosqlite
from datetime import datetime
from collections import defaultdict
from loguru import logger


DB_PATH = "data/bookmap.db"

# Binance Futures WebSocket depth
WS_BASE = "wss://fstream.binance.com/stream?streams="


CREATE_TABLES = """
CREATE TABLE IF NOT EXISTS ob_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol TEXT NOT NULL,
    timestamp REAL NOT NULL,
    current_price REAL NOT NULL,
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
    total_bid_usdt REAL DEFAULT 0,
    total_ask_usdt REAL DEFAULT 0,
    imbalance_ratio REAL DEFAULT 1.0,
    spread_pct REAL DEFAULT 0,
    wall_bid_price REAL DEFAULT 0,
    wall_bid_usdt REAL DEFAULT 0,
    wall_ask_price REAL DEFAULT 0,
    wall_ask_usdt REAL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_ob_symbol_ts ON ob_snapshots(symbol, timestamp);

CREATE TABLE IF NOT EXISTS absorption_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol TEXT NOT NULL, timestamp REAL NOT NULL,
    side TEXT NOT NULL, price REAL NOT NULL,
    absorbed_usdt REAL NOT NULL, duration_seconds INTEGER DEFAULT 0,
    strength REAL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS iceberg_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol TEXT NOT NULL, timestamp REAL NOT NULL,
    side TEXT NOT NULL, price REAL NOT NULL,
    total_usdt REAL NOT NULL, chunks_count INTEGER DEFAULT 0,
    avg_chunk_usdt REAL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS sweep_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol TEXT NOT NULL, timestamp REAL NOT NULL,
    side TEXT NOT NULL, level_price REAL NOT NULL,
    swept_usdt REAL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS sr_levels (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol TEXT NOT NULL, price REAL NOT NULL,
    level_type TEXT NOT NULL, strength REAL DEFAULT 0,
    first_seen_ts REAL DEFAULT 0,
    last_test_ts REAL DEFAULT 0,
    UNIQUE(symbol, price, level_type)
);
CREATE INDEX IF NOT EXISTS idx_sr_symbol ON sr_levels(symbol);
"""


class BookmapEngine:
    """
    Bookmap uslubidagi order book tahlili.
    WebSocket orqali — REST EMAS! Ban yo'q.
    """

    def __init__(self):
        self._running = False
        self._db_path = DB_PATH
        self._ws_tasks: list[asyncio.Task] = []
        self._current_ob: dict[str, dict] = {}  # symbol -> {bids, asks, price}
        self._prev_walls: dict[str, dict] = {}  # sweep uchun
        self._absorption_track: dict[str, dict] = {}
        self._iceberg_track: dict[str, dict] = {}
        self._snapshot_counter = 0
        # Rate limiting: symbol:event_type -> last_alert_ts
        self._alert_cooldown: dict[str, float] = {}
        # Dynamic threshold: arzon coinlarda yuqori threshold
        self._price_thresholds: dict[str, float] = {}

    def _get_threshold(self, symbol: str, price: float, base_threshold: float) -> float:
        """Dinamik threshold — arzon coinlarda threshold oshiriladi"""
        if price < 0.1:
            return base_threshold * 10  # ADA $0.14 → $500K = $5M
        elif price < 1:
            return base_threshold * 5   # DOGE $0.5 → $500K = $2.5M
        elif price < 10:
            return base_threshold * 2   # XRP $5 → $500K = $1M
        return base_threshold

    def _check_cooldown(self, symbol: str, event_type: str, cooldown: float = 60) -> bool:
        """Rate limiting — har symbol uchun 60s da 1 marta"""
        key = f"{symbol}:{event_type}"
        now = time.time()
        last = self._alert_cooldown.get(key, 0)
        if now - last < cooldown:
            return False
        self._alert_cooldown[key] = now
        return True

    async def start(self):
        self._running = True
        import os
        os.makedirs("data", exist_ok=True)
        await self._init_db()
        asyncio.create_task(self._snapshot_loop())
        asyncio.create_task(self._sr_detection_loop())
        logger.info("✅ Bookmap Engine started (WebSocket)")

    async def _init_db(self):
        async with aiosqlite.connect(self._db_path) as db:
            await db.executescript(CREATE_TABLES)
            await db.commit()

    # ═══════════════════════════════════════════════════════
    # WEBSOCKET DEPTH STREAM
    # ═══════════════════════════════════════════════════════

    def start_ws_for_symbols(self, symbols: list[str]):
        """Top symbollar uchun WebSocket ochish"""
        if not symbols:
            return

        # Har 10 ta symbol uchun 1 ta WebSocket (Binance limit 200 stream/conn)
        chunks = [symbols[i:i+10] for i in range(0, len(symbols), 10)]

        for chunk in chunks:
            streams = "/".join([f"{s.lower()}@depth5@100ms" for s in chunk])
            url = f"{WS_BASE}{streams}"
            task = asyncio.create_task(self._ws_loop(url, chunk))
            self._ws_tasks.append(task)

        logger.info(f"✅ Bookmap WebSocket: {len(chunks)} connections, {len(symbols)} symbols")

    async def _ws_loop(self, url: str, symbols: list[str]):
        """WebSocket ulanish — avtomatik qayta ulanish"""
        async with aiohttp.ClientSession() as session:
            while self._running:
                try:
                    async with session.ws_connect(
                        url,
                        heartbeat=20,
                        timeout=30,
                        max_msg_size=10 * 1024 * 1024
                    ) as ws:
                        logger.debug(f"Bookmap WS connected: {len(symbols)} symbols")
                        async for msg in ws:
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                if not self._running:
                                    break
                                try:
                                    data = json.loads(msg.data)
                                    await self._on_depth_message(data)
                                except Exception as e:
                                    logger.debug(f"Bookmap WS parse error: {e}")
                            elif msg.type in (aiohttp.WSMsgType.ERROR, aiohttp.WSMsgType.CLOSED):
                                break
                except Exception as e:
                    if self._running:
                        logger.debug(f"Bookmap WS reconnecting: {e}")
                        await asyncio.sleep(5)

    async def _on_depth_message(self, data: dict):
        """WebSocket dan depth message kelganda"""
        stream = data.get("stream", "")
        if "@depth5" not in stream:
            return

        # symbol olish: "btcusdt@depth5@100ms" -> "BTCUSDT"
        symbol = stream.split("@")[0].upper()
        payload = data.get("data", {})

        bids = payload.get("b", [])  # [[price, qty], ...]
        asks = payload.get("a", [])

        now = time.time()

        # Current price (best bid + ask o'rtasi)
        best_bid = float(bids[0][0]) if bids else 0
        best_ask = float(asks[0][0]) if asks else 0
        price = (best_bid + best_ask) / 2 if best_bid > 0 and best_ask > 0 else best_bid or best_ask

        if price <= 0:
            return

        # OB data ni saqlash
        self._current_ob[symbol] = {
            "bids": [(float(p), float(q), float(p) * float(q)) for p, q in bids],
            "asks": [(float(p), float(q), float(p) * float(q)) for p, q in asks],
            "price": price,
            "timestamp": now,
        }

        # Har 2 soniyada (20 ta message) snapshot saqlash
        self._snapshot_counter += 1
        if self._snapshot_counter % 20 == 0:
            await self._save_snapshot(symbol, price, now)

        # Real-time tahlil
        await self._analyze(symbol, price, now)

    # ═══════════════════════════════════════════════════════
    # SNAPSHOT SAQLASH (har 2s)
    # ═══════════════════════════════════════════════════════

    async def _snapshot_loop(self):
        """Har 5 soniyada barcha active OB larni tahlil qiladi"""
        while self._running:
            try:
                for symbol, ob in list(self._current_ob.items()):
                    await self._save_snapshot(symbol, ob["price"], ob["timestamp"])
            except Exception as e:
                logger.debug(f"Snapshot loop error: {e}")
            await asyncio.sleep(5)

    async def _save_snapshot(self, symbol: str, price: float, now: float):
        """OB snapshot ni SQLite ga saqlash"""
        ob = self._current_ob.get(symbol)
        if not ob:
            return

        bids = ob["bids"][:5]
        asks = ob["asks"][:5]

        total_bid = sum(b[2] for b in bids)
        total_ask = sum(a[2] for a in asks)
        imbalance = total_bid / total_ask if total_ask > 0 else 1.0
        spread = ((asks[0][0] - bids[0][0]) / bids[0][0] * 100) if bids and asks and bids[0][0] > 0 else 0

        wall_bid = max(bids, key=lambda x: x[2]) if bids else (0, 0, 0)
        wall_ask = max(asks, key=lambda x: x[2]) if asks else (0, 0, 0)

        try:
            async with aiosqlite.connect(self._db_path) as db:
                await db.execute("""
                    INSERT OR IGNORE INTO ob_snapshots (
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
                        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                        ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    symbol, now, price,
                    bids[0][0] if len(bids) > 0 else 0, bids[0][2] if len(bids) > 0 else 0,
                    bids[1][0] if len(bids) > 1 else 0, bids[1][2] if len(bids) > 1 else 0,
                    bids[2][0] if len(bids) > 2 else 0, bids[2][2] if len(bids) > 2 else 0,
                    bids[3][0] if len(bids) > 3 else 0, bids[3][2] if len(bids) > 3 else 0,
                    bids[4][0] if len(bids) > 4 else 0, bids[4][2] if len(bids) > 4 else 0,
                    asks[0][0] if len(asks) > 0 else 0, asks[0][2] if len(asks) > 0 else 0,
                    asks[1][0] if len(asks) > 1 else 0, asks[1][2] if len(asks) > 1 else 0,
                    asks[2][0] if len(asks) > 2 else 0, asks[2][2] if len(asks) > 2 else 0,
                    asks[3][0] if len(asks) > 3 else 0, asks[3][2] if len(asks) > 3 else 0,
                    asks[4][0] if len(asks) > 4 else 0, asks[4][2] if len(asks) > 4 else 0,
                    total_bid, total_ask, imbalance, spread,
                    wall_bid[0], wall_bid[2], wall_ask[0], wall_ask[2]
                ))
                await db.commit()
        except Exception as e:
            logger.debug(f"Snapshot save error {symbol}: {e}")

    # ═══════════════════════════════════════════════════════
    # REAL-TIME TAHLIL
    # ═══════════════════════════════════════════════════════

    async def _analyze(self, symbol: str, price: float, now: float):
        """Har bir depth update da tahlil qilish"""
        ob = self._current_ob.get(symbol)
        if not ob:
            return

        bids = ob["bids"]
        asks = ob["asks"]

        # Absorption tekshirish
        await self._check_absorption(symbol, bids, asks, price, now)

        # Iceberg tekshirish
        await self._check_iceberg(symbol, bids, asks, price, now)

        # Sweep tekshirish
        await self._check_sweep(symbol, bids, asks, price, now)

    # ═══════════════════════════════════════════════════════
    # ABSORPTION DETECTION
    # ═══════════════════════════════════════════════════════

    async def _check_absorption(self, symbol: str, bids, asks, price, now):
        """Absorption: katta devor turib, narx o'zgarmayapti"""
        threshold = self._get_threshold(symbol, price, 500_000)
        for bid_price, bid_qty, bid_usdt in bids[:3]:
            dist = abs(bid_price - price) / price * 100
            if bid_usdt >= threshold and dist < 0.5:
                key = f"{symbol}:bid:{bid_price}"
                if key not in self._absorption_track:
                    self._absorption_track[key] = {
                        "price": bid_price, "side": "buy",
                        "start_usdt": bid_usdt, "start_ts": now, "max_usdt": bid_usdt,
                    }
                else:
                    t = self._absorption_track[key]
                    t["max_usdt"] = max(t["max_usdt"], bid_usdt)
                    dur = now - t["start_ts"]
                    if dur >= 30 and bid_usdt >= t["start_usdt"] * 0.7:
                        strength = min(100, int(dur / 60 * 10 + bid_usdt / 100_000 * 5))
                        await self._log_absorption(symbol, "buy", bid_price, bid_usdt, int(dur), strength)
                        del self._absorption_track[key]

        threshold = self._get_threshold(symbol, price, 500_000)
        for ask_price, ask_qty, ask_usdt in asks[:3]:
            dist = abs(ask_price - price) / price * 100
            if ask_usdt >= threshold and dist < 0.5:
                key = f"{symbol}:ask:{ask_price}"
                if key not in self._absorption_track:
                    self._absorption_track[key] = {
                        "price": ask_price, "side": "sell",
                        "start_usdt": ask_usdt, "start_ts": now, "max_usdt": ask_usdt,
                    }
                else:
                    t = self._absorption_track[key]
                    t["max_usdt"] = max(t["max_usdt"], ask_usdt)
                    dur = now - t["start_ts"]
                    if dur >= 30 and ask_usdt >= t["start_usdt"] * 0.7:
                        strength = min(100, int(dur / 60 * 10 + ask_usdt / 100_000 * 5))
                        await self._log_absorption(symbol, "sell", ask_price, ask_usdt, int(dur), strength)
                        del self._absorption_track[key]

    async def _log_absorption(self, symbol, side, price, usdt, duration, strength):
        # Rate limiting — 60s cooldown
        if not self._check_cooldown(symbol, "absorption", 60):
            return
        # Faqat $1M+ absorption log qilish
        if usdt < 1_000_000:
            return
        try:
            async with aiosqlite.connect(self._db_path) as db:
                await db.execute("""
                    INSERT INTO absorption_events (symbol, timestamp, side, price, absorbed_usdt, duration_seconds, strength)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (symbol, time.time(), side, price, usdt, duration, strength))
                await db.commit()
            emoji = "🟢" if side == "buy" else "🔴"
            logger.info(f"🧱 ABSORPTION: {symbol} {emoji} {side.upper()} ${usdt/1e6:.1f}M @ {price:.2f} | {duration}s")
        except Exception:
            pass

    # ═══════════════════════════════════════════════════════
    # ICEBERG DETECTION
    # ═══════════════════════════════════════════════════════

    async def _check_iceberg(self, symbol, bids, asks, price, now):
        """Iceberg: katta order kichik qismlarga bo'lingan"""
        threshold = self._get_threshold(symbol, price, 500_000)
        for bid_price, bid_qty, bid_usdt in bids[:3]:
            if bid_usdt >= threshold:
                key = f"{symbol}:bid:{bid_price}"
                if key not in self._iceberg_track:
                    self._iceberg_track[key] = {"price": bid_price, "side": "buy", "chunks": 1, "total": bid_usdt, "last": now}
                else:
                    t = self._iceberg_track[key]
                    if now - t["last"] < 5:
                        t["chunks"] += 1
                        t["total"] += bid_usdt
                        t["last"] = now
                        if t["chunks"] >= 3 and t["total"] >= self._get_threshold(symbol, price, 500_000):
                            await self._log_iceberg(symbol, "buy", bid_price, t["total"], t["chunks"])
                            del self._iceberg_track[key]
                    else:
                        self._iceberg_track[key] = {"price": bid_price, "side": "buy", "chunks": 1, "total": bid_usdt, "last": now}

        threshold = self._get_threshold(symbol, price, 500_000)
        for ask_price, ask_qty, ask_usdt in asks[:3]:
            if ask_usdt >= threshold:
                key = f"{symbol}:ask:{ask_price}"
                if key not in self._iceberg_track:
                    self._iceberg_track[key] = {"price": ask_price, "side": "sell", "chunks": 1, "total": ask_usdt, "last": now}
                else:
                    t = self._iceberg_track[key]
                    if now - t["last"] < 5:
                        t["chunks"] += 1
                        t["total"] += ask_usdt
                        t["last"] = now
                        if t["chunks"] >= 3 and t["total"] >= self._get_threshold(symbol, price, 500_000):
                            await self._log_iceberg(symbol, "sell", ask_price, t["total"], t["chunks"])
                            del self._iceberg_track[key]
                    else:
                        self._iceberg_track[key] = {"price": ask_price, "side": "sell", "chunks": 1, "total": ask_usdt, "last": now}

    async def _log_iceberg(self, symbol, side, price, total, chunks):
        # Rate limiting — 60s cooldown
        if not self._check_cooldown(symbol, "iceberg", 60):
            return
        # Faqat $2M+ iceberg log qilish
        if total < 2_000_000:
            return
        try:
            async with aiosqlite.connect(self._db_path) as db:
                await db.execute("""
                    INSERT INTO iceberg_events (symbol, timestamp, side, price, total_usdt, chunks_count, avg_chunk_usdt)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (symbol, time.time(), side, price, total, chunks, total / chunks))
                await db.commit()
            emoji = "🟢" if side == "buy" else "🔴"
            logger.info(f"🧊 ICEBERG: {symbol} {emoji} {side.upper()} ${total/1e6:.1f}M @ {price:.2f} | {chunks} chunks")
        except Exception:
            pass

    # ═══════════════════════════════════════════════════════
    # SWEEP DETECTION
    # ═══════════════════════════════════════════════════════

    async def _check_sweep(self, symbol, bids, asks, price, now):
        """Sweep: devor buzildi (support/resistance sindi)"""
        prev = self._prev_walls.get(symbol, {})
        curr = {}
        threshold = self._get_threshold(symbol, price, 500_000)

        for bid_price, bid_qty, bid_usdt in bids:
            if bid_usdt >= threshold:
                curr[bid_price] = {"side": "bid", "usdt": bid_usdt}

        for ask_price, ask_qty, ask_usdt in asks:
            if ask_usdt >= threshold:
                curr[ask_price] = {"side": "ask", "usdt": ask_usdt}

        for p_price, p_data in prev.items():
            if p_price not in curr:
                if p_data["side"] == "ask" and price > p_price:
                    await self._log_sweep(symbol, "sweep_up", p_price, p_data["usdt"])
                elif p_data["side"] == "bid" and price < p_price:
                    await self._log_sweep(symbol, "sweep_down", p_price, p_data["usdt"])

        self._prev_walls[symbol] = curr

    async def _log_sweep(self, symbol, side, level, usdt):
        # Rate limiting — 60s cooldown
        if not self._check_cooldown(symbol, "sweep", 60):
            return
        # Faqat $2M+ sweep log qilish
        if usdt < 2_000_000:
            return
        try:
            async with aiosqlite.connect(self._db_path) as db:
                await db.execute("""
                    INSERT INTO sweep_events (symbol, timestamp, side, level_price, swept_usdt)
                    VALUES (?, ?, ?, ?, ?)
                """, (symbol, time.time(), side, level, usdt))
                await db.commit()
            emoji = "📈" if side == "sweep_up" else "📉"
            logger.info(f"💨 SWEEP: {symbol} {emoji} {side} | ${usdt/1e6:.1f}M @ {level:.2f} sindi")
        except Exception:
            pass

    # ═══════════════════════════════════════════════════════
    # SUPPORT/RESISTANCE DETECTION
    # ═══════════════════════════════════════════════════════

    async def _sr_detection_loop(self):
        """Har 5 daqiqada S/R level'larni yangilaydi"""
        while self._running:
            try:
                await self._update_sr_levels()
            except Exception as e:
                logger.debug(f"SR detection error: {e}")
            await asyncio.sleep(300)

    async def _update_sr_levels(self):
        async with aiosqlite.connect(self._db_path) as db:
            cutoff = time.time() - 3600
            cursor = await db.execute("""
                SELECT symbol, wall_bid_price, wall_bid_usdt, wall_ask_price, wall_ask_usdt
                FROM ob_snapshots WHERE timestamp > ?
            """, (cutoff,))
            rows = await cursor.fetchall()

        wall_counts = defaultdict(lambda: defaultdict(float))
        for symbol, bid_price, bid_usdt, ask_price, ask_usdt in rows:
            if bid_price > 0 and bid_usdt >= 50_000:
                level = round(bid_price * 20) / 20
                wall_counts[symbol][level] += bid_usdt
            if ask_price > 0 and ask_usdt >= 50_000:
                level = round(ask_price * 20) / 20
                wall_counts[symbol][level] += ask_usdt

        async with aiosqlite.connect(self._db_path) as db:
            for symbol, levels in wall_counts.items():
                for level, total_usdt in levels.items():
                    count = int(total_usdt / 100_000)
                    if count >= 3:
                        cursor = await db.execute("""
                            SELECT current_price FROM ob_snapshots
                            WHERE symbol = ? ORDER BY timestamp DESC LIMIT 1
                        """, (symbol,))
                        row = await cursor.fetchone()
                        if row:
                            level_type = "support" if level < row[0] else "resistance"
                            # Avvalgi first_seen_ts ni tekshirish
                            cursor = await db.execute("""
                                SELECT first_seen_ts FROM sr_levels
                                WHERE symbol = ? AND price = ? AND level_type = ?
                            """, (symbol, level, level_type))
                            existing = await cursor.fetchone()
                            first_seen = existing[0] if existing and existing[0] > 0 else time.time()
                            await db.execute("""
                                INSERT OR REPLACE INTO sr_levels (symbol, price, level_type, strength, first_seen_ts, last_test_ts)
                                VALUES (?, ?, ?, ?, ?, ?)
                            """, (symbol, level, level_type, count, first_seen, time.time()))
            await db.commit()

    # ═══════════════════════════════════════════════════════
    # PUBLIC METHODS
    # ═══════════════════════════════════════════════════════

    def get_current_ob(self, symbol: str) -> dict | None:
        """Hozirgi order book ni qaytaradi"""
        return self._current_ob.get(symbol)

    async def get_heatmap(self, symbol: str, minutes: int = 60) -> list[dict]:
        cutoff = time.time() - minutes * 60
        async with aiosqlite.connect(self._db_path) as db:
            cursor = await db.execute("""
                SELECT timestamp, current_price,
                       bid1_price, bid1_usdt, ask1_price, ask1_usdt,
                       total_bid_usdt, total_ask_usdt, imbalance_ratio
                FROM ob_snapshots WHERE symbol = ? AND timestamp > ?
                ORDER BY timestamp ASC
            """, (symbol, cutoff))
            rows = await cursor.fetchall()
        return [
            {"ts": r[0], "price": r[1], "bid": r[2], "bid_usdt": r[3],
             "ask": r[4], "ask_usdt": r[5], "total_bid": r[6], "total_ask": r[7], "imb": r[8]}
            for r in rows
        ]

    async def get_sr_levels(self, symbol: str) -> dict:
        async with aiosqlite.connect(self._db_path) as db:
            cursor = await db.execute("""
                SELECT price, level_type, strength, first_seen_ts FROM sr_levels
                WHERE symbol = ? ORDER BY strength DESC LIMIT 20
            """, (symbol,))
            rows = await cursor.fetchall()
        supports = [{"price": r[0], "strength": r[2], "first_seen_ts": r[3]} for r in rows if r[1] == "support"]
        resistances = [{"price": r[0], "strength": r[2], "first_seen_ts": r[3]} for r in rows if r[1] == "resistance"]
        return {"supports": supports, "resistances": resistances}

    async def get_events(self, symbol: str, event_type: str, hours: int = 24) -> list[dict]:
        cutoff = time.time() - hours * 3600
        table_map = {"absorption": "absorption_events", "iceberg": "iceberg_events", "sweep": "sweep_events"}
        table = table_map.get(event_type)
        if not table:
            return []
        async with aiosqlite.connect(self._db_path) as db:
            cursor = await db.execute(f"SELECT * FROM {table} WHERE symbol = ? AND timestamp > ? ORDER BY timestamp DESC LIMIT 30", (symbol, cutoff))
            return [dict(r) for r in await cursor.fetchall()]

    async def get_stats(self, symbol: str) -> dict:
        async with aiosqlite.connect(self._db_path) as db:
            result = {}
            for table in ["ob_snapshots", "absorption_events", "iceberg_events", "sweep_events", "sr_levels"]:
                cursor = await db.execute(f"SELECT COUNT(*) FROM {table} WHERE symbol = ?", (symbol,))
                result[table] = (await cursor.fetchone())[0]
            return result

    async def stop(self):
        self._running = False
        for task in self._ws_tasks:
            task.cancel()


# Global instance
bookmap_engine = BookmapEngine()
