"""
ALPHATRADERAI — Data Collector
Har 1 daqiqada market data ni SQLite ga saqlaydi.
ML model uchun training data tayyorlaydi.

SAQLANADIGAN MA'LUMOTLAR:
- symbol, timestamp
- price, price_change_1m, price_change_5m, price_change_1h
- oi_usdt, oi_change_pct
- volume_5m, volume_1h, volume_spike_pct
- funding_rate
- cvd_1m, cvd_5m
- ob_imbalance_ratio
- whale_volume_usdt (0 agar yo'q)
- liq_long_usdt, liq_short_usdt
- signal_direction (LONG/SHORT/NONE)
- signal_score
- outcome_1m, outcome_5m, outcome_1h, outcome_4h (kelajak narx)
"""
import asyncio
import aiosqlite
import time
from datetime import datetime
from loguru import logger


DB_PATH = "data/market_data.db"

# Schemas
CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS market_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol TEXT NOT NULL,
    timestamp REAL NOT NULL,

    -- Price
    price REAL DEFAULT 0,
    price_change_1m REAL DEFAULT 0,
    price_change_5m REAL DEFAULT 0,
    price_change_1h REAL DEFAULT 0,

    -- Open Interest
    oi_usdt REAL DEFAULT 0,
    oi_change_pct REAL DEFAULT 0,

    -- Volume
    volume_5m REAL DEFAULT 0,
    volume_1h REAL DEFAULT 0,
    volume_spike_pct REAL DEFAULT 0,

    -- Funding
    funding_rate REAL DEFAULT 0,

    -- CVD
    cvd_1m REAL DEFAULT 0,
    cvd_5m REAL DEFAULT 0,
    cvd_15m REAL DEFAULT 0,
    cvd_trend TEXT DEFAULT 'flat',

    -- Order Book
    ob_imbalance_ratio REAL DEFAULT 1.0,

    -- Whale
    whale_volume_usdt REAL DEFAULT 0,
    whale_direction TEXT DEFAULT '',

    -- Liquidation
    liq_long_usdt REAL DEFAULT 0,
    liq_short_usdt REAL DEFAULT 0,

    -- Signal (snapshot paytida)
    signal_direction TEXT DEFAULT '',
    signal_score REAL DEFAULT 0,
    signal_events TEXT DEFAULT '',

    -- Outcome (keyingi 1m, 5m, 1h, 4h narx o'zgarishi)
    outcome_1m REAL DEFAULT NULL,
    outcome_5m REAL DEFAULT NULL,
    outcome_1h REAL DEFAULT NULL,
    outcome_4h REAL DEFAULT NULL,

    -- Features (ML uchun qo'shimcha)
    btc_price REAL DEFAULT 0,
    btc_change_1m REAL DEFAULT 0,
    btc_change_5m REAL DEFAULT 0,
    fear_greed_index INTEGER DEFAULT 50,
    long_short_ratio REAL DEFAULT 1.0
);

CREATE INDEX IF NOT EXISTS idx_symbol_ts ON market_snapshots(symbol, timestamp);
CREATE INDEX IF NOT EXISTS idx_timestamp ON market_snapshots(timestamp);
CREATE INDEX IF NOT EXISTS idx_symbol ON market_snapshots(symbol);
"""

CREATE_SIGNALS_TABLE = """
CREATE TABLE IF NOT EXISTS signals_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol TEXT NOT NULL,
    timestamp REAL NOT NULL,
    direction TEXT NOT NULL,
    entry_price REAL NOT NULL,
    sl_price REAL NOT NULL,
    tp_price REAL NOT NULL,
    score REAL DEFAULT 0,
    events TEXT DEFAULT '',

    -- Outcome
    max_pnl_pct REAL DEFAULT 0,
    min_pnl_pct REAL DEFAULT 0,
    final_pnl_pct REAL DEFAULT 0,
    hit_tp INTEGER DEFAULT 0,
    hit_sl INTEGER DEFAULT 0,
    duration_seconds INTEGER DEFAULT 0,
    status TEXT DEFAULT 'active'
);

CREATE INDEX IF NOT EXISTS idx_signals_symbol ON signals_log(symbol);
CREATE INDEX IF NOT EXISTS idx_signals_ts ON signals_log(timestamp);
"""


class DataCollector:
    """
    Market data ni har 1 daqiqada SQLite ga saqlaydi.
    ML model uchun training data tayyorlaydi.
    """

    def __init__(self):
        self._running = False
        self._db_path = DB_PATH
        self._last_snapshot: dict[str, dict] = {}  # symbol -> last snapshot

    async def start(self):
        self._running = True
        import os
        os.makedirs("data", exist_ok=True)
        await self._init_db()
        asyncio.create_task(self._snapshot_loop())
        asyncio.create_task(self._update_outcomes_loop())
        logger.info("✅ Data Collector started")

    async def _init_db(self):
        async with aiosqlite.connect(self._db_path) as db:
            await db.executescript(CREATE_TABLE)
            await db.executescript(CREATE_SIGNALS_TABLE)
            await db.commit()
        logger.info(f"✅ Data DB ready: {self._db_path}")

    async def _snapshot_loop(self):
        """Har 1 daqiqada barcha active symbollar uchun snapshot oladi"""
        while self._running:
            try:
                await self._take_snapshots()
            except Exception as e:
                logger.debug(f"Snapshot error: {e}")
            await asyncio.sleep(60)

    async def _take_snapshots(self):
        """Barcha tracked symbollar uchun ma'lumot to'playdi va saqlaydi"""
        from core.state_manager import state_manager
        from modules.price_tracker import price_tracker
        from modules.cvd_tracker import cvd_tracker
        from modules.volume_scanner import VolumeScanner
        from modules.ob_tracker import OrderBookWallTracker

        symbols = await state_manager.get_symbols("binance", "futures")
        now = time.time()

        snapshots = []

        for symbol in list(symbols)[:500]:
            try:
                snapshot = await self._build_snapshot(symbol, now, state_manager, price_tracker, cvd_tracker)
                if snapshot:
                    snapshots.append(snapshot)
            except Exception as e:
                logger.debug(f"Snapshot build error {symbol}: {e}")

        if snapshots:
            await self._save_snapshots(snapshots)
            logger.debug(f"📊 Snapshot: {len(snapshots)} symbol saqlandi")

    async def _build_snapshot(self, symbol: str, now: float, state_manager, price_tracker, cvd_tracker) -> dict | None:
        """Bitta symbol uchun snapshot yasaydi"""
        # Price
        pc = price_tracker.get_price_changes(symbol)
        price = pc.get("current", 0)
        if price <= 0:
            return None

        # OI
        oi_history = await state_manager.get_oi_history("binance", symbol, 2)
        oi_usdt = oi_history[0]["oi_usdt"] if oi_history else 0
        oi_prev = oi_history[1]["oi_usdt"] if len(oi_history) > 1 else 0
        oi_change = ((oi_usdt - oi_prev) / oi_prev * 100) if oi_prev > 0 else 0

        # Volume
        vol_5m = sum(v["usdt"] for v in price_tracker._history.get(symbol, []) if now - v[1] <= 300)
        vol_1h = sum(v["usdt"] for v in price_tracker._history.get(symbol, []) if now - v[1] <= 3600)
        vol_5m_prev = sum(v["usdt"] for v in price_tracker._history.get(symbol, []) if 300 < now - v[1] <= 600)
        volume_spike_pct = ((vol_5m - vol_5m_prev) / vol_5m_prev * 100) if vol_5m_prev > 0 else 0

        # Funding
        funding_data = await state_manager.get_funding("binance", symbol)
        funding = funding_data[0].get("rate", 0) if funding_data and funding_data[0] else 0

        # CVD
        cvd_data = cvd_tracker.get_cvd_data(symbol)
        cvd_1m = cvd_data.get("cvd_1m", 0)
        cvd_5m = cvd_data.get("cvd_5m", 0)
        cvd_15m = cvd_data.get("cvd_15m", 0)
        cvd_trend = cvd_data.get("cvd_trend", "flat")

        # OB
        ob_imbalance = 1.0
        try:
            from modules.bookmap_engine import bookmap_engine
            ob = bookmap_engine.get_current_ob(symbol)
            if ob:
                bids = ob.get("bids", [])
                asks = ob.get("asks", [])
                if bids and asks:
                    total_bid = sum(b[2] for b in bids)
                    total_ask = sum(a[2] for a in asks)
                    if total_ask > 0:
                        ob_imbalance = total_bid / total_ask
        except Exception:
            pass

        # BTC (market context)
        btc_pc = price_tracker.get_price_changes("BTCUSDT")
        btc_price = btc_pc.get("current", 0)
        btc_change_1m = btc_pc.get("change_1m", 0)
        btc_change_5m = btc_pc.get("change_5m", 0)

        return {
            "symbol": symbol,
            "timestamp": now,
            "price": price,
            "price_change_1m": pc.get("change_1m", 0),
            "price_change_5m": pc.get("change_5m", 0),
            "price_change_1h": pc.get("change_1h", 0),
            "oi_usdt": oi_usdt,
            "oi_change_pct": oi_change,
            "volume_5m": vol_5m,
            "volume_1h": vol_1h,
            "volume_spike_pct": volume_spike_pct,
            "funding_rate": funding,
            "cvd_1m": cvd_1m,
            "cvd_5m": cvd_5m,
            "cvd_15m": cvd_15m,
            "cvd_trend": cvd_trend,
            "ob_imbalance_ratio": ob_imbalance,
            "whale_volume_usdt": 0,
            "whale_direction": "",
            "liq_long_usdt": 0,
            "liq_short_usdt": 0,
            "signal_direction": "",
            "signal_score": 0,
            "signal_events": "",
            "btc_price": btc_price,
            "btc_change_1m": btc_change_1m,
            "btc_change_5m": btc_change_5m,
        }

    async def _save_snapshots(self, snapshots: list[dict]):
        """Snapshots ni SQLite ga saqlaydi"""
        if not snapshots:
            return

        cols = list(snapshots[0].keys())
        placeholders = ", ".join(["?" for _ in cols])
        col_names = ", ".join(cols)
        sql = f"INSERT INTO market_snapshots ({col_names}) VALUES ({placeholders})"

        values = [[s[c] for c in cols] for s in snapshots]

        async with aiosqlite.connect(self._db_path) as db:
            await db.executemany(sql, values)
            await db.commit()

    async def _update_outcomes_loop(self):
        """Eski snapshotlarning outcome (kelajak narx) ni yangilaydi"""
        while self._running:
            try:
                await self._update_outcomes()
            except Exception as e:
                logger.debug(f"Outcome update error: {e}")
            await asyncio.sleep(60)

    async def _update_outcomes(self):
        """1, 5, 60, 240 daqiqa oldingi snapshotlarning natijasini hisoblaydi"""
        now = time.time()

        async with aiosqlite.connect(self._db_path) as db:
            # 1m oldingi snapshotlarni topish va outcome ni yangilash
            for delta_sec, col in [(60, "outcome_1m"), (300, "outcome_5m"), (3600, "outcome_1h"), (14400, "outcome_4h")]:
                cutoff = now - delta_sec
                # Oldingi snapshotlar (outcome hali NULL)
                cursor = await db.execute("""
                    SELECT id, symbol, price, timestamp FROM market_snapshots
                    WHERE timestamp <= ? AND timestamp > ? AND {} IS NULL
                    LIMIT 500
                """.format(col), (cutoff, cutoff - 10))

                rows = await cursor.fetchall()
                for row_id, symbol, old_price, old_ts in rows:
                    # Hozirgi narxni topish
                    cursor2 = await db.execute("""
                        SELECT price FROM market_snapshots
                        WHERE symbol = ? AND timestamp > ?
                        ORDER BY timestamp ASC LIMIT 1
                    """, (symbol, old_ts))
                    future = await cursor2.fetchone()
                    if future and old_price > 0:
                        outcome = (future[0] - old_price) / old_price * 100
                        await db.execute(f"""
                            UPDATE market_snapshots SET {col} = ? WHERE id = ?
                        """, (outcome, row_id))

            await db.commit()

    async def log_signal(self, symbol: str, direction: str, entry: float, sl: float, tp: float,
                          score: float, events: str):
        """Signal chiqqanda saqlaydi — keyin outcome ni tekshirish uchun"""
        async with aiosqlite.connect(self._db_path) as db:
            await db.execute("""
                INSERT INTO signals_log (symbol, timestamp, direction, entry_price, sl_price, tp_price, score, events)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (symbol, time.time(), direction, entry, sl, tp, score, events))
            await db.commit()

    async def get_stats(self) -> dict:
        """Statistika — qancha ma'lumot yig'ildi"""
        async with aiosqlite.connect(self._db_path) as db:
            cursor = await db.execute("SELECT COUNT(*) FROM market_snapshots")
            total = (await cursor.fetchone())[0]

            cursor = await db.execute("SELECT COUNT(DISTINCT symbol) FROM market_snapshots")
            symbols = (await cursor.fetchone())[0]

            cursor = await db.execute("SELECT MIN(timestamp), MAX(timestamp) FROM market_snapshots")
            min_ts, max_ts = await cursor.fetchone()

            cursor = await db.execute("SELECT COUNT(*) FROM signals_log")
            signals = (await cursor.fetchone())[0]

            return {
                "total_snapshots": total,
                "unique_symbols": symbols,
                "oldest": datetime.fromtimestamp(min_ts).isoformat() if min_ts else "N/A",
                "newest": datetime.fromtimestamp(max_ts).isoformat() if max_ts else "N/A",
                "total_signals": signals,
            }

    async def stop(self):
        self._running = False


# Global instance
data_collector = DataCollector()
