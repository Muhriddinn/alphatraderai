"""
ALPHATRADERAI — SQLite-backed State Manager
RAM o'rniga SQLite ishlatiladi — 512MB Render free tier uchun
"""
import asyncio
import json
import os
import time
from datetime import datetime
from typing import Optional, Any
from loguru import logger

try:
    import aiosqlite
except ImportError:
    aiosqlite = None


class StateManager:
    def __init__(self, db_path: str = "data/state.db"):
        self._db_path = db_path
        self._db: Optional[aiosqlite.Connection] = None
        self._connected = False
        self._cache: dict[str, Any] = {}
        self._cache_max = 2000
        self._write_queue: list[tuple] = []
        self._flush_task: Optional[asyncio.Task] = None
        self._counter_dirty: bool = False

    async def connect(self):
        os.makedirs(os.path.dirname(self._db_path), exist_ok=True)
        self._db = await aiosqlite.connect(self._db_path)
        self._db.row_factory = aiosqlite.Row
        await self._db.execute("PRAGMA journal_mode=WAL")
        await self._db.execute("PRAGMA synchronous=NORMAL")
        await self._db.execute("PRAGMA cache_size=-64000")

        await self._db.executescript("""
            CREATE TABLE IF NOT EXISTS kv (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL,
                updated_at REAL NOT NULL
            );
            CREATE TABLE IF NOT EXISTS lists (
                key TEXT NOT NULL,
                idx INTEGER NOT NULL,
                value TEXT NOT NULL,
                PRIMARY KEY(key, idx)
            );
            CREATE TABLE IF NOT EXISTS sets (
                key TEXT NOT NULL,
                member TEXT NOT NULL,
                PRIMARY KEY(key, member)
            );
            CREATE TABLE IF NOT EXISTS counters (
                name TEXT PRIMARY KEY,
                value INTEGER NOT NULL DEFAULT 0
            );
            CREATE INDEX IF NOT EXISTS idx_kv_updated ON kv(updated_at);
        """)
        await self._db.commit()

        self._connected = True
        self._flush_task = asyncio.create_task(self._periodic_flush())
        logger.info("✅ SQLite State Manager ready")

    async def disconnect(self):
        if self._flush_task:
            self._flush_task.cancel()
        if self._db:
            await self._flush()
            await self._db.close()
        self._connected = False

    async def _periodic_flush(self):
        while True:
            await asyncio.sleep(5)
            await self._flush()

    async def _flush(self):
        if not self._db:
            return
        flushed = False
        if self._write_queue:
            batch = self._write_queue[:]
            self._write_queue.clear()
            try:
                await self._db.executemany(
                    "INSERT OR REPLACE INTO kv(key, value, updated_at) VALUES(?,?,?)",
                    batch
                )
                flushed = True
            except Exception as e:
                logger.debug(f"SQLite flush error: {e}")
        # Only commit if counter was incremented
        if self._counter_dirty:
            try:
                await self._db.commit()
                self._counter_dirty = False
                flushed = True
            except Exception as e:
                logger.debug(f"SQLite commit error: {e}")

    def _key(self, exchange, symbol, data_type):
        return f"crypto:{exchange}:{symbol}:{data_type}"

    def _global_key(self, data_type):
        return f"crypto:global:{data_type}"

    async def _kv_get(self, key: str) -> Optional[Any]:
        if key in self._cache:
            return self._cache[key]
        if not self._db:
            return None
        try:
            async with self._db.execute("SELECT value FROM kv WHERE key=?", (key,)) as cur:
                row = await cur.fetchone()
                if row:
                    val = json.loads(row[0])
                    self._cache[key] = val
                    return val
        except Exception:
            pass
        return None

    async def _kv_set(self, key: str, value: Any):
        self._cache[key] = value
        if len(self._cache) > self._cache_max:
            oldest = list(self._cache.keys())[:500]
            for k in oldest:
                self._cache.pop(k, None)
        self._write_queue.append((key, json.dumps(value), time.time()))

    async def _list_get(self, key: str, limit: int = 500) -> list:
        if not self._db:
            return []
        try:
            async with self._db.execute(
                "SELECT value FROM lists WHERE key=? ORDER BY idx LIMIT ?", (key, limit)
            ) as cur:
                rows = await cur.fetchall()
                return [json.loads(r[0]) for r in rows]
        except Exception:
            return []

    async def _list_insert(self, key: str, value: Any, max_len: int = 500):
        if not self._db:
            return
        try:
            await self._db.execute(
                "INSERT INTO lists(key, idx, value) VALUES(?,?,?)",
                (key, 0, json.dumps(value))
            )
            await self._db.execute(
                "DELETE FROM lists WHERE key=? AND idx NOT IN (SELECT idx FROM lists WHERE key=? ORDER BY idx DESC LIMIT ?)",
                (key, key, max_len)
            )
            await self._db.commit()
        except Exception:
            pass

    async def _set_add(self, key: str, member: str):
        if not self._db:
            return
        try:
            await self._db.execute(
                "INSERT OR IGNORE INTO sets(key, member) VALUES(?,?)",
                (key, member)
            )
            await self._db.commit()
        except Exception:
            pass

    async def _set_get(self, key: str) -> set:
        if not self._db:
            return set()
        try:
            async with self._db.execute("SELECT member FROM sets WHERE key=?", (key,)) as cur:
                rows = await cur.fetchall()
                return {r[0] for r in rows}
        except Exception:
            return set()

    async def _set_remove(self, key: str, member: str):
        if not self._db:
            return
        try:
            await self._db.execute("DELETE FROM sets WHERE key=? AND member=?", (key, member))
            await self._db.commit()
        except Exception:
            pass

    async def _counter_get(self, name: str) -> int:
        if not self._db:
            return 0
        try:
            async with self._db.execute("SELECT value FROM counters WHERE name=?", (name,)) as cur:
                row = await cur.fetchone()
                return row[0] if row else 0
        except Exception:
            return 0

    async def _counter_inc(self, name: str, amount: int = 1):
        if not self._db:
            return
        try:
            await self._db.execute(
                "INSERT INTO counters(name, value) VALUES(?,?) ON CONFLICT(name) DO UPDATE SET value=value+?",
                (name, amount, amount)
            )
            self._counter_dirty = True
        except Exception as e:
            logger.debug(f"Counter inc error ({name}): {e}")

    async def set_ticker(self, exchange, symbol, data):
        await self._kv_set(self._key(exchange, symbol, "ticker"), data)

    async def get_ticker(self, exchange, symbol):
        return await self._kv_get(self._key(exchange, symbol, "ticker"))

    async def set_oi(self, exchange, symbol, oi_usdt, timestamp):
        key = self._key(exchange, symbol, "oi_current")
        await self._kv_set(key, {"oi_usdt": oi_usdt, "ts": timestamp})
        hist_key = self._key(exchange, symbol, "oi_history")
        await self._list_insert(hist_key, {"oi_usdt": oi_usdt, "ts": timestamp}, max_len=60)

    async def get_oi_history(self, exchange, symbol, count=10):
        hist_key = self._key(exchange, symbol, "oi_history")
        return await self._list_get(hist_key, limit=count)

    async def get_oi_baseline(self, exchange, symbol):
        history = await self.get_oi_history(exchange, symbol, 60)
        if len(history) >= 10:
            return history[-1]["oi_usdt"]
        return None

    async def update_volume_baseline(self, exchange, symbol, volume):
        hist_key = self._key(exchange, symbol, "vol_history")
        items = await self._list_get(hist_key, limit=20)
        items.insert(0, volume)
        items = items[:20]
        if not self._db:
            return
        try:
            await self._db.execute("DELETE FROM lists WHERE key=?", (hist_key,))
            for i, v in enumerate(items):
                await self._db.execute(
                    "INSERT INTO lists(key, idx, value) VALUES(?,?,?)",
                    (hist_key, i, json.dumps(v))
                )
            await self._db.commit()
        except Exception:
            pass
        if len(items) >= 5:
            avg = sum(items) / len(items)
            await self._kv_set(self._key(exchange, symbol, "vol_baseline"), avg)

    async def get_volume_baseline(self, exchange, symbol):
        return await self._kv_get(self._key(exchange, symbol, "vol_baseline"))

    async def add_liquidation(self, exchange, symbol, side, usdt, ts):
        key = self._key(exchange, symbol, "liq_window")
        await self._list_insert(key, {"side": side, "usdt": usdt, "ts": ts}, max_len=500)

    async def get_liquidations_window(self, exchange, symbol, seconds=60):
        key = self._key(exchange, symbol, "liq_window")
        items = await self._list_get(key, limit=500)
        now = time.time()
        cutoff = now - seconds
        return [l for l in items if l.get("ts", 0) >= cutoff]

    async def add_whale_trade(self, exchange, symbol, side, usdt, ts):
        key = self._key(exchange, symbol, "whale_window")
        await self._list_insert(key, {"side": side, "usdt": usdt, "ts": ts}, max_len=100)

    async def get_whale_window(self, exchange, symbol, seconds=60):
        key = self._key(exchange, symbol, "whale_window")
        items = await self._list_get(key, limit=100)
        now = time.time()
        cutoff = now - seconds
        return [w for w in items if w.get("ts", 0) >= cutoff]

    async def set_last_whale(self, exchange, symbol, direction, usdt, ts):
        key = self._key(exchange, symbol, "last_whale")
        await self._kv_set(key, {"direction": direction, "usdt": usdt, "ts": ts})
        await self._kv_set("global:last_whale", {"symbol": symbol, "direction": direction, "usdt": usdt, "ts": ts})

    async def get_last_whale(self, exchange, symbol):
        return await self._kv_get(self._key(exchange, symbol, "last_whale"))

    async def get_global_last_whale(self):
        return await self._kv_get("global:last_whale")

    async def set_funding(self, exchange, symbol, rate, next_time):
        key = self._key(exchange, symbol, "funding")
        prev_key = self._key(exchange, symbol, "funding_prev")
        current = await self._kv_get(key)
        if current:
            await self._kv_set(prev_key, current)
        await self._kv_set(key, {"rate": rate, "next_time": next_time})

    async def get_funding(self, exchange, symbol):
        key = self._key(exchange, symbol, "funding")
        prev_key = self._key(exchange, symbol, "funding_prev")
        return await self._kv_get(key), await self._kv_get(prev_key)

    async def add_symbol(self, exchange, symbol, market_type):
        key = self._global_key(f"symbols:{exchange}:{market_type}")
        await self._set_add(key, symbol)

    async def get_symbols(self, exchange, market_type):
        key = self._global_key(f"symbols:{exchange}:{market_type}")
        return await self._set_get(key)

    async def remove_symbol(self, exchange, symbol, market_type):
        key = self._global_key(f"symbols:{exchange}:{market_type}")
        await self._set_remove(key, symbol)

    async def is_event_sent(self, exchange, symbol, event_type):
        key = self._key(exchange, symbol, f"alert_cooldown:{event_type}")
        entry = await self._kv_get(key)
        if not entry:
            return False
        if time.time() > entry.get("expires", 0):
            return False
        return True

    async def mark_event_sent(self, exchange, symbol, event_type, cooldown=120):
        key = self._key(exchange, symbol, f"alert_cooldown:{event_type}")
        await self._kv_set(key, {"expires": time.time() + cooldown})

    async def cache_user_settings(self, user_id, settings_data):
        await self._kv_set(f"user:{user_id}:settings", settings_data)

    async def get_cached_user_settings(self, user_id):
        return await self._kv_get(f"user:{user_id}:settings")

    async def invalidate_user_cache(self, user_id):
        pass

    async def increment_stat(self, stat_name, amount=1):
        await self._counter_inc(stat_name, amount)

    async def get_stat(self, stat_name):
        return await self._counter_get(stat_name)

    async def get_all_stats(self):
        if not self._db:
            return {}
        try:
            async with self._db.execute("SELECT name, value FROM counters") as cur:
                rows = await cur.fetchall()
                return {r[0]: r[1] for r in rows}
        except Exception:
            return {}

    async def set_bootstrap(self, symbol: str, data: dict):
        await self._kv_set(f"bootstrap:{symbol}", data)

    async def get_bootstrap(self, symbol: str) -> Optional[dict]:
        return await self._kv_get(f"bootstrap:{symbol}")

    async def set_volume_data(self, key: str, data: list):
        if not self._db:
            return
        try:
            await self._db.execute("DELETE FROM lists WHERE key=?", (key,))
            for i, v in enumerate(data[-200:]):
                await self._db.execute(
                    "INSERT INTO lists(key, idx, value) VALUES(?,?,?)",
                    (key, i, json.dumps(v))
                )
            await self._db.commit()
        except Exception:
            pass

    async def get_volume_data(self, key: str, limit: int = 200) -> list:
        return await self._list_get(key, limit=limit)


state_manager = StateManager()
