"""
CRYPTO MONITOR PRO — In-Memory State Manager
Redis o'rniga oddiy Python dict ishlatiladi
"""
import asyncio
from datetime import datetime
from typing import Optional
from loguru import logger
from collections import defaultdict


class StateManager:
    def __init__(self):
        self._data = {}
        self._sets = defaultdict(set)
        self._lists = defaultdict(list)
        self._counters = defaultdict(int)
        self._connected = False

    async def connect(self):
        self._connected = True
        logger.info("✅ Memory State Manager ready (Redis o'rniga)")

    async def disconnect(self):
        self._connected = False

    def _key(self, exchange, symbol, data_type):
        return f"crypto:{exchange}:{symbol}:{data_type}"

    def _global_key(self, data_type):
        return f"crypto:global:{data_type}"

    async def set_ticker(self, exchange, symbol, data):
        self._data[self._key(exchange, symbol, "ticker")] = data

    async def get_ticker(self, exchange, symbol):
        return self._data.get(self._key(exchange, symbol, "ticker"))

    async def set_oi(self, exchange, symbol, oi_usdt, timestamp):
        key = self._key(exchange, symbol, "oi_current")
        self._data[key] = {"oi_usdt": oi_usdt, "ts": timestamp}
        hist_key = self._key(exchange, symbol, "oi_history")
        self._lists[hist_key].insert(0, {"oi_usdt": oi_usdt, "ts": timestamp})
        self._lists[hist_key] = self._lists[hist_key][:60]

    async def get_oi_history(self, exchange, symbol, count=10):
        hist_key = self._key(exchange, symbol, "oi_history")
        return self._lists[hist_key][:count]

    async def get_oi_baseline(self, exchange, symbol):
        history = await self.get_oi_history(exchange, symbol, 60)
        if len(history) >= 10:
            return history[-1]["oi_usdt"]
        return None

    async def update_volume_baseline(self, exchange, symbol, volume):
        hist_key = self._key(exchange, symbol, "vol_history")
        self._lists[hist_key].insert(0, volume)
        self._lists[hist_key] = self._lists[hist_key][:20]
        items = self._lists[hist_key]
        if len(items) >= 5:
            avg = sum(items) / len(items)
            self._data[self._key(exchange, symbol, "vol_baseline")] = avg

    async def get_volume_baseline(self, exchange, symbol):
        return self._data.get(self._key(exchange, symbol, "vol_baseline"))

    async def add_liquidation(self, exchange, symbol, side, usdt, ts):
        key = self._key(exchange, symbol, "liq_window")
        self._lists[key].insert(0, {"side": side, "usdt": usdt, "ts": ts})
        self._lists[key] = self._lists[key][:500]

    async def get_liquidations_window(self, exchange, symbol, seconds=60):
        key = self._key(exchange, symbol, "liq_window")
        now = datetime.utcnow().timestamp()
        cutoff = now - seconds
        return [l for l in self._lists[key] if l["ts"] >= cutoff]

    async def add_whale_trade(self, exchange, symbol, side, usdt, ts):
        key = self._key(exchange, symbol, "whale_window")
        self._lists[key].insert(0, {"side": side, "usdt": usdt, "ts": ts})
        self._lists[key] = self._lists[key][:100]

    async def get_whale_window(self, exchange, symbol, seconds=60):
        key = self._key(exchange, symbol, "whale_window")
        now = datetime.utcnow().timestamp()
        cutoff = now - seconds
        return [w for w in self._lists[key] if w["ts"] >= cutoff]

    async def set_last_whale(self, exchange, symbol, direction, usdt, ts):
        """Oxirgi whale eventini saqlash — kelgusi xabarlarda fallback uchun"""
        key = self._key(exchange, symbol, "last_whale")
        self._data[key] = {"direction": direction, "usdt": usdt, "ts": ts}
        # Global last_whale — hamma coin uchun oxirgisi
        self._data["global:last_whale"] = {"symbol": symbol, "direction": direction, "usdt": usdt, "ts": ts}

    async def get_last_whale(self, exchange, symbol):
        key = self._key(exchange, symbol, "last_whale")
        return self._data.get(key)

    async def get_global_last_whale(self):
        """Global oxirgi whale — istalgan coin uchun"""
        return self._data.get("global:last_whale")

    async def set_funding(self, exchange, symbol, rate, next_time):
        key = self._key(exchange, symbol, "funding")
        prev_key = self._key(exchange, symbol, "funding_prev")
        current = self._data.get(key)
        if current:
            self._data[prev_key] = current
        self._data[key] = {"rate": rate, "next_time": next_time}

    async def get_funding(self, exchange, symbol):
        key = self._key(exchange, symbol, "funding")
        prev_key = self._key(exchange, symbol, "funding_prev")
        return self._data.get(key), self._data.get(prev_key)

    async def add_symbol(self, exchange, symbol, market_type):
        key = self._global_key(f"symbols:{exchange}:{market_type}")
        self._sets[key].add(symbol)

    async def get_symbols(self, exchange, market_type):
        key = self._global_key(f"symbols:{exchange}:{market_type}")
        return self._sets[key].copy()

    async def remove_symbol(self, exchange, symbol, market_type):
        key = self._global_key(f"symbols:{exchange}:{market_type}")
        self._sets[key].discard(symbol)

    async def is_event_sent(self, exchange, symbol, event_type):
        key = self._key(exchange, symbol, f"alert_cooldown:{event_type}")
        entry = self._data.get(key)
        if not entry:
            return False
        if datetime.utcnow().timestamp() > entry["expires"]:
            del self._data[key]
            return False
        return True

    async def mark_event_sent(self, exchange, symbol, event_type, cooldown=120):
        key = self._key(exchange, symbol, f"alert_cooldown:{event_type}")
        self._data[key] = {"expires": datetime.utcnow().timestamp() + cooldown}

    async def cache_user_settings(self, user_id, settings_data):
        self._data[f"user:{user_id}:settings"] = settings_data

    async def get_cached_user_settings(self, user_id):
        return self._data.get(f"user:{user_id}:settings")

    async def invalidate_user_cache(self, user_id):
        self._data.pop(f"user:{user_id}:settings", None)

    async def increment_stat(self, stat_name, amount=1):
        self._counters[stat_name] += amount

    async def get_stat(self, stat_name):
        return self._counters[stat_name]

    async def get_all_stats(self):
        return dict(self._counters)


state_manager = StateManager()
