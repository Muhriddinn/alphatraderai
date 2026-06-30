"""
CRYPTO MONITOR PRO — Signal Tracker
Real-time PNL tracking, TP/SL hit detection, live message updates, performance stats
"""
import time
import json
from datetime import datetime, timedelta
from typing import Optional
from loguru import logger
from collections import defaultdict


class Signal:
    """Represents an active signal with entry, TP, SL"""
    def __init__(
        self, symbol: str, direction: str, entry_price: float,
        sl_price: float, tp_price: float, strategy: str = "",
        extra_data: dict = None, message_id: int = 0, chat_id: int = 0,
        signal_time: float = 0, signal_id: str = "",
    ):
        self.symbol = symbol
        self.direction = direction  # "LONG" or "SHORT"
        self.entry_price = entry_price
        self.sl_price = sl_price
        self.tp_price = tp_price
        self.strategy = strategy
        self.extra_data = extra_data or {}
        self.message_id = message_id
        self.chat_id = chat_id
        self.signal_time = signal_time or time.time()
        self.signal_id = signal_id
        self.status = "active"  # active, tp_hit, sl_hit, expired
        self.exit_price = 0.0
        self.exit_time = 0.0
        self.pnl_pct = 0.0
        self.max_price = entry_price  # LONG uchun eng yuqori narx
        self.min_price = entry_price  # SHORT uchun eng past narx

    def check_tp_sl(self, current_price: float) -> Optional[str]:
        """Check if TP or SL is hit. Returns 'tp_hit', 'sl_hit', or None"""
        if self.status != "active":
            return None
        # Max narxni yangilash
        if current_price > self.max_price:
            self.max_price = current_price
        if current_price < self.min_price:
            self.min_price = current_price
        if self.direction == "LONG":
            if current_price >= self.tp_price:
                self._close(current_price, "tp_hit")
                return "tp_hit"
            elif current_price <= self.sl_price:
                self._close(current_price, "sl_hit")
                return "sl_hit"
        elif self.direction == "SHORT":
            if current_price <= self.tp_price:
                self._close(current_price, "tp_hit")
                return "tp_hit"
            elif current_price >= self.sl_price:
                self._close(current_price, "sl_hit")
                return "sl_hit"
        return None

    def _close(self, exit_price: float, status: str):
        self.exit_price = exit_price
        self.exit_time = time.time()
        self.status = status
        if self.direction == "LONG":
            self.pnl_pct = ((exit_price - self.entry_price) / self.entry_price) * 100
        else:
            self.pnl_pct = ((self.entry_price - exit_price) / self.entry_price) * 100

    def get_duration_str(self) -> str:
        elapsed = time.time() - self.signal_time
        if elapsed < 60:
            return f"{int(elapsed)}s"
        elif elapsed < 3600:
            return f"{int(elapsed // 60)}m"
        elif elapsed < 86400:
            h = int(elapsed // 3600)
            m = int((elapsed % 3600) // 60)
            return f"{h}h {m}m"
        else:
            d = int(elapsed // 86400)
            h = int((elapsed % 86400) // 3600)
            return f"{d}d {h}h"

    def get_pnl_pct(self, current_price: float) -> float:
        if self.direction == "LONG":
            return ((current_price - self.entry_price) / self.entry_price) * 100
        else:
            return ((self.entry_price - current_price) / self.entry_price) * 100

    def get_max_pnl_pct(self) -> float:
        """Signal chiqqandan beri eng baland foyda %"""
        if self.direction == "LONG":
            return ((self.max_price - self.entry_price) / self.entry_price) * 100
        else:
            return ((self.entry_price - self.min_price) / self.entry_price) * 100

    def to_dict(self) -> dict:
        return {
            "symbol": self.symbol,
            "direction": self.direction,
            "entry": self.entry_price,
            "sl": self.sl_price,
            "tp": self.tp_price,
            "strategy": self.strategy,
            "status": self.status,
            "signal_time": self.signal_time,
            "exit_price": self.exit_price,
            "exit_time": self.exit_time,
            "pnl_pct": self.pnl_pct,
            "message_id": self.message_id,
            "chat_id": self.chat_id,
            "signal_id": self.signal_id,
            "extra_data": self.extra_data,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "Signal":
        s = cls(
            symbol=d["symbol"],
            direction=d["direction"],
            entry_price=d["entry"],
            sl_price=d["sl"],
            tp_price=d["tp"],
            strategy=d.get("strategy", ""),
            signal_time=d.get("signal_time", 0),
            message_id=d.get("message_id", 0),
            chat_id=d.get("chat_id", 0),
            signal_id=d.get("signal_id", ""),
            extra_data=d.get("extra_data", {}),
        )
        s.status = d.get("status", "active")
        s.exit_price = d.get("exit_price", 0)
        s.exit_time = d.get("exit_time", 0)
        s.pnl_pct = d.get("pnl_pct", 0)
        return s


class SignalTracker:
    """
    Tracks all active signals per symbol.
    Manages live message updates via edit_message_text.
    Maintains performance stats (winrate, avg PNL, etc.)
    """

    EXPIRY_HOURS = 24  # Signals expire after 24h

    def __init__(self):
        # symbol -> list of Signal objects (most recent first)
        self._signals: dict[str, list] = defaultdict(list)
        # Completed signals for performance stats
        self._history: list[dict] = []
        # chat_id -> message_id -> Signal (for live edit tracking)
        self._message_map: dict[tuple, Signal] = {}
        self._last_cleanup = time.time()

    def add_signal(self, signal: Signal):
        """Register a new signal"""
        self._signals[signal.symbol].insert(0, signal)
        if signal.message_id and signal.chat_id:
            key = (signal.chat_id, signal.message_id)
            self._message_map[key] = signal
        # Expire old active signals for this symbol
        for old in self._signals[signal.symbol][1:]:
            if old.status == "active":
                old.status = "expired"
        logger.info(
            f"📍 Signal added: {signal.symbol} {signal.direction} "
            f"Entry={signal.entry_price} SL={signal.sl_price} TP={signal.tp_price}"
        )

    def check_price(self, symbol: str, current_price: float) -> list[Signal]:
        """Check all active signals for this symbol against current price"""
        triggered = []
        for signal in self._signals.get(symbol, []):
            if signal.status != "active":
                continue
            result = signal.check_tp_sl(current_price)
            if result:
                triggered.append(signal)
                self._history.append(signal.to_dict())
                logger.info(
                    f"🎯 Signal {result.upper()}: {symbol} {signal.direction} "
                    f"PnL={signal.pnl_pct:+.2f}%"
                )
        self._maybe_cleanup()
        return triggered

    def get_active_signal(self, symbol: str) -> Optional[Signal]:
        for s in self._signals.get(symbol, []):
            if s.status == "active":
                return s
        return None

    def get_all_active(self) -> list[Signal]:
        result = []
        for signals in self._signals.values():
            for s in signals:
                if s.status == "active":
                    result.append(s)
        return result

    def get_signal_by_message(self, chat_id: int, message_id: int) -> Optional[Signal]:
        return self._message_map.get((chat_id, message_id))

    def get_performance_stats(self) -> dict:
        """Calculate overall performance stats"""
        if not self._history:
            return {
                "total": 0, "wins": 0, "losses": 0,
                "winrate": 0, "avg_pnl": 0, "avg_duration": 0,
                "best_trade": 0, "worst_trade": 0,
                "active_count": len(self.get_all_active()),
            }
        
        completed = [h for h in self._history if h["status"] in ("tp_hit", "sl_hit")]
        if not completed:
            return {
                "total": 0, "wins": 0, "losses": 0,
                "winrate": 0, "avg_pnl": 0, "avg_duration": 0,
                "best_trade": 0, "worst_trade": 0,
                "active_count": len(self.get_all_active()),
            }

        wins = sum(1 for h in completed if h["status"] == "tp_hit")
        losses = sum(1 for h in completed if h["status"] == "sl_hit")
        pnls = [h["pnl_pct"] for h in completed]
        durations = [
            h["exit_time"] - h["signal_time"]
            for h in completed
            if h["exit_time"] > 0 and h["signal_time"] > 0
        ]

        return {
            "total": len(completed),
            "wins": wins,
            "losses": losses,
            "winrate": (wins / len(completed) * 100) if completed else 0,
            "avg_pnl": sum(pnls) / len(pnls) if pnls else 0,
            "avg_duration": sum(durations) / len(durations) if durations else 0,
            "best_trade": max(pnls) if pnls else 0,
            "worst_trade": min(pnls) if pnls else 0,
            "active_count": len(self.get_all_active()),
        }

    def get_symbol_stats(self, symbol: str) -> dict:
        """Stats for a specific symbol"""
        history = [h for h in self._history if h["symbol"] == symbol]
        if not history:
            return {"total": 0, "wins": 0, "losses": 0, "winrate": 0, "avg_pnl": 0}
        
        wins = sum(1 for h in history if h["status"] == "tp_hit")
        losses = sum(1 for h in history if h["status"] == "sl_hit")
        pnls = [h["pnl_pct"] for h in history]
        return {
            "total": len(history),
            "wins": wins,
            "losses": losses,
            "winrate": (wins / len(history) * 100) if history else 0,
            "avg_pnl": sum(pnls) / len(pnls) if pnls else 0,
        }

    def _maybe_cleanup(self):
        now = time.time()
        if now - self._last_cleanup < 3600:
            return
        self._last_cleanup = now
        cutoff = now - self.EXPIRY_HOURS * 3600
        for symbol in list(self._signals.keys()):
            self._signals[symbol] = [
                s for s in self._signals[symbol]
                if s.signal_time >= cutoff or s.status == "active"
            ]
            if not self._signals[symbol]:
                del self._signals[symbol]

    def save_to_file(self, path: str = "logs/signal_history.json"):
        try:
            data = {
                "history": self._history[-500:],
                "active": [
                    s.to_dict() for s in self.get_all_active()
                ],
            }
            with open(path, "w") as f:
                json.dump(data, f, indent=2)
        except Exception as e:
            logger.debug(f"Signal history save error: {e}")

    def load_from_file(self, path: str = "logs/signal_history.json"):
        try:
            with open(path) as f:
                data = json.load(f)
            self._history = data.get("history", [])
            for d in data.get("active", []):
                s = Signal.from_dict(d)
                self._signals[s.symbol].append(s)
            logger.info(f"Loaded {len(self._history)} history, {len(self.get_all_active())} active signals")
        except FileNotFoundError:
            pass
        except Exception as e:
            logger.debug(f"Signal history load error: {e}")


# Global instance
signal_tracker = SignalTracker()
