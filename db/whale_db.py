"""
Whale Events DB — oxirgi whale eventlarni saqlash va olish
"""
import sqlite3
import time
from typing import Optional


DB_PATH = "crypto_monitor.db"


def save_whale_event(symbol: str, exchange: str, direction: str, volume_usdt: float,
                     volume_24h: float, volume_pct: float, price: float,
                     price_change_pct: float, duration_seconds: int, order_count: int):
    """Whale eventni DB ga saqlash"""
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.execute(
            """INSERT INTO whale_events 
               (symbol, exchange, direction, volume_usdt, volume_24h, volume_pct, 
                price, price_change_pct, duration_seconds, order_count)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (symbol, exchange, direction, volume_usdt, volume_24h, volume_pct,
             price, price_change_pct, duration_seconds, order_count)
        )
        conn.commit()
        conn.close()
    except Exception as e:
        pass


def get_last_whale_time(symbol: str) -> Optional[float]:
    """Oxirgi whale event vaqtini olish (Unix timestamp)"""
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.execute(
            "SELECT created_at FROM whale_events WHERE symbol = ? ORDER BY created_at DESC LIMIT 1",
            (symbol,)
        )
        row = cursor.fetchone()
        conn.close()
        if row:
            from datetime import datetime
            dt = datetime.strptime(row[0], "%Y-%m-%d %H:%M:%S") if isinstance(row[0], str) else row[0]
            return dt.timestamp()
    except Exception:
        pass
    return None


def get_last_whale_info(symbol: str) -> Optional[dict]:
    """Oxirgi whale event haqida to'liq ma'lumot"""
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        cursor = conn.execute(
            """SELECT symbol, exchange, direction, volume_usdt, volume_24h, 
                      volume_pct, price, price_change_pct, duration_seconds, created_at
               FROM whale_events WHERE symbol = ? ORDER BY created_at DESC LIMIT 1""",
            (symbol,)
        )
        row = cursor.fetchone()
        conn.close()
        if row:
            from datetime import datetime
            dt = datetime.strptime(row["created_at"], "%Y-%m-%d %H:%M:%S") if isinstance(row["created_at"], str) else row["created_at"]
            return {
                "symbol": row["symbol"],
                "exchange": row["exchange"],
                "direction": row["direction"],
                "volume_usdt": row["volume_usdt"],
                "volume_24h": row["volume_24h"],
                "volume_pct": row["volume_pct"],
                "price": row["price"],
                "price_change_pct": row["price_change_pct"],
                "duration_seconds": row["duration_seconds"],
                "created_at": dt.timestamp(),
            }
    except Exception:
        pass
    return None


def fmt_last_seen(timestamp: float) -> str:
    """Unix timestamp dan '6 kun oldin', '2 soat oldin' formatiga"""
    diff = time.time() - timestamp
    if diff < 60:
        return f"{int(diff)} soniya oldin"
    elif diff < 3600:
        return f"{int(diff // 60)} daqiqa oldin"
    elif diff < 86400:
        hours = int(diff // 3600)
        mins = int((diff % 3600) // 60)
        if mins > 0:
            return f"{hours} soat {mins} daqiqa oldin"
        return f"{hours} soat oldin"
    else:
        days = int(diff // 86400)
        hours = int((diff % 86400) // 3600)
        if hours > 0:
            return f"{days} kun {hours} soat oldin"
        return f"{days} kun oldin"
