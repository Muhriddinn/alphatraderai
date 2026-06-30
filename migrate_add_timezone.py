"""
Migratsiya: user_settings jadvaliga timezone_offset ustunini qo'shish.

Ishlatish:
    python migrate_add_timezone.py

Eslatma: bot to'xtatilgan holatda ishlatish tavsiya etiladi.
Agar ustun allaqachon mavjud bo'lsa, hech narsa o'zgartirmaydi (xavfsiz).
"""
import asyncio
import sqlite3
import os

DB_PATH = os.path.join(os.path.dirname(__file__), "crypto_monitor.db")


def migrate():
    if not os.path.exists(DB_PATH):
        print(f"❌ DB topilmadi: {DB_PATH}")
        return

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.execute("PRAGMA table_info(user_settings)")
    columns = [row[1] for row in cur.fetchall()]

    if "timezone_offset" in columns:
        print("✅ timezone_offset ustuni allaqachon mavjud, hech narsa qilinmadi.")
    else:
        cur.execute("ALTER TABLE user_settings ADD COLUMN timezone_offset INTEGER DEFAULT 0")
        conn.commit()
        print("✅ timezone_offset ustuni qo'shildi (default = 0, ya'ni UTC).")

    conn.close()


if __name__ == "__main__":
    migrate()
