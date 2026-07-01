import sqlite3
c = sqlite3.connect('/app/data/state.db')
print("counters:", c.execute('SELECT name,value FROM counters').fetchall())
print("kv_count:", c.execute('SELECT COUNT(*) FROM kv').fetchone()[0])
c.close()
