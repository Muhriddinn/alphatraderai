import sqlite3, os

for db_path in ['data/market_data.db', 'data/bookmap.db', 'data/state.db']:
    if not os.path.exists(db_path):
        print(f"\n=== {db_path} — NOT FOUND ===")
        continue
    print(f"\n=== {db_path} ({os.path.getsize(db_path) / 1024 / 1024:.1f} MB) ===")
    c = sqlite3.connect(db_path)
    tables = c.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    for (name,) in tables:
        try:
            count = c.execute(f'SELECT COUNT(*) FROM [{name}]').fetchone()[0]
            print(f"  {name}: {count:,} rows")
        except:
            print(f"  {name}: (error reading)")
    c.close()
