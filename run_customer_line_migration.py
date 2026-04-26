import sqlite3
from pathlib import Path

DB_PATH = Path("data/db/salon.db")

conn = sqlite3.connect(DB_PATH)
cur = conn.cursor()

cur.execute("PRAGMA table_info(customers)")
columns = [row[1] for row in cur.fetchall()]

if "line_user_id" not in columns:
    cur.execute("ALTER TABLE customers ADD COLUMN line_user_id TEXT")
    print("customers に line_user_id を追加しました。")
else:
    print("line_user_id は既にあります。")

conn.commit()
conn.close()