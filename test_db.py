# test_db.py
from viewer.db import get_conn, fetch_one
print("Testing DB connection...")
conn = get_conn()
print("Connected:", bool(conn))
print("Server version:", conn.info.server_version if hasattr(conn, 'info') else "unknown")
print("Counting tickets (example):", fetch_one("SELECT COUNT(*) AS c FROM tickets")['c'])
