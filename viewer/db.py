# viewer/db.py
import os
import json
from pathlib import Path

# Try relative import first (package mode), else absolute (script mode)
try:
    from . import settings
except Exception:
    import sys
    # Allow running `python viewer/app.py`
    sys.path.append(os.path.dirname(__file__))
    import settings  # type: ignore

import psycopg
from psycopg.rows import dict_row

# ---------- Connection ----------
def get_conn():
    """
    Connect to Postgres using psycopg3.
    - prepare_threshold=None disables server-side prepared statements
      (avoids 'prepared statement already exists' collisions).
    - row_factory=dict_row returns dict rows.
    """
    dsn = settings.DATABASE_URL
    conn = psycopg.connect(dsn, row_factory=dict_row, prepare_threshold=None)
    return conn  # no autocommit needed for reads

# ---------- Basic helpers ----------
def fetch_all(sql, params=None):
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, params or (), prepare=False)
        return cur.fetchall()

def fetch_one(sql, params=None):
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, params or (), prepare=False)
        return cur.fetchone()

# ---------- Comments with author info (optional) ----------
def fetch_ticket_comments_with_authors(ticket_id: int):
    """
    Fetch comments for a ticket, enriched with author name/email and photo JSON.
    """
    rows = fetch_all(
        """
        SELECT
            c.id,
            c.ticket_id,
            c.author_id,
            c.public,
            c.body,
            c.created_at,
            c.updated_at,
            u.name  AS author_name,
            u.email AS author_email,
            u.photo_json AS author_photo_json
        FROM ticket_comments c
        LEFT JOIN users u ON u.id = c.author_id
        WHERE c.ticket_id = %s
        ORDER BY c.created_at ASC
        """,
        (ticket_id,),
    )

    # Normalize JSON for photo in case it comes back as text in some drivers
    for r in rows:
        pj = r.get("author_photo_json")
        if isinstance(pj, str):
            try:
                pj = json.loads(pj)
            except Exception:
                pj = None
        r["author_photo_json"] = pj if isinstance(pj, dict) else None

        r["author_display"] = (
            r.get("author_name")
            or r.get("author_email")
            or (f"User #{r.get('author_id')}" if r.get("author_id") else "Unknown")
        )
    return rows
