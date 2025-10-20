#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Restore structured tables from `raw_snapshots` (idempotent UPSERTs) in Postgres.

Restores:
  - users
  - organizations
  - tickets
  - views
  - triggers
  - trigger_categories
  - macros

Notes:
  - We skip ticket_comments & attachments because raw payloads often lack ticket_id linkage.
Usage examples:
  python restore.py --scope all
  python restore.py --scope users,tickets
  python restore.py --scope views --truncate-first
  python restore.py --dry-run
"""

import os
import json
import argparse
import logging
import datetime as dt
from typing import Dict, Any, Optional, List

import psycopg
from psycopg.rows import dict_row
from urllib.parse import urlparse
from dotenv import load_dotenv, find_dotenv
from pathlib import Path

# ------------- robust .env load -------------
_env_path = find_dotenv(usecwd=True)
if not _env_path:
    p = Path(__file__).resolve().parent / ".env"
    if p.exists():
        _env_path = str(p)
load_dotenv(dotenv_path=_env_path or ".env", override=False)

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")

PG_DSN = os.getenv("DATABASE_URL")
assert PG_DSN, "DATABASE_URL missing in .env"
assert urlparse(PG_DSN).hostname, f"Bad DATABASE_URL: {PG_DSN!r}"

# ----------------------------
# DB helpers
# ----------------------------
def get_db():
    """
    Disable server-side prepared statements (prepare_threshold=None)
    and use autocommit so one failing row doesn't abort the whole run.
    """
    conn = psycopg.connect(PG_DSN, row_factory=dict_row, prepare_threshold=None)
    conn.autocommit = True
    return conn

def parse_dt(s: Optional[str]) -> Optional[str]:
    if not s:
        return None
    try:
        return dt.datetime.fromisoformat(str(s).replace("Z", "+00:00")).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return None

# ---------- raw upsert ----------
def upsert_raw(conn, resource: str, entity_id: int, updated_at: Optional[str], payload: Dict[str, Any]):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO raw_snapshots (resource, entity_id, updated_at, payload_json)
            VALUES (%s, %s, %s, %s::jsonb)
            ON CONFLICT (resource, entity_id) DO UPDATE SET
              updated_at  = EXCLUDED.updated_at,
              payload_json = EXCLUDED.payload_json
        """, (resource, entity_id, parse_dt(updated_at), json.dumps(payload, ensure_ascii=False)), prepare=False)

# ---------- Structured UPSERTs ----------
def upsert_user(conn, u: Dict[str, Any]):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO users (id, name, email, role, role_type, active, suspended, organization_id, phone, locale, time_zone,
                               created_at, updated_at, last_login_at, tags_json, user_fields_json, photo_json)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (id) DO UPDATE SET
              name=EXCLUDED.name, email=EXCLUDED.email, role=EXCLUDED.role, role_type=EXCLUDED.role_type,
              active=EXCLUDED.active, suspended=EXCLUDED.suspended, organization_id=EXCLUDED.organization_id,
              phone=EXCLUDED.phone, locale=EXCLUDED.locale, time_zone=EXCLUDED.time_zone,
              created_at=EXCLUDED.created_at, updated_at=EXCLUDED.updated_at, last_login_at=EXCLUDED.last_login_at,
              tags_json=EXCLUDED.tags_json, user_fields_json=EXCLUDED.user_fields_json, photo_json=EXCLUDED.photo_json
        """, (
            u.get("id"), u.get("name"), u.get("email"), u.get("role"), u.get("role_type"),
            bool(u.get("active")), bool(u.get("suspended")), u.get("organization_id"),
            u.get("phone"), u.get("locale"), u.get("time_zone"),
            parse_dt(u.get("created_at")), parse_dt(u.get("updated_at")), parse_dt(u.get("last_login_at")),
            json.dumps(u.get("tags") or []), json.dumps(u.get("user_fields") or {}), json.dumps(u.get("photo") or {})
        ), prepare=False)
    upsert_raw(conn, "users", int(u["id"]), u.get("updated_at"), u)

def upsert_org(conn, o: Dict[str, Any]):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO organizations (id, name, external_id, group_id, details, notes, shared_tickets, shared_comments,
                                       domain_names_json, tags_json, organization_fields_json, created_at, updated_at)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (id) DO UPDATE SET
              name=EXCLUDED.name, external_id=EXCLUDED.external_id, group_id=EXCLUDED.group_id,
              details=EXCLUDED.details, notes=EXCLUDED.notes, shared_tickets=EXCLUDED.shared_tickets,
              shared_comments=EXCLUDED.shared_comments, domain_names_json=EXCLUDED.domain_names_json,
              tags_json=EXCLUDED.tags_json, organization_fields_json=EXCLUDED.organization_fields_json,
              created_at=EXCLUDED.created_at, updated_at=EXCLUDED.updated_at
        """, (
            o.get("id"), o.get("name"), o.get("external_id"), o.get("group_id"), o.get("details"), o.get("notes"),
            bool(o.get("shared_tickets")), bool(o.get("shared_comments")),
            json.dumps(o.get("domain_names") or []), json.dumps(o.get("tags") or []),
            json.dumps(o.get("organization_fields") or {}), parse_dt(o.get("created_at")), parse_dt(o.get("updated_at"))
        ), prepare=False)
    upsert_raw(conn, "organizations", int(o["id"]), o.get("updated_at"), o)

def upsert_ticket(conn, t: Dict[str, Any]):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO tickets (id, subject, description, status, priority, type, requester_id, assignee_id, organization_id,
                                 created_at, updated_at, due_at)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (id) DO UPDATE SET
              subject=EXCLUDED.subject, description=EXCLUDED.description, status=EXCLUDED.status,
              priority=EXCLUDED.priority, type=EXCLUDED.type, requester_id=EXCLUDED.requester_id,
              assignee_id=EXCLUDED.assignee_id, organization_id=EXCLUDED.organization_id,
              created_at=EXCLUDED.created_at, updated_at=EXCLUDED.updated_at, due_at=EXCLUDED.due_at
        """, (
            t.get("id"), t.get("subject"), t.get("description"), t.get("status"), t.get("priority"), t.get("type"),
            t.get("requester_id"), t.get("assignee_id"), t.get("organization_id"),
            parse_dt(t.get("created_at")), parse_dt(t.get("updated_at")), parse_dt(t.get("due_at"))
        ), prepare=False)
    upsert_raw(conn, "tickets", int(t["id"]), t.get("updated_at"), t)

def upsert_view(conn, v: Dict[str, Any]):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO views (id, title, description, active, position, default_view, restriction_json, execution_json, conditions_json,
                               created_at, updated_at)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (id) DO UPDATE SET
               title=EXCLUDED.title, description=EXCLUDED.description, active=EXCLUDED.active, position=EXCLUDED.position,
               default_view=EXCLUDED.default_view, restriction_json=EXCLUDED.restriction_json,
               execution_json=EXCLUDED.execution_json, conditions_json=EXCLUDED.conditions_json,
               created_at=EXCLUDED.created_at, updated_at=EXCLUDED.updated_at
        """, (
            v.get("id"), v.get("title"), v.get("description"), bool(v.get("active")), v.get("position"),
            bool(v.get("default")), json.dumps(v.get("restriction")), json.dumps(v.get("execution")),
            json.dumps(v.get("conditions")), parse_dt(v.get("created_at")), parse_dt(v.get("updated_at"))
        ), prepare=False)
    upsert_raw(conn, "views", int(v["id"]), v.get("updated_at"), v)

def upsert_trigger(conn, t: Dict[str, Any]):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO triggers (id, title, description, active, position, category_id, raw_title, default_trigger,
                                  conditions_json, actions_json, created_at, updated_at)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (id) DO UPDATE SET
               title=EXCLUDED.title, description=EXCLUDED.description, active=EXCLUDED.active, position=EXCLUDED.position,
               category_id=EXCLUDED.category_id, raw_title=EXCLUDED.raw_title, default_trigger=EXCLUDED.default_trigger,
               conditions_json=EXCLUDED.conditions_json, actions_json=EXCLUDED.actions_json,
               created_at=EXCLUDED.created_at, updated_at=EXCLUDED.updated_at
        """, (
            t.get("id"), t.get("title"), t.get("description"), bool(t.get("active")), t.get("position"),
            t.get("category_id"), t.get("raw_title"), bool(t.get("default")),
            json.dumps(t.get("conditions") or {}), json.dumps(t.get("actions") or []),
            parse_dt(t.get("created_at")), parse_dt(t.get("updated_at"))
        ), prepare=False)
    upsert_raw(conn, "triggers", int(t["id"]), t.get("updated_at"), t)

def upsert_trigger_category(conn, c: Dict[str, Any]):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO trigger_categories (id, name, position, created_at, updated_at)
            VALUES (%s,%s,%s,%s,%s)
            ON CONFLICT (id) DO UPDATE SET
               name=EXCLUDED.name, position=EXCLUDED.position, created_at=EXCLUDED.created_at, updated_at=EXCLUDED.updated_at
        """, (str(c.get("id")), c.get("name"), c.get("position"),
              parse_dt(c.get("created_at")), parse_dt(c.get("updated_at"))), prepare=False)
    upsert_raw(conn, "trigger_categories", int(c["id"]), c.get("updated_at"), c)

def upsert_macro(conn, m: Dict[str, Any]):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO macros (id, title, description, active, position, default_macro, restriction_json, actions_json, created_at, updated_at)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (id) DO UPDATE SET
               title=EXCLUDED.title, description=EXCLUDED.description, active=EXCLUDED.active, position=EXCLUDED.position,
               default_macro=EXCLUDED.default_macro, restriction_json=EXCLUDED.restriction_json,
               actions_json=EXCLUDED.actions_json, created_at=EXCLUDED.created_at, updated_at=EXCLUDED.updated_at
        """, (
            m.get("id"), m.get("title"), m.get("description"), bool(m.get("active")), m.get("position"),
            bool(m.get("default")), json.dumps(m.get("restriction")), json.dumps(m.get("actions") or []),
            parse_dt(m.get("created_at")), parse_dt(m.get("updated_at"))
        ), prepare=False)
    upsert_raw(conn, "macros", int(m["id"]), m.get("updated_at"), m)

# ---------- Driver ----------
RESTORERS = {
    "users": upsert_user,
    "organizations": upsert_org,
    "tickets": upsert_ticket,
    "views": upsert_view,
    "triggers": upsert_trigger,
    "trigger_categories": upsert_trigger_category,
    "macros": upsert_macro,
}
RESTORE_ORDER = list(RESTORERS.keys())

def load_raw_rows(conn, resource: str, limit: Optional[int] = None, offset: int = 0) -> List[Dict[str, Any]]:
    with conn.cursor() as cur:
        sql = """
            SELECT entity_id, payload_json
            FROM raw_snapshots
            WHERE resource = %s
            ORDER BY updated_at DESC NULLS LAST, entity_id DESC
        """
        params = [resource]
        if limit is not None:
            sql += " LIMIT %s OFFSET %s"
            params.extend([limit, offset])
        cur.execute(sql, params, prepare=False)
        return cur.fetchall()

def truncate_table(conn, table: str):
    with conn.cursor() as cur:
        cur.execute(f"TRUNCATE TABLE {table} RESTART IDENTITY CASCADE", prepare=False)

def maybe_truncate(conn, scope_list, truncate_first: bool):
    if not truncate_first: 
        return
    table_map = {
        "users": "users",
        "organizations": "organizations",
        "tickets": "tickets",
        "views": "views",
        "triggers": "triggers",
        "trigger_categories": "trigger_categories",
        "macros": "macros",
    }
    logging.warning("Truncating tables before restore: %s",
                    ", ".join(table_map[s] for s in scope_list if s in table_map))
    for s in scope_list:
        t = table_map.get(s)
        if t:
            truncate_table(conn, t)

def restore(scope: str, limit: Optional[int], offset: int, truncate_first: bool, dry_run: bool):
    with get_db() as conn:
        scope_list = RESTORE_ORDER if scope == "all" else [
            p.strip().lower() for p in scope.split(",") if p.strip().lower() in RESTORERS
        ]
        if not scope_list:
            raise SystemExit(f"Empty or invalid scope: use comma list of {', '.join(RESTORERS)} or 'all'.")

        maybe_truncate(conn, scope_list, truncate_first)

        for resource in scope_list:
            fn = RESTORERS[resource]
            restored = 0
            seen_ids = set()
            logging.info("Restoring resource: %s", resource)

            rows = load_raw_rows(conn, resource, limit=limit, offset=offset)
            for row in rows:
                entity_id = int(row["entity_id"])
                if entity_id in seen_ids:
                    continue

                payload = row["payload_json"]
                if isinstance(payload, (bytes, bytearray)):
                    payload = payload.decode("utf-8", errors="ignore")
                if isinstance(payload, str):
                    try:
                        payload = json.loads(payload)
                    except Exception as e:
                        logging.error("Failed parsing JSON for %s #%s: %s", resource, entity_id, e)
                        continue

                if dry_run:
                    restored += 1
                    seen_ids.add(entity_id)
                    continue

                try:
                    fn(conn, payload)
                    restored += 1
                    seen_ids.add(entity_id)
                except Exception as e:
                    logging.error("Failed restoring %s #%s: %s", resource, entity_id, e)

            logging.info("✔ %s restored: %d", resource, restored)

        if "tickets" in scope_list and not dry_run:
            logging.warning(
                "Comments & attachments were SKIPPED due to missing ticket_id in raw payloads."
            )

# ---------- CLI ----------
def main():
    ap = argparse.ArgumentParser(description="Restore structured tables from raw_snapshots.")
    ap.add_argument("--scope", default="all",
                    help=f"Comma-separated resources or 'all'. Options: {', '.join(RESTORERS.keys())}")
    ap.add_argument("--limit", type=int, default=None, help="Limit rows per resource.")
    ap.add_argument("--offset", type=int, default=0, help="Offset per resource.")
    ap.add_argument("--truncate-first", action="store_true", help="TRUNCATE target tables before restore.")
    ap.add_argument("--dry-run", action="store_true", help="Do not write, just count.")
    args = ap.parse_args()

    logging.info("Starting restore | scope=%s, limit=%s, offset=%s, truncate=%s, dry_run=%s",
                 args.scope, args.limit, args.offset, args.truncate_first, args.dry_run)
    restore(args.scope, args.limit, args.offset, args.truncate_first, args.dry_run)
    logging.info("✅ Restore finished.")

if __name__ == "__main__":
    main()
