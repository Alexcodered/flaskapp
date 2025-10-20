#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Zendesk → Postgres (Supabase) incremental backup

- Users (cursor)
- Organizations (incremental, resilient)
- Tickets (cursor) + per-ticket Comments + Attachments
- Views, Triggers, Trigger Categories, Macros (snapshot lists)

Differences vs old MySQL version:
- Uses psycopg3
- Postgres DDL (JSONB, BOOLEAN)
- UPSERT via ON CONFLICT ... DO UPDATE
"""

import os, time, json, logging, re
import datetime as dt
from pathlib import Path
from typing import Dict, Any, Iterable, Optional, Union

import requests
import psycopg
from psycopg.rows import dict_row
from urllib.parse import urlparse, parse_qs
from dotenv import load_dotenv, find_dotenv

# ----------------------------
# Robust .env load (works no matter where you run from)
# ----------------------------
_env_path = find_dotenv(usecwd=True)
if not _env_path:
    p = Path(__file__).resolve().parent / ".env"
    if p.exists():
        _env_path = str(p)
load_dotenv(dotenv_path=_env_path or ".env", override=False)

# ----------------------------
# Config (from .env)
# ----------------------------
Z_SUBDOMAIN = os.getenv("ZENDESK_SUBDOMAIN")
Z_EMAIL = os.getenv("ZENDESK_EMAIL")
Z_TOKEN = os.getenv("ZENDESK_API_TOKEN")
assert Z_SUBDOMAIN and Z_EMAIL and Z_TOKEN, "Zendesk credentials missing in .env"

PG_DSN = os.getenv("DATABASE_URL")
assert PG_DSN, "DATABASE_URL missing in .env (Supabase connection string)."
# Basic sanity on DSN
_u = urlparse(PG_DSN)
assert _u.hostname and _u.scheme.startswith("postgres"), f"Malformed DATABASE_URL: {PG_DSN!r}"

ATTACHMENTS_DIR = os.getenv("ATTACHMENTS_DIR", "./attachments")
PER_PAGE = int(os.getenv("ZENDESK_PER_PAGE", "500"))
INCLUDE = os.getenv("ZENDESK_INCLUDE", "")
EXCLUDE_DELETED = os.getenv("ZENDESK_EXCLUDE_DELETED", "false").lower() == "true"
BOOTSTRAP_HOURS = int(os.getenv("ZENDESK_BOOTSTRAP_START_HOURS", "24"))

# Tickets/Comments toggles
CLOSED_TICKETS_ONLY = os.getenv("CLOSED_TICKETS_ONLY", "false").lower() == "true"
USE_TICKET_EVENTS_FOR_COMMENTS = os.getenv("USE_TICKET_EVENTS_FOR_COMMENTS", "false").lower() == "true"
PRUNE_REOPENED_FROM_DB = os.getenv("PRUNE_REOPENED_FROM_DB", "false").lower() == "true"

# Orgs throttling
ORG_PER_PAGE = int(os.getenv("ORG_PER_PAGE", "100"))
ORG_PAGE_DELAY_SECS = float(os.getenv("ORG_PAGE_DELAY_SECS", "0.2"))
ORG_RETRY_CAP_SECS = float(os.getenv("ORG_RETRY_CAP_SECS", "4"))

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")

SESSION = requests.Session()
SESSION.auth = (f"{Z_EMAIL}/token", Z_TOKEN)
SESSION.headers.update({
    "Accept": "application/json",
    "User-Agent": "zendesk-backup/pg-1.0 (+python-requests)"
})
RETRY_STATUS = {429, 500, 502, 503, 504}
Z_BASE = f"https://{Z_SUBDOMAIN}.zendesk.com"

# ----------------------------
# Helpers
# ----------------------------
def ensure_dir(p: str) -> None:
    Path(p).mkdir(parents=True, exist_ok=True)

def safe_filename(name: str) -> str:
    name = re.sub(r"[\\/:*?\"<>|]+", "_", name or "")
    return name[:180] if len(name) > 180 else name

def initial_start_time_epoch() -> int:
    return int(time.time()) - max(BOOTSTRAP_HOURS * 3600, 120)

def parse_dt(s: Optional[str]) -> Optional[str]:
    if not s:
        return None
    try:
        return dt.datetime.fromisoformat(str(s).replace("Z", "+00:00")).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return None

def get_with_retry(url: str, params: Dict[str, Any] = None, stream: bool = False) -> Union[requests.Response, Dict[str, Any]]:
    backoff = 1.0
    for _ in range(8):
        resp = SESSION.get(url, params=params, timeout=120, stream=stream)
        if resp.status_code == 200:
            return resp if stream else resp.json()
        if resp.status_code in RETRY_STATUS:
            retry_after = resp.headers.get("Retry-After")
            sleep_for = float(retry_after) if retry_after else backoff
            logging.warning("Retryable %s for %s. Sleeping %.1fs", resp.status_code, url, sleep_for)
            time.sleep(sleep_for)
            backoff = min(backoff * 2, 30.0)
            continue
        try:
            detail = resp.json()
        except Exception:
            detail = {"text": resp.text[:300]}
        raise RuntimeError(f"GET {url} failed [{resp.status_code}]: {detail}")
    raise RuntimeError(f"GET {url} exhausted retries")

def iter_list_pages(url: str, params: Dict[str, Any] = None) -> Iterable[Dict[str, Any]]:
    page = get_with_retry(url, params=params or {})
    while True:
        yield page
        next_url = page.get("next_page") or page.get("links", {}).get("next")
        if not next_url:
            break
        page = get_with_retry(next_url)

def iter_cursor_page(first_page_json: Dict[str, Any]) -> Iterable[Dict[str, Any]]:
    page = first_page_json
    while True:
        yield page
        after_url = page.get("after_url") or page.get("links", {}).get("next")
        if not after_url:
            break
        page = get_with_retry(after_url)

# ----------------------------
# Postgres: connection + schema
# ----------------------------
def get_db():
    # row_factory=dict_row gives dict rows
    return psycopg.connect(PG_DSN, row_factory=dict_row)

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS sync_state (
  resource TEXT PRIMARY KEY,
  cursor_token TEXT NOT NULL,
  updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS raw_snapshots (
  resource TEXT NOT NULL,
  entity_id BIGINT NOT NULL,
  updated_at TIMESTAMP NULL,
  payload_json JSONB NOT NULL,
  PRIMARY KEY (resource, entity_id)
);

CREATE TABLE IF NOT EXISTS users (
  id BIGINT PRIMARY KEY,
  name TEXT,
  email TEXT,
  role TEXT,
  role_type INT,
  active BOOLEAN,
  suspended BOOLEAN,
  organization_id BIGINT,
  phone TEXT,
  locale TEXT,
  time_zone TEXT,
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  last_login_at TIMESTAMP,
  tags_json JSONB,
  user_fields_json JSONB,
  photo_json JSONB
);

CREATE TABLE IF NOT EXISTS organizations (
  id BIGINT PRIMARY KEY,
  name TEXT,
  external_id TEXT,
  group_id BIGINT,
  details TEXT,
  notes TEXT,
  shared_tickets BOOLEAN,
  shared_comments BOOLEAN,
  domain_names_json JSONB,
  tags_json JSONB,
  organization_fields_json JSONB,
  created_at TIMESTAMP,
  updated_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS tickets (
  id BIGINT PRIMARY KEY,
  subject TEXT,
  description TEXT,
  status TEXT,
  priority TEXT,
  type TEXT,
  requester_id BIGINT,
  assignee_id BIGINT,
  organization_id BIGINT,
  created_at TIMESTAMP,
  updated_at TIMESTAMP,
  due_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS ticket_comments (
  id BIGINT PRIMARY KEY,
  ticket_id BIGINT,
  author_id BIGINT,
  public BOOLEAN,
  body TEXT,
  created_at TIMESTAMP,
  updated_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS attachments (
  id BIGINT PRIMARY KEY,
  ticket_id BIGINT,
  comment_id BIGINT,
  file_name TEXT,
  content_url TEXT,
  local_path TEXT,
  content_type TEXT,
  size BIGINT,
  thumbnails_json JSONB,
  created_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS views (
  id BIGINT PRIMARY KEY,
  title TEXT,
  description TEXT,
  active BOOLEAN,
  position INT,
  default_view BOOLEAN,
  restriction_json JSONB,
  execution_json JSONB,
  conditions_json JSONB,
  created_at TIMESTAMP,
  updated_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS triggers (
  id BIGINT PRIMARY KEY,
  title TEXT,
  description TEXT,
  active BOOLEAN,
  position INT,
  category_id TEXT,
  raw_title TEXT,
  default_trigger BOOLEAN,
  conditions_json JSONB,
  actions_json JSONB,
  created_at TIMESTAMP,
  updated_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS trigger_categories (
  id TEXT PRIMARY KEY,
  name TEXT,
  position INT,
  created_at TIMESTAMP,
  updated_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS macros (
  id BIGINT PRIMARY KEY,
  title TEXT,
  description TEXT,
  active BOOLEAN,
  position INT,
  default_macro BOOLEAN,
  restriction_json JSONB,
  actions_json JSONB,
  created_at TIMESTAMP,
  updated_at TIMESTAMP
);
"""

def init_schema():
    with get_db() as conn, conn.cursor() as cur:
        cur.execute("BEGIN")
        cur.execute(SCHEMA_SQL)
        conn.commit()

def get_cursor_val(conn, resource: str) -> Optional[str]:
    with conn.cursor() as cur:
        cur.execute("SELECT cursor_token FROM sync_state WHERE resource=%s", (resource,))
        row = cur.fetchone()
        return row["cursor_token"] if row else None

def set_cursor_val(conn, resource: str, token: str):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO sync_state (resource, cursor_token)
            VALUES (%s, %s)
            ON CONFLICT (resource) DO UPDATE SET
              cursor_token = EXCLUDED.cursor_token,
              updated_at = NOW()
        """, (resource, token))
        conn.commit()

def upsert_raw(conn, resource: str, entity_id: int, updated_at: Optional[str], payload: Dict[str, Any]):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO raw_snapshots (resource, entity_id, updated_at, payload_json)
            VALUES (%s, %s, %s, %s::jsonb)
            ON CONFLICT (resource, entity_id) DO UPDATE SET
              updated_at = EXCLUDED.updated_at,
              payload_json = EXCLUDED.payload_json
        """, (resource, entity_id, parse_dt(updated_at), json.dumps(payload, ensure_ascii=False)))
        conn.commit()

# ----------------------------
# UPSERT helpers (Postgres ON CONFLICT)
# ----------------------------
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
        ))
        conn.commit()
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
        ))
        conn.commit()
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
        ))
        conn.commit()
    upsert_raw(conn, "tickets", int(t["id"]), t.get("updated_at"), t)

def delete_ticket(conn, ticket_id: int):
    with conn.cursor() as cur:
        cur.execute("DELETE FROM attachments WHERE ticket_id=%s", (ticket_id,))
        cur.execute("DELETE FROM ticket_comments WHERE ticket_id=%s", (ticket_id,))
        cur.execute("DELETE FROM tickets WHERE id=%s", (ticket_id,))
        conn.commit()

def upsert_comment(conn, ticket_id: int, c: Dict[str, Any]):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO ticket_comments (id, ticket_id, author_id, public, body, created_at, updated_at)
            VALUES (%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (id) DO UPDATE SET
              ticket_id=EXCLUDED.ticket_id, author_id=EXCLUDED.author_id, public=EXCLUDED.public,
              body=EXCLUDED.body, created_at=EXCLUDED.created_at, updated_at=EXCLUDED.updated_at
        """, (
            c.get("id"), ticket_id, c.get("author_id"), bool(c.get("public")),
            c.get("body"), parse_dt(c.get("created_at")), parse_dt(c.get("updated_at") or c.get("created_at"))
        ))
        conn.commit()
    upsert_raw(conn, "comments", int(c["id"]), c.get("updated_at") or c.get("created_at"), c)

def upsert_attachment(conn, ticket_id: int, comment_id: Optional[int], a: Dict[str, Any], local_path: Optional[str] = None):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO attachments (id, ticket_id, comment_id, file_name, content_url, local_path, content_type, size, thumbnails_json, created_at)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (id) DO UPDATE SET
              ticket_id=EXCLUDED.ticket_id, comment_id=EXCLUDED.comment_id, file_name=EXCLUDED.file_name,
              content_url=EXCLUDED.content_url, local_path=EXCLUDED.local_path, content_type=EXCLUDED.content_type,
              size=EXCLUDED.size, thumbnails_json=EXCLUDED.thumbnails_json, created_at=EXCLUDED.created_at
        """, (
            a.get("id"), ticket_id, comment_id, a.get("file_name"), a.get("content_url"),
            local_path, a.get("content_type"), a.get("size"), json.dumps(a.get("thumbnails") or []),
            parse_dt(a.get("created_at"))
        ))
        conn.commit()
    upsert_raw(conn, "attachments", int(a["id"]), a.get("created_at"), a)

# ----------------------------
# Downloads
# ----------------------------
def download_attachment(ticket_id: int, comment_id: Optional[int], att: Dict[str, Any]) -> Optional[str]:
    if not os.getenv("DOWNLOAD_ATTACHMENTS", "true").lower() == "true":
        return None
    url = att.get("content_url")
    if not url:
        return None
    base_dir = Path(ATTACHMENTS_DIR) / str(ticket_id)
    ensure_dir(str(base_dir))
    rid = str(att.get("id"))
    fname = safe_filename(att.get("file_name") or f"attachment_{rid}")
    target = base_dir / f"{rid}__{fname}"
    if target.exists() and target.stat().st_size > 0:
        return str(target.resolve())
    try:
        resp = get_with_retry(url, stream=True)
        with open(target, "wb") as f:
            for chunk in resp.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)
        return str(target.resolve())
    except Exception as e:
        logging.warning("Attachment download failed (ticket %s, att %s): %s", ticket_id, att.get("id"), e)
        return None

# ----------------------------
# Sync functions
# ----------------------------
def sync_users(conn):
    url = f"{Z_BASE}/api/v2/incremental/users/cursor.json"
    last = get_cursor_val(conn, "users")
    params = {"per_page": PER_PAGE}
    if last:
        params["cursor"] = last
    else:
        params["start_time"] = initial_start_time_epoch()

    first = get_with_retry(url, params=params)
    for page in iter_cursor_page(first):
        for u in page.get("users", []):
            upsert_user(conn, u)
        ac = page.get("after_cursor")
        if ac:
            set_cursor_val(conn, "users", ac)
        if page.get("end_of_stream"):
            break
    logging.info("Users: sync complete.")

def sync_organizations(conn):
    import random
    force_snapshot = os.getenv("FORCE_ORG_SNAPSHOT", "false").lower() == "true"
    per_page = min(max(ORG_PER_PAGE, 25), 100)
    page_delay = max(ORG_PAGE_DELAY_SECS, 0.0)
    retry_cap  = max(ORG_RETRY_CAP_SECS, 0.5)

    def set_now_cursor():
        set_cursor_val(conn, "organizations", str(int(time.time()) - 60))

    def snapshot_once():
        url = f"{Z_BASE}/api/v2/organizations.json"
        params = {"per_page": per_page}
        seen = 0
        logging.info("Organizations: snapshot mode (per_page=%s)", per_page)
        page = get_with_retry(url, params=params)
        while True:
            for o in page.get("organizations", []):
                upsert_org(conn, o); seen += 1
            nxt = page.get("next_page") or page.get("links", {}).get("next")
            if not nxt: break
            page = get_with_retry(nxt)
        set_now_cursor()
        logging.info("Organizations: snapshot complete (rows=%d).", seen)

    last_epoch = get_cursor_val(conn, "organizations")
    if force_snapshot or not last_epoch:
        snapshot_once(); return

    url = f"{Z_BASE}/api/v2/incremental/organizations.json"
    params = {"per_page": per_page, "start_time": int(last_epoch)}
    processed = 0; pages_seen = 0; seen_ids = set(); dup_only_streak = 0
    logging.info("Organizations: incremental start")

    while True:
        try:
            resp = SESSION.get(url, params=params, headers={"Accept": "application/json"}, timeout=45)
        except requests.RequestException as e:
            logging.warning("Organizations: network error %s — retrying", e)
            time.sleep(min(1.0, retry_cap)); continue

        if resp.status_code == 200:
            page = resp.json()
            orgs = page.get("organizations", [])
            before = len(seen_ids)
            for o in orgs:
                seen_ids.add(int(o.get("id"))); upsert_org(conn, o)
            added = len(seen_ids) - before
            processed += len(orgs); pages_seen += 1
            end_time = page.get("end_time")
            if end_time: set_cursor_val(conn, "organizations", str(end_time))
            if len(orgs) > 0 and added == 0:
                dup_only_streak += 1
            else:
                dup_only_streak = 0
            if dup_only_streak >= 3:
                logging.info("Organizations: duplicate-only → snapshot")
                snapshot_once(); return
            next_page = page.get("next_page")
            if not next_page: break
            if page_delay > 0:
                time.sleep(page_delay + random.uniform(0, min(0.25, page_delay)))
            url = next_page; params = {}; continue

        if resp.status_code == 429:
            ra = resp.headers.get("Retry-After")
            suggested = float(ra) if ra else page_delay * 2 or 1.0
            time.sleep(min(max(0.5, suggested), retry_cap)); continue

        if resp.status_code in RETRY_STATUS:
            time.sleep(min(retry_cap, 1.0)); continue

        try:
            detail = resp.json()
        except Exception:
            detail = {"text": resp.text[:300]}
        raise RuntimeError(f"Organizations GET failed [{resp.status_code}]: {detail}")

    logging.info("Organizations: incremental complete (rows=%d, pages=%d).", processed, pages_seen)

def ticket_initial_params(last_cursor: Optional[str]) -> Dict[str, Any]:
    p = {"per_page": PER_PAGE}
    if INCLUDE: p["include"] = INCLUDE
    if EXCLUDE_DELETED: p["exclude_deleted"] = "true"
    if last_cursor: p["cursor"] = last_cursor
    else: p["start_time"] = initial_start_time_epoch()
    return p

def sync_tickets_comments_attachments(conn):
    import random
    url = f"{Z_BASE}/api/v2/incremental/tickets/cursor.json"
    last = get_cursor_val(conn, "tickets")
    per_page = int(os.getenv("ZENDESK_PER_PAGE", "100"))
    page_delay = float(os.getenv("TICKETS_PAGE_DELAY_SECS", "0.2"))
    retry_cap  = float(os.getenv("TICKETS_RETRY_CAP_SECS", "6"))
    params = ticket_initial_params(last); params["per_page"] = per_page

    processed = 0; pages = 0
    logging.info("Tickets: start")
    while True:
        try:
            resp = SESSION.get(url, params=params, timeout=90)
        except requests.RequestException as e:
            logging.warning("Tickets: network error %s — brief wait", e)
            time.sleep(min(1.0, retry_cap)); continue

        if resp.status_code == 200:
            page = resp.json(); tickets = page.get("tickets", []); pages += 1
            for t in tickets:
                status = (t.get("status") or "").lower()
                is_closed = (status == "closed")
                if CLOSED_TICKETS_ONLY and not is_closed:
                    if PRUNE_REOPENED_FROM_DB: delete_ticket(conn, int(t["id"]))
                    continue
                upsert_ticket(conn, t)
                if not USE_TICKET_EVENTS_FOR_COMMENTS:
                    comments_url = f"{Z_BASE}/api/v2/tickets/{t['id']}/comments.json"
                    for cpage in iter_list_pages(comments_url, params={"per_page": 100}):
                        for c in cpage.get("comments", []):
                            upsert_comment(conn, int(t["id"]), c)
                            for a in (c.get("attachments") or []):
                                local_path = download_attachment(int(t["id"]), int(c.get("id") or 0), a)
                                upsert_attachment(conn, int(t["id"]), int(c.get("id") or 0), a, local_path)
            processed += len(tickets)
            ac = page.get("after_cursor")
            if ac: set_cursor_val(conn, "tickets", ac)
            if page.get("end_of_stream"): break
            if page_delay > 0:
                time.sleep(page_delay + random.uniform(0, min(0.25, page_delay)))
            url = page.get("after_url") or page.get("links", {}).get("next") or url
            params = {}; continue

        if resp.status_code == 429:
            ra = resp.headers.get("Retry-After")
            suggested = float(ra) if ra else (page_delay * 2 or 1.0)
            time.sleep(min(max(0.5, suggested), retry_cap)); continue

        if resp.status_code in RETRY_STATUS:
            time.sleep(min(1.0, retry_cap)); continue

        try:
            detail = resp.json()
        except Exception:
            detail = {"text": resp.text[:300]}
        raise RuntimeError(f"Tickets GET failed [{resp.status_code}]: {detail}")
    logging.info("Tickets (+comments:%s, attachments:%s): sync complete.",
                 "events" if USE_TICKET_EVENTS_FOR_COMMENTS else "per-ticket",
                 os.getenv("DOWNLOAD_ATTACHMENTS", "true").lower() == "true")

def sync_ticket_events_for_comments(conn):
    url = f"{Z_BASE}/api/v2/incremental/ticket_events.json"
    last_epoch = get_cursor_val(conn, "ticket_events")
    params = {"start_time": int(last_epoch) if last_epoch else initial_start_time_epoch(),
              "per_page": PER_PAGE, "include": "comment_events"}
    while True:
        page = get_with_retry(url, params=params)
        for ev in page.get("ticket_events", []):
            tid = int(ev.get("ticket_id"))
            if CLOSED_TICKETS_ONLY:
                t = get_with_retry(f"{Z_BASE}/api/v2/tickets/{tid}.json").get("ticket", {})
                if (t.get("status") or "").lower() != "closed": continue
            for ce in (ev.get("child_events") or []):
                if (ce.get("event_type") or ce.get("type")) == "Comment":
                    c = {
                        "id": ce["id"], "author_id": ce.get("author_id"),
                        "public": ce.get("public", False), "body": ce.get("body"),
                        "created_at": ce.get("created_at"), "updated_at": ce.get("created_at"),
                        "attachments": ce.get("attachments") or []
                    }
                    upsert_comment(conn, tid, c)
                    for a in (ce.get("attachments") or []):
                        local_path = download_attachment(tid, int(ce["id"]), a)
                        upsert_attachment(conn, tid, int(ce["id"]), a, local_path)
        next_page = page.get("next_page")
        if next_page:
            url = next_page; params = {}
        else:
            end_time = page.get("end_time")
            if end_time: set_cursor_val(conn, "ticket_events", str(end_time))
            break
    logging.info("Ticket events (comments): sync complete.")

def sync_views(conn):
    url = f"{Z_BASE}/api/v2/views.json"
    params = {"per_page": min(PER_PAGE, 100)}
    if INCLUDE: params["include"] = INCLUDE
    page = get_with_retry(url, params=params)
    while True:
        for v in page.get("views", []): upsert_view(conn, v)
        next_page = page.get("next_page") or page.get("links", {}).get("next")
        if not next_page: break
        page = get_with_retry(next_page)
    logging.info("Views: sync complete.")

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
        ))
        conn.commit()
    upsert_raw(conn, "views", int(v["id"]), v.get("updated_at"), v)

def sync_triggers(conn):
    url = f"{Z_BASE}/api/v2/triggers.json"
    params = {"per_page": min(PER_PAGE, 100)}
    page = get_with_retry(url, params=params)
    while True:
        for t in page.get("triggers", []): upsert_trigger(conn, t)
        next_page = page.get("next_page") or page.get("links", {}).get("next")
        if not next_page: break
        page = get_with_retry(next_page)
    logging.info("Triggers: sync complete.")

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
        ))
        conn.commit()
    upsert_raw(conn, "triggers", int(t["id"]), t.get("updated_at"), t)

def sync_trigger_categories(conn):
    url = f"{Z_BASE}/api/v2/trigger_categories.json"
    params = {"per_page": min(PER_PAGE, 100)}
    page = get_with_retry(url, params=params)
    while True:
        for c in page.get("trigger_categories", []): upsert_trigger_category(conn, c)
        next_page = page.get("next_page") or page.get("links", {}).get("next")
        if not next_page: break
        page = get_with_retry(next_page)
    logging.info("Trigger categories: sync complete.")

def upsert_trigger_category(conn, c: Dict[str, Any]):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO trigger_categories (id, name, position, created_at, updated_at)
            VALUES (%s,%s,%s,%s,%s)
            ON CONFLICT (id) DO UPDATE SET
               name=EXCLUDED.name, position=EXCLUDED.position, created_at=EXCLUDED.created_at, updated_at=EXCLUDED.updated_at
        """, (str(c.get("id")), c.get("name"), c.get("position"),
              parse_dt(c.get("created_at")), parse_dt(c.get("updated_at"))))
        conn.commit()
    upsert_raw(conn, "trigger_categories", int(c["id"]), c.get("updated_at"), c)

def sync_macros(conn):
    url = f"{Z_BASE}/api/v2/macros.json"
    params = {"per_page": min(PER_PAGE, 100)}
    if INCLUDE: params["include"] = INCLUDE
    page = get_with_retry(url, params=params)
    while True:
        for m in page.get("macros", []): upsert_macro(conn, m)
        next_page = page.get("next_page") or page.get("links", {}).get("next")
        if not next_page: break
        page = get_with_retry(next_page)
    logging.info("Macros: sync complete.")

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
        ))
        conn.commit()
    upsert_raw(conn, "macros", int(m["id"]), m.get("updated_at"), m)

# ----------------------------
# Main
# ----------------------------
def main():
    if os.getenv("DOWNLOAD_ATTACHMENTS", "true").lower() == "true":
        ensure_dir(ATTACHMENTS_DIR)
        logging.info("Attachment download enabled → %s", ATTACHMENTS_DIR)

    init_schema()
    with get_db() as conn:
        logging.info("Starting USERS…")
        sync_users(conn)

        logging.info("Starting ORGANIZATIONS…")
        sync_organizations(conn)

        logging.info("Starting TICKETS (+comments, attachments)…")
        sync_tickets_comments_attachments(conn)
        if USE_TICKET_EVENTS_FOR_COMMENTS:
            logging.info("Starting TICKET EVENTS for comments…")
            sync_ticket_events_for_comments(conn)

        logging.info("Starting VIEWS…")
        sync_views(conn)

        logging.info("Starting TRIGGERS…")
        sync_triggers(conn)

        logging.info("Starting TRIGGER CATEGORIES…")
        sync_trigger_categories(conn)

        logging.info("Starting MACROS…")
        sync_macros(conn)

    logging.info("✅ Zendesk incremental backup complete.")

if __name__ == "__main__":
    main()
