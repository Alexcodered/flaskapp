import os
from math import ceil
from datetime import datetime, timezone
from flask import Flask, render_template, request, redirect, url_for, send_from_directory, abort
import humanize

# Support both "python -m viewer.app" and "python viewer/app.py"
try:
    from .db import fetch_all, fetch_one
    from . import settings
except ImportError:
    import sys
    sys.path.append(os.path.dirname(__file__))
    from db import fetch_all, fetch_one
    import settings

app = Flask(__name__)

# -----------------------
# Helpers / Jinja filters
# -----------------------
@app.template_filter("humansize")
def j_humansize(val):
    try:
        n = int(val or 0)
    except Exception:
        return "—"
    return humanize.naturalsize(n, binary=False) if n else "—"

def _parse_dt(val):
    if not val:
        return None
    if isinstance(val, datetime):
        return val
    try:
        # MySQL DATETIME string -> datetime
        return datetime.fromisoformat(str(val))
    except Exception:
        return None

@app.template_filter("humants")
def j_humants(val):
    dt = _parse_dt(val)
    if not dt:
        return "—"

    # Normalize both to timezone-aware UTC for subtraction
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    now = datetime.now(timezone.utc)

    delta = now - dt
    # show date if older than 2 days, else relative
    return humanize.naturaldate(dt) if delta.days >= 2 else humanize.naturaltime(delta)

def paginate(total, page, per_page):
    pages = max(1, ceil(total / per_page)) if total else 1
    page = max(1, min(page, pages))
    return page, pages

# -----------------------
# Routes
# -----------------------
@app.route("/")
def home():
    return redirect(url_for("tickets"))

# ---- Tickets ----
@app.route("/tickets")
def tickets():
    q = (request.args.get("q") or "").strip()
    status = (request.args.get("status") or "").strip().lower()
    page = int(request.args.get("page", 1))
    per_page = int(request.args.get("per_page", settings.PER_PAGE_DEFAULT))

    where = []
    params = {}

    if q:
        where.append("(t.id = %(qid)s OR t.subject LIKE %(qs)s OR t.description LIKE %(qd)s)")
        params.update({"qid": q if q.isdigit() else -1, "qs": f"%{q}%", "qd": f"%{q}%"})
    if status:
        where.append("LOWER(t.status) = %(st)s")
        params["st"] = status

    where_sql = ("WHERE " + " AND ".join(where)) if where else ""
    total = fetch_one(f"SELECT COUNT(*) AS c FROM tickets t {where_sql}", params)["c"]
    page, pages = paginate(total, page, per_page)
    offset = (page - 1) * per_page

    rows = fetch_all(
        f"""
        SELECT t.id, t.subject, t.status, t.updated_at, t.created_at, t.priority, t.type,
               t.requester_id, t.assignee_id, t.organization_id
        FROM tickets t
        {where_sql}
        ORDER BY t.updated_at DESC
        LIMIT %(limit)s OFFSET %(offset)s
        """,
        {**params, "limit": per_page, "offset": offset},
    )

    return render_template(
        "tickets_list.html",
        rows=rows, q=q, status=status,
        page=page, pages=pages, total=total, per_page=per_page
    )

@app.route("/tickets/<int:ticket_id>")
def ticket_detail(ticket_id: int):
    t = fetch_one("SELECT * FROM tickets WHERE id=%s", (ticket_id,))
    if not t:
        abort(404)

    org = fetch_one("SELECT * FROM organizations WHERE id=%s", (t["organization_id"],)) if t.get("organization_id") else None
    requester = fetch_one("SELECT * FROM users WHERE id=%s", (t["requester_id"],)) if t.get("requester_id") else None
    assignee = fetch_one("SELECT * FROM users WHERE id=%s", (t["assignee_id"],)) if t.get("assignee_id") else None

    comments = fetch_all(
        "SELECT * FROM ticket_comments WHERE ticket_id=%s ORDER BY created_at ASC",
        (ticket_id,)
    )
    atts = fetch_all(
        "SELECT * FROM attachments WHERE ticket_id=%s ORDER BY created_at ASC",
        (ticket_id,)
    )
    # map comment_id -> list
    atts_by_comment = {}
    for a in atts:
        cid = a.get("comment_id")
        atts_by_comment.setdefault(cid, []).append(a)

    return render_template(
        "ticket_detail.html",
        t=t, org=org, requester=requester, assignee=assignee,
        comments=comments, atts_by_comment=atts_by_comment
    )

# serve local attachments (read-only)
@app.route("/attachments/<int:ticket_id>/<path:filename>")
def serve_attachment(ticket_id, filename):
    base = os.path.join(settings.ATTACHMENTS_DIR, str(ticket_id))
    if not os.path.isdir(base):
        abort(404)
    return send_from_directory(base, filename, as_attachment=False)

# ---- Users ----
@app.route("/users")
def users():
    q = (request.args.get("q") or "").strip()
    active = request.args.get("active")  # "1", "0", or None
    page = int(request.args.get("page", 1))
    per_page = int(request.args.get("per_page", settings.PER_PAGE_DEFAULT))

    where = []
    params = {}

    if q:
        where.append("(u.id = %(qid)s OR u.name LIKE %(qs)s OR u.email LIKE %(qe)s)")
        params.update({"qid": q if q.isdigit() else -1, "qs": f"%{q}%", "qe": f"%{q}%"})
    if active in ("0", "1"):
        where.append("u.active = %(act)s")
        params["act"] = int(active)

    where_sql = ("WHERE " + " AND ".join(where)) if where else ""
    total = fetch_one(f"SELECT COUNT(*) AS c FROM users u {where_sql}", params)["c"]
    page, pages = paginate(total, page, per_page)
    offset = (page - 1) * per_page

    rows = fetch_all(
        f"""
        SELECT u.id, u.name, u.email, u.role, u.active, u.suspended,
               u.organization_id, u.updated_at, u.created_at, u.last_login_at
        FROM users u
        {where_sql}
        ORDER BY u.updated_at DESC
        LIMIT %(limit)s OFFSET %(offset)s
        """,
        {**params, "limit": per_page, "offset": offset},
    )

    return render_template("users_list.html",
        rows=rows, q=q, active=active, page=page, pages=pages, total=total, per_page=per_page
    )

@app.route("/users/<int:user_id>")
def user_detail(user_id: int):
    u = fetch_one("SELECT * FROM users WHERE id=%s", (user_id,))
    if not u:
        abort(404)

    org = fetch_one("SELECT * FROM organizations WHERE id=%s", (u["organization_id"],)) if u.get("organization_id") else None
    tickets = fetch_all(
        """
        SELECT id, subject, status, created_at, updated_at
        FROM tickets
        WHERE requester_id=%s OR assignee_id=%s
        ORDER BY updated_at DESC
        LIMIT 50
        """,
        (user_id, user_id),
    )
    return render_template("user_detail.html", u=u, org=org, tickets=tickets)

# ---- Orgs ----
@app.route("/orgs")
def orgs():
    q = (request.args.get("q") or "").strip()
    page = int(request.args.get("page", 1))
    per_page = int(request.args.get("per_page", settings.PER_PAGE_DEFAULT))

    where = []
    params = {}
    if q:
        where.append("(o.id = %(qid)s OR o.name LIKE %(qs)s)")
        params.update({"qid": q if q.isdigit() else -1, "qs": f"%{q}%"})
    where_sql = ("WHERE " + " AND ".join(where)) if where else ""

    total = fetch_one(f"SELECT COUNT(*) AS c FROM organizations o {where_sql}", params)["c"]
    page, pages = paginate(total, page, per_page)
    offset = (page - 1) * per_page

    rows = fetch_all(
        f"""
        SELECT o.id, o.name, o.updated_at, o.created_at
        FROM organizations o
        {where_sql}
        ORDER BY o.updated_at DESC
        LIMIT %(limit)s OFFSET %(offset)s
        """,
        {**params, "limit": per_page, "offset": offset},
    )
    return render_template("orgs_list.html",
        rows=rows, q=q, page=page, pages=pages, total=total, per_page=per_page
    )

# ---- Snapshots ----
@app.route("/views")
def views_():
    rows = fetch_all("SELECT id, title, active, updated_at FROM views ORDER BY updated_at DESC LIMIT 200")
    return render_template("simple_list.html", title="Views", rows=rows, cols=[("id","ID"),("title","Title"),("active","Active"),("updated_at","Updated")])

@app.route("/triggers")
def triggers_():
    rows = fetch_all("SELECT id, title, active, updated_at FROM triggers ORDER BY updated_at DESC LIMIT 200")
    return render_template("simple_list.html", title="Triggers", rows=rows, cols=[("id","ID"),("title","Title"),("active","Active"),("updated_at","Updated")])

@app.route("/trigger-categories")
def trigger_categories_():
    rows = fetch_all("SELECT id, name, updated_at FROM trigger_categories ORDER BY updated_at DESC LIMIT 200")
    return render_template("simple_list.html", title="Trigger Categories", rows=rows, cols=[("id","ID"),("name","Name"),("updated_at","Updated")])

@app.route("/macros")
def macros_():
    rows = fetch_all("SELECT id, title, active, updated_at FROM macros ORDER BY updated_at DESC LIMIT 200")
    return render_template("simple_list.html", title="Macros", rows=rows, cols=[("id","ID"),("title","Title"),("active","Active"),("updated_at","Updated")])

# -----------------------
if __name__ == "__main__":
    app.run(host="127.0.0.1", port=5050, debug=True)
