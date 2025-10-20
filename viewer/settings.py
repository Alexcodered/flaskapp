# viewer/settings.py
import os
from pathlib import Path
from dotenv import load_dotenv, find_dotenv

# Load .env robustly (works when called from root or viewer/)
_env = find_dotenv(usecwd=True)
if not _env:
    p = Path(__file__).resolve().parent.parent / ".env"
    if p.exists():
        _env = str(p)
load_dotenv(dotenv_path=_env or ".env", override=False)

# Postgres DSN for the viewer (same as backup/restore)
DATABASE_URL = os.getenv("DATABASE_URL")
assert DATABASE_URL, "DATABASE_URL missing in .env for viewer."

# Viewer settings
ATTACHMENTS_DIR = os.getenv("ATTACHMENTS_DIR", "./attachments")
PER_PAGE_DEFAULT = int(os.getenv("VIEWER_PER_PAGE", "25"))
