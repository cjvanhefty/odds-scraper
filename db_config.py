"""
Load DB credentials from a single .env file at repo root.
Import this module first so PROPS_DB_* env vars are set before any DB connection.
Used by: app (FastAPI), underdog_scraper, prizepicks_scraper, parlayplay_scraper,
         projection_over_streak, nba_scraper, soccer_scraper.
"""
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent
# Load .env; if missing, try .env.txt (e.g. Windows hides leading dot)
_ENV_FILE = _REPO_ROOT / ".env"
if not _ENV_FILE.exists():
    _ENV_FILE = _REPO_ROOT / ".env.txt"


def _load_env():
    if _ENV_FILE.exists():
        try:
            from dotenv import load_dotenv
            load_dotenv(_ENV_FILE)
        except ImportError:
            pass


_load_env()
