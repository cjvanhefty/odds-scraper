"""DB connection for the API. Uses same env and logic as projection_over_streak."""

import db_config  # noqa: F401 - load .env from repo root before reading PROPS_DB_*
import os
import sys

# Allow importing from repo root
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from projection_over_streak import _get_db_conn, _get_db_conn_trusted


def get_conn():
    """Return a pyodbc connection to Props DB. Caller must close."""
    server = os.environ.get("PROPS_DB_SERVER", "localhost\\SQLEXPRESS")
    database = os.environ.get("PROPS_DATABASE", "Props")
    user = os.environ.get("PROPS_DB_USER", "dbadmin")
    password = os.environ.get("PROPS_DB_PASSWORD", "")
    # Support both names used in this repo
    trusted_env = (
        os.environ.get("PROPS_DB_USE_TRUSTED_CONNECTION", "").strip().lower()
        or os.environ.get("PROPS_DB_TRUSTED", "").strip().lower()
    )
    trusted = trusted_env in ("1", "true", "yes")
    if trusted:
        return _get_db_conn_trusted(server, database)
    if not user or not password:
        # Avoid login failed for 'dbadmin' when no credentials set — try Windows auth once
        try:
            return _get_db_conn_trusted(server, database)
        except Exception:
            pass
    return _get_db_conn(server, database, user, password)
