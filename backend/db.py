"""
SQLite connection and schema setup for sports props app.
"""
import sqlite3
from pathlib import Path

DB_PATH = Path(__file__).resolve().parent / "data" / "props.db"


def get_connection():
    """Return a connection to the SQLite database."""
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    return sqlite3.connect(DB_PATH)


def init_db():
    """Create tables and seed data from schema.sql."""
    schema_path = Path(__file__).resolve().parent / "schema.sql"
    conn = get_connection()
    try:
        conn.executescript(schema_path.read_text())
        conn.commit()
    finally:
        conn.close()


if __name__ == "__main__":
    init_db()
    print(f"Database initialized at {DB_PATH}")
