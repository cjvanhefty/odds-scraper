"""
Persist scraped events and prop lines to the shared SQLite DB.
"""
import sys
from pathlib import Path

# Allow importing backend from project root
ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backend.db import get_connection

PRIZEPICKS_BOOK_ID = 1
UNDERDOG_BOOK_ID = 2


def upsert_event(external_id: str | None, name: str, game_date: str, start_time: str | None = None) -> int:
    """Insert or get existing event; return event_id."""
    conn = get_connection()
    try:
        cur = conn.execute("SELECT id FROM events WHERE name = ? AND game_date = ?", (name, game_date))
        row = cur.fetchone()
        if row:
            return row[0]
        conn.execute(
            "INSERT INTO events (external_id, name, game_date, start_time) VALUES (?, ?, ?, ?)",
            (external_id, name, game_date, start_time),
        )
        conn.commit()
        cur = conn.execute("SELECT last_insert_rowid()")
        return cur.fetchone()[0]
    finally:
        conn.close()


def insert_prop_lines(book_id: int, event_id: int, lines: list[dict]) -> int:
    """
    Insert prop lines for an event. Each dict: player_name, stat_type, line_value, multiplier (optional).
    Returns count inserted.
    """
    conn = get_connection()
    try:
        count = 0
        for line in lines:
            conn.execute(
                """INSERT INTO prop_lines (event_id, book_id, player_name, stat_type, line_value, multiplier)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (
                    event_id,
                    book_id,
                    line["player_name"],
                    line["stat_type"],
                    line["line_value"],
                    line.get("multiplier"),
                ),
            )
            count += 1
        conn.commit()
        return count
    finally:
        conn.close()
