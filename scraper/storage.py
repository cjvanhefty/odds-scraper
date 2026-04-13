"""
Persist scraped events and prop lines to the shared SQLite DB.
"""
import json
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


def _resolve_book_slug(conn, book_id: int) -> str:
    cur = conn.execute("SELECT slug FROM books WHERE id = ?", (book_id,))
    row = cur.fetchone()
    if row and row[0]:
        return row[0]
    return f"book_{book_id}"


def _upsert_player_projection(conn, event_id: int, book_slug: str, line: dict):
    """
    Upsert one projection row keyed by event/player/stat and merge book-specific data.
    """
    player_name = line["player_name"]
    stat_type = line["stat_type"]
    book_projection = {
        "line_value": line["line_value"],
        "multiplier": line.get("multiplier"),
    }

    cur = conn.execute(
        """SELECT id, books_json
           FROM player_projections
           WHERE event_id = ? AND player_name = ? AND stat_type = ?""",
        (event_id, player_name, stat_type),
    )
    row = cur.fetchone()

    if row:
        projection_id = row[0]
        existing_books = {}
        if row[1]:
            try:
                existing_books = json.loads(row[1])
            except json.JSONDecodeError:
                existing_books = {}
        existing_books[book_slug] = book_projection
        conn.execute(
            """UPDATE player_projections
               SET books_json = ?, updated_at = datetime('now')
               WHERE id = ?""",
            (json.dumps(existing_books), projection_id),
        )
        return

    conn.execute(
        """INSERT INTO player_projections (event_id, player_name, stat_type, books_json)
           VALUES (?, ?, ?, ?)""",
        (event_id, player_name, stat_type, json.dumps({book_slug: book_projection})),
    )


def insert_prop_lines(book_id: int, event_id: int, lines: list[dict]) -> int:
    """
    Insert prop lines for an event. Each dict: player_name, stat_type, line_value, multiplier (optional).
    Returns count inserted.
    """
    conn = get_connection()
    try:
        count = 0
        book_slug = _resolve_book_slug(conn, book_id)
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
            _upsert_player_projection(conn, event_id, book_slug, line)
            count += 1
        conn.commit()
        return count
    finally:
        conn.close()


def get_player_projection_groups(event_id: int) -> list[dict]:
    """
    Return projections grouped by player, then stat_type, with all books tied together.
    """
    conn = get_connection()
    try:
        cur = conn.execute(
            """SELECT player_name, stat_type, books_json
               FROM player_projections
               WHERE event_id = ?
               ORDER BY player_name, stat_type""",
            (event_id,),
        )

        grouped: dict[str, dict[str, dict]] = {}
        for player_name, stat_type, books_json in cur.fetchall():
            player_group = grouped.setdefault(player_name, {})
            try:
                player_group[stat_type] = json.loads(books_json or "{}")
            except json.JSONDecodeError:
                player_group[stat_type] = {}

        return [
            {"player_name": player_name, "stats": stats}
            for player_name, stats in grouped.items()
        ]
    finally:
        conn.close()
