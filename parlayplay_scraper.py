"""
Parlay Play projections scraper.
Fetches player prop lines for matching with PrizePicks (same player + stat + game).
Use --input to load from a JSON file, or --browser to capture from the app via Playwright.
"""

import db_config  # noqa: F401 - load .env from repo root before DB
import argparse
import json
import os
import re
from datetime import datetime
from pathlib import Path

# Map Parlay Play stat names to PrizePicks stat_type_name for matching
STAT_NORMALIZE = {
    "pts": "Points",
    "points": "Points",
    "reb": "Rebounds",
    "rebounds": "Rebounds",
    "ast": "Assists",
    "assists": "Assists",
    "stl": "Steals",
    "steals": "Steals",
    "blk": "Blocks",
    "blocks": "Blocks",
    "tov": "Turnovers",
    "turnovers": "Turnovers",
    "3pm": "3 Pointers Made",
    "3pmade": "3 Pointers Made",
    "threes": "3 Pointers Made",
    "3pt": "3 Pointers",
    "3ptm": "3 Pointers Made",
    "oreb": "Offensive Rebounds",
    "dreb": "Defensive Rebounds",
}


def _normalize_stat(s: str) -> str:
    if not s:
        return ""
    key = re.sub(r"[^a-z0-9]", "", s.strip().lower())
    return STAT_NORMALIZE.get(key, s.strip())


def _get_db_conn(server: str, database: str, user: str, password: str):
    import pyodbc
    conn_str = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={server};DATABASE={database};UID={user};PWD={password}"
    )
    try:
        return pyodbc.connect(conn_str)
    except pyodbc.Error:
        conn_str = (
            f"DRIVER={{SQL Server}};"
            f"SERVER={server};DATABASE={database};UID={user};PWD={password}"
        )
        return pyodbc.connect(conn_str)


def parse_records_from_json(data) -> list[dict]:
    """
    Parse API-like JSON into stage records.
    Expects list of objects with player name, stat, line, start_time (or game date).
    """
    records = []
    if isinstance(data, list):
        items = data
    elif isinstance(data, dict) and "data" in data:
        items = data["data"] if isinstance(data["data"], list) else [data["data"]]
    elif isinstance(data, dict) and "projections" in data:
        items = data["projections"] if isinstance(data["projections"], list) else [data["projections"]]
    else:
        items = [data]
    seen = set()
    for item in items:
        if not isinstance(item, dict):
            continue
        name = (item.get("display_name") or item.get("player_name") or item.get("name") or "").strip()
        stat = (item.get("stat_type_name") or item.get("stat") or item.get("stat_type") or "").strip()
        stat = _normalize_stat(stat) or stat
        line = item.get("line_score") or item.get("line")
        if line is not None:
            try:
                line = float(line)
            except (TypeError, ValueError):
                line = None
        st = item.get("start_time") or item.get("game_date") or item.get("startTime")
        if st is None:
            continue
        if isinstance(st, (int, float)):
            st = datetime.utcfromtimestamp(st / 1000 if st > 1e12 else st).isoformat() + "Z"
        key = (name, stat, str(st)[:19])
        if key in seen:
            continue
        seen.add(key)
        projection_id = hash(key) & 0x7FFFFFFF
        if projection_id < 0:
            projection_id = -projection_id
        records.append({
            "projection_id": projection_id,
            "display_name": name[:100] if name else "",
            "stat_type_name": (stat or "Unknown")[:100],
            "line_score": line,
            "start_time": st,
        })
    return records


def insert_parlay_play_stage(
    records: list[dict],
    server: str = "localhost\\SQLEXPRESS",
    database: str = "Props",
    user: str | None = None,
    password: str | None = None,
) -> int:
    """Truncate parlay_play_projection_stage and insert records. Returns count."""
    import pyodbc
    user = user or os.environ.get("PROPS_DB_USER", "dbadmin")
    password = password or os.environ.get("PROPS_DB_PASSWORD", "")
    conn = _get_db_conn(server, database, user, password)
    cols = ["projection_id", "display_name", "stat_type_name", "line_score", "start_time"]
    placeholders = ", ".join("?" * len(cols))
    with conn:
        cursor = conn.cursor()
        cursor.execute("TRUNCATE TABLE [dbo].[parlay_play_projection_stage]")
        for r in records:
            cursor.execute(
                f"INSERT INTO [dbo].[parlay_play_projection_stage] ({', '.join(cols)}) VALUES ({placeholders})",
                [r.get(c) for c in cols],
            )
    conn.close()
    return len(records)


def upsert_parlay_play_from_stage(
    server: str = "localhost\\SQLEXPRESS",
    database: str = "Props",
    user: str | None = None,
    password: str | None = None,
) -> int:
    """MERGE parlay_play_projection_stage into parlay_play_projection. Returns rows merged."""
    import pyodbc
    user = user or os.environ.get("PROPS_DB_USER", "dbadmin")
    password = password or os.environ.get("PROPS_DB_PASSWORD", "")
    conn = _get_db_conn(server, database, user, password)
    sql = """
    MERGE [dbo].[parlay_play_projection] AS t
    USING [dbo].[parlay_play_projection_stage] AS s
    ON t.projection_id = s.projection_id
    WHEN MATCHED AND (ISNULL(t.line_score,-1) <> ISNULL(s.line_score,-1) OR ISNULL(t.start_time,'') <> ISNULL(CAST(s.start_time AS nvarchar(50)),''))
        THEN UPDATE SET display_name = s.display_name, stat_type_name = s.stat_type_name, line_score = s.line_score, start_time = s.start_time, last_modified_at = GETUTCDATE()
    WHEN NOT MATCHED BY TARGET
        THEN INSERT (projection_id, display_name, stat_type_name, line_score, start_time, last_modified_at)
        VALUES (s.projection_id, s.display_name, s.stat_type_name, s.line_score, s.start_time, GETUTCDATE());
    """
    with conn:
        cursor = conn.cursor()
        cursor.execute(sql)
        count = cursor.rowcount
    conn.close()
    return count


def _extract_from_nested(obj, out: list):
    """Recursively find arrays of objects that look like projections."""
    if isinstance(obj, list):
        for item in obj:
            if isinstance(item, dict):
                name = item.get("display_name") or item.get("player_name") or item.get("name") or item.get("title")
                stat = item.get("stat_type_name") or item.get("stat_type") or item.get("stat") or item.get("market")
                line = item.get("line_score") or item.get("line") or item.get("stat_line")
                if name or stat or line is not None:
                    out.append(item)
            _extract_from_nested(item, out)
    elif isinstance(obj, dict):
        for v in obj.values():
            _extract_from_nested(v, out)


def fetch_with_playwright(save_path: str | None = None) -> list[dict]:
    """Load Parlay Play in browser and capture API responses. Returns parsed records or []."""
    from playwright.sync_api import sync_playwright
    captured = []
    url = "https://parlayplay.io"
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )
        page = context.new_page()

        def on_response(response):
            try:
                if response.status != 200:
                    return
                u = response.url
                if "parlayplay" not in u.lower():
                    return
                ct = (response.headers.get("content-type") or "").lower()
                if "json" not in ct and "javascript" not in ct:
                    return
                body = response.json()
                captured.append({"url": u, "body": body})
            except Exception:
                pass

        page.on("response", on_response)
        try:
            page.goto(url, wait_until="domcontentloaded", timeout=20000)
        except Exception:
            pass
        page.wait_for_timeout(8000)
        browser.close()

    if save_path:
        Path(save_path).parent.mkdir(parents=True, exist_ok=True)
        with open(save_path, "w") as f:
            json.dump(captured, f, indent=2)
    records = []
    for c in captured:
        body = c.get("body") or {}
        recs = parse_records_from_json(body)
        if not recs:
            candidates = []
            _extract_from_nested(body, candidates)
            recs = parse_records_from_json(candidates)
        records.extend(recs)
    return records


def main():
    parser = argparse.ArgumentParser(description="Parlay Play projections scraper")
    parser.add_argument("--input", metavar="JSON", help="Load projections from JSON file (list or { data: [] })")
    parser.add_argument("--browser", action="store_true", help="Capture from Parlay Play in browser (saves parlayplay_captured.json)")
    parser.add_argument("--db", action="store_true", help="Insert to stage and MERGE into parlay_play_projection")
    parser.add_argument("--db-server", default="localhost\\SQLEXPRESS")
    parser.add_argument("--db-user", default=os.environ.get("PROPS_DB_USER", "dbadmin"))
    parser.add_argument("--db-password", default=os.environ.get("PROPS_DB_PASSWORD", ""))
    parser.add_argument("--database", default="Props")
    args = parser.parse_args()

    records = []
    if args.input:
        with open(args.input) as f:
            data = json.load(f)
        records = parse_records_from_json(data)
        print(f"Parsed {len(records)} records from {args.input}")
    elif args.browser:
        path = "parlayplay_captured.json"
        records = fetch_with_playwright(save_path=path)
        print(f"Captured {len(records)} projection records (raw saved to {path})")
    else:
        print("Use --input <file.json> to load data, or --browser to capture from the app.")
        return 0

    if not records:
        print("No records to load. Use --input <file.json> with projection data, or check parlayplay_captured.json for API shape.")
        if args.db:
            print("Skipping DB update (0 records).")
        return 0

    if args.db:
        n = insert_parlay_play_stage(records, server=args.db_server, database=args.database, user=args.db_user, password=args.db_password)
        print(f"Staged {n} rows")
        m = upsert_parlay_play_from_stage(server=args.db_server, database=args.database, user=args.db_user, password=args.db_password)
        print(f"Merged {m} rows into parlay_play_projection")
    return 0


if __name__ == "__main__":
    exit(main())
