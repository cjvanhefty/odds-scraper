"""
Underdog Fantasy projections scraper.
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

# Map Underdog stat names to PrizePicks stat_type_name for matching
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


def _get_db_conn(
    server: str,
    database: str,
    user: str,
    password: str,
    trusted_connection: bool = False,
):
    """Get pyodbc connection. Use trusted_connection=True for Windows auth."""
    import pyodbc
    if trusted_connection:
        conn_str = (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={server};DATABASE={database};Trusted_Connection=yes;"
        )
    else:
        conn_str = (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={server};DATABASE={database};UID={user};PWD={password or ''}"
        )
    try:
        return pyodbc.connect(conn_str)
    except pyodbc.Error:
        if trusted_connection:
            conn_str = (
                f"DRIVER={{SQL Server}};"
                f"SERVER={server};DATABASE={database};Trusted_Connection=yes;"
            )
        else:
            conn_str = (
                f"DRIVER={{SQL Server}};"
                f"SERVER={server};DATABASE={database};UID={user};PWD={password or ''}"
            )
        return pyodbc.connect(conn_str)


def parse_records_from_json(data) -> list[dict]:
    """
    Parse API-like JSON into stage records.
    Expects list of objects with player name, stat, line, start_time (or game date).
    Accepts: { "display_name", "stat_type_name" or "stat", "line_score" or "line", "start_time" or "game_date" }.
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
    for i, item in enumerate(items):
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


def insert_underdog_stage(
    records: list[dict],
    server: str = "localhost\\SQLEXPRESS",
    database: str = "Props",
    user: str | None = None,
    password: str | None = None,
    trusted_connection: bool = False,
) -> int:
    """Truncate underdog_projection_stage and insert records. Returns count."""
    import pyodbc
    user = user or os.environ.get("PROPS_DB_USER", "dbadmin")
    password = password or os.environ.get("PROPS_DB_PASSWORD", "")
    conn = _get_db_conn(server, database, user, password, trusted_connection)
    cols = ["projection_id", "display_name", "stat_type_name", "line_score", "start_time"]
    placeholders = ", ".join("?" * len(cols))
    with conn:
        cursor = conn.cursor()
        cursor.execute("TRUNCATE TABLE [dbo].[underdog_projection_stage]")
        for r in records:
            cursor.execute(
                f"INSERT INTO [dbo].[underdog_projection_stage] ({', '.join(cols)}) VALUES ({placeholders})",
                [r.get(c) for c in cols],
            )
    count = len(records)
    conn.close()
    return count


def upsert_underdog_from_stage(
    server: str = "localhost\\SQLEXPRESS",
    database: str = "Props",
    user: str | None = None,
    password: str | None = None,
    trusted_connection: bool = False,
) -> int:
    """MERGE underdog_projection_stage into underdog_projection. Returns rows merged."""
    import pyodbc
    user = user or os.environ.get("PROPS_DB_USER", "dbadmin")
    password = password or os.environ.get("PROPS_DB_PASSWORD", "")
    conn = _get_db_conn(server, database, user, password, trusted_connection)
    sql = """
    MERGE [dbo].[underdog_projection] AS t
    USING [dbo].[underdog_projection_stage] AS s
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


OVER_UNDER_LINES_URL = "https://api.underdogfantasy.com/beta/v5/over_under_lines"


def _normalize_to_list(val):
    """If val is a dict, return list of values; else if list, return as-is; else []."""
    if isinstance(val, dict):
        return list(val.values())
    if isinstance(val, list):
        return val
    return []


def _normalize_to_dict(val, id_key="id"):
    """If val is a list of dicts, return dict by id; else if already dict, return as-is; else {}."""
    if isinstance(val, dict):
        return val
    if isinstance(val, list):
        return {x.get(id_key): x for x in val if isinstance(x, dict) and x.get(id_key) is not None}
    return {}


def parse_underdog_over_under_api(pickem_data: dict) -> list[dict]:
    """
    Parse Underdog's over_under_lines API response into stage records.
    Expects keys: players, appearances, over_under_lines; optional: games.
    Accepts top-level "data" wrapper.
    Accepts both array format (beta/v5 API) and dict format (lobby content API: player_grouped_lines).
    """
    records = []
    if not isinstance(pickem_data, dict):
        return records
    if "data" in pickem_data and isinstance(pickem_data["data"], dict):
        pickem_data = pickem_data["data"]
    raw_players = pickem_data.get("players") or []
    raw_appearances = pickem_data.get("appearances") or []
    raw_games = pickem_data.get("games") or []
    raw_oul = pickem_data.get("over_under_lines") or []
    players = _normalize_to_dict(raw_players)
    if not players and isinstance(raw_players, list):
        players = {p.get("id"): p for p in raw_players if isinstance(p, dict)}
    games = _normalize_to_dict(raw_games)
    if not games and isinstance(raw_games, list):
        games = {str(g.get("id")): g for g in raw_games if isinstance(g, dict)}
    appearances = _normalize_to_list(raw_appearances)
    over_under_lines = _normalize_to_list(raw_oul)

    # appearance_id -> (display_name, start_time_iso)
    appearance_info = {}
    for a in appearances:
        if not isinstance(a, dict):
            continue
        aid = a.get("id") or a.get("appearance_id")
        pid = a.get("player_id")
        gid = a.get("game_id") or a.get("match_id")
        if gid is not None and gid not in games:
            gid = str(gid)
        start_time = None
        if gid is not None and gid in games:
            g = games.get(gid)
            st = (g or {}).get("start_time") or (g or {}).get("starts_at") or (g or {}).get("scheduled_at")
            if st is not None:
                if isinstance(st, (int, float)):
                    start_time = datetime.utcfromtimestamp(st / 1000 if st > 1e12 else st).isoformat() + "Z"
                else:
                    start_time = str(st)[:19]
        if start_time is None:
            start_time = ""
        name = ""
        if pid and pid in players:
            p = players[pid]
            first = (p.get("first_name") or "").strip()
            last = (p.get("last_name") or "").strip()
            name = f"{first} {last}".strip() or (p.get("full_name") or p.get("display_name") or "")
        if aid is not None:
            appearance_info[aid] = (name, start_time)

    seen = set()
    for oul in over_under_lines:
        if not isinstance(oul, dict):
            continue
        appearance_stat = (oul.get("over_under") or {}).get("appearance_stat") or {}
        aid = appearance_stat.get("appearance_id")
        stat = (appearance_stat.get("display_stat") or appearance_stat.get("stat") or "").strip()
        stat = _normalize_stat(stat) or stat
        if not stat:
            stat = "Unknown"
        name, start_time = appearance_info.get(aid, ("", ""))
        # Line can be on the over_under_line (stat_value) or on each option (line/line_score)
        default_line = oul.get("stat_value")
        if default_line is not None:
            try:
                default_line = float(default_line)
            except (TypeError, ValueError):
                default_line = None
        options = oul.get("options") or []
        for opt in options:
            if not isinstance(opt, dict):
                continue
            line = opt.get("line") or opt.get("line_score")
            if line is None and default_line is not None:
                line = default_line
            if line is None:
                # Parse from choice_display_name_shorter e.g. "H 5.5" or "L 8.5"
                short = (opt.get("choice_display_name_shorter") or "").strip()
                if short:
                    parts = short.split()
                    for p in parts:
                        try:
                            line = float(p)
                            break
                        except (TypeError, ValueError):
                            continue
            if line is not None:
                try:
                    line = float(line)
                except (TypeError, ValueError):
                    line = None
            if line is None:
                continue
            key = (name, stat, start_time, line)
            if key in seen:
                continue
            seen.add(key)
            projection_id = hash(key) & 0x7FFFFFFF
            if projection_id < 0:
                projection_id = -projection_id
            records.append({
                "projection_id": projection_id,
                "display_name": (name or "")[:100],
                "stat_type_name": stat[:100],
                "line_score": line,
                "start_time": start_time or datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
            })
    return records


def _extract_from_nested(obj, out: list):
    """Recursively find arrays of objects that look like projections (have line/line_score and name)."""
    if isinstance(obj, list):
        for item in obj:
            if isinstance(item, dict):
                name = item.get("display_name") or item.get("player_name") or item.get("name") or item.get("title")
                stat = item.get("stat_type_name") or item.get("stat_type") or item.get("stat") or item.get("market")
                line = item.get("line_score") or item.get("line") or item.get("stat_line")
                st = item.get("start_time") or item.get("game_date") or item.get("startTime") or item.get("game_time")
                if name or stat or line is not None:
                    out.append(item)
            _extract_from_nested(item, out)
    elif isinstance(obj, dict):
        for v in obj.values():
            _extract_from_nested(v, out)


USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"


def fetch_with_playwright(
    save_path: str | None = None,
    user_data_dir: str | None = None,
    connect_url: str | None = None,
    headed: bool = False,
    debug: bool = False,
) -> list[dict]:
    """Load Underdog Fantasy pick'em in browser and capture API responses. Returns parsed records or [].
    Use user_data_dir for persistent profile (login saved between runs), or connect_url to attach to existing browser (e.g. Cursor).
    Use debug=True to capture every JSON response (any URL) so you can see what APIs the page calls."""
    from playwright.sync_api import sync_playwright
    captured = []
    api_records = []
    pickem_url = "https://www.underdogfantasy.com/games/pickem"
    close_browser = True

    with sync_playwright() as p:
        if connect_url:
            browser = p.chromium.connect_over_cdp(connect_url)
            context = browser.contexts[0] if browser.contexts else browser.new_context()
            close_browser = False
        elif user_data_dir:
            try:
                context = p.chromium.launch_persistent_context(
                    user_data_dir,
                    headless=not headed,
                    user_agent=USER_AGENT,
                    viewport={"width": 1280, "height": 720},
                    channel="chrome",
                    args=["--disable-blink-features=AutomationControlled"],
                )
            except Exception:
                context = p.chromium.launch_persistent_context(
                    user_data_dir,
                    headless=not headed,
                    user_agent=USER_AGENT,
                    viewport={"width": 1280, "height": 720},
                    args=["--disable-blink-features=AutomationControlled"],
                )
            browser = None
        else:
            browser = p.chromium.launch(headless=not headed)
            context = browser.new_context(user_agent=USER_AGENT)

        page = context.new_page()

        def on_response(response):
            try:
                if response.status != 200:
                    return
                u = response.url
                ct = (response.headers.get("content-type") or "").lower()
                if "json" not in ct and "javascript" not in ct:
                    return
                body = response.json()
                if not debug and "underdogfantasy.com" not in u:
                    return
                captured.append({"url": u, "body": body})
            except Exception:
                pass

        page.on("response", on_response)
        try:
            page.goto(pickem_url, wait_until="domcontentloaded", timeout=20000)
        except Exception:
            page.goto("https://www.underdogfantasy.com/", wait_until="domcontentloaded", timeout=20000)
        if headed:
            input("Log in in the browser window if needed. Press Enter here when done to continue capturing... ")
        # Only reload when not using a saved profile; reload can trigger logout on some sites
        if not user_data_dir:
            try:
                page.goto(pickem_url, wait_until="domcontentloaded", timeout=20000)
            except Exception:
                pass
        page.wait_for_timeout(3000)
        # Fetch Underdog's over_under_lines API from page context (uses page cookies/auth)
        api_records = []
        try:
            pickem_data = page.evaluate(
                """async (url) => {
                    const controller = new AbortController();
                    const t = setTimeout(() => controller.abort(), 15000);
                    try {
                        const r = await fetch(url, { credentials: 'include', signal: controller.signal });
                        clearTimeout(t);
                        if (!r.ok) return { _error: r.status };
                        return await r.json();
                    } catch (e) {
                        clearTimeout(t);
                        return { _error: -1 };
                    }
                }""",
                OVER_UNDER_LINES_URL,
                timeout=20000,
            )
            if pickem_data and isinstance(pickem_data, dict):
                err = pickem_data.get("_error")
                if err is not None:
                    if err == 401:
                        print("Underdog API returned 401 (unauthorized). Log in again: run with --headed and --user-data-dir .playwright-underdog, sign in, press Enter.")
                    elif err == 403:
                        print("Underdog API returned 403 (forbidden). Session may be invalid; try logging in again with --headed.")
                    elif err == -1:
                        print("Underdog API request failed (timeout or network error).")
                    else:
                        print(f"Underdog API returned HTTP {err}.")
                elif pickem_data:
                    captured.append({"url": OVER_UNDER_LINES_URL, "body": pickem_data})
                    api_records = parse_underdog_over_under_api(pickem_data)
        except Exception:
            pass
        page.wait_for_timeout(2000)

        if close_browser and browser:
            browser.close()
        elif user_data_dir and context:
            context.close()

    if save_path:
        Path(save_path).parent.mkdir(parents=True, exist_ok=True)
        with open(save_path, "w") as f:
            json.dump(captured, f, indent=2)
    if debug and captured:
        print("Captured URLs:")
        for c in captured:
            print("  ", c.get("url", "")[:100])
    records = []
    # Prefer records from direct over_under_lines API fetch (passed back from page.evaluate path)
    if api_records:
        records = api_records
    else:
        for c in captured:
            body = c.get("body") or {}
            recs = parse_underdog_over_under_api(body) if isinstance(body, dict) else []
            if not recs:
                recs = parse_records_from_json(body)
            if not recs:
                candidates = []
                _extract_from_nested(body, candidates)
                recs = parse_records_from_json(candidates)
            records.extend(recs)
    return records


def main():
    parser = argparse.ArgumentParser(description="Underdog Fantasy projections scraper")
    parser.add_argument("--input", metavar="JSON", help="Load projections from JSON file (list or { data: [] })")
    parser.add_argument("--browser", action="store_true", help="Capture from Underdog Fantasy in browser (saves underdog_captured.json)")
    parser.add_argument("--user-data-dir", metavar="DIR", help="Use persistent browser profile (login saved between runs); e.g. .playwright-underdog")
    parser.add_argument("--connect", metavar="WS_URL", help="Connect to existing browser via CDP (e.g. http://localhost:9222 or ws://...); use your logged-in Cursor/Chrome session")
    parser.add_argument("--headed", action="store_true", help="Show browser window (use with --user-data-dir to log in)")
    parser.add_argument("--debug", action="store_true", help="Capture every JSON response (any URL) to see what APIs the page calls; see underdog_captured.json")
    parser.add_argument("--db", action="store_true", help="Insert to stage and MERGE into underdog_projection")
    parser.add_argument("--db-server", default="localhost\\SQLEXPRESS")
    parser.add_argument("--db-user", default=os.environ.get("PROPS_DB_USER", "dbadmin"))
    parser.add_argument("--db-password", default=os.environ.get("PROPS_DB_PASSWORD", ""))
    parser.add_argument("--trusted-connection", action="store_true", help="Use Windows Authentication")
    parser.add_argument("--database", default="Props")
    args = parser.parse_args()

    records = []
    if args.input:
        with open(args.input) as f:
            data = json.load(f)
        # Captured format: list of { "url": "...", "body": {...} } from --browser --debug
        if isinstance(data, list) and data and isinstance(data[0], dict) and "url" in data[0] and "body" in data[0]:
            for entry in data:
                body = entry.get("body") or {}
                recs = parse_underdog_over_under_api(body)
                if not recs:
                    recs = parse_records_from_json(body)
                if not recs:
                    candidates = []
                    _extract_from_nested(body, candidates)
                    recs = parse_records_from_json(candidates)
                records.extend(recs)
        else:
            records = parse_records_from_json(data)
        print(f"Parsed {len(records)} records from {args.input}")
    elif args.browser:
        path = "underdog_captured.json"
        if args.user_data_dir:
            print(f"Using profile: {args.user_data_dir}")
        records = fetch_with_playwright(
            save_path=path,
            user_data_dir=args.user_data_dir,
            connect_url=args.connect,
            headed=args.headed,
            debug=args.debug,
        )
        print(f"Captured {len(records)} projection records (raw saved to {path})")
    else:
        print("Use --input <file.json> to load data, or --browser to capture from the app.")
        return 0

    if not records:
        print("No records to load. Use --input <file.json> with projection data, or check underdog_captured.json for API shape.")
        if args.db:
            print("Skipping DB update (0 records).")
        return 0

    if args.db:
        trusted = getattr(args, "trusted_connection", False) or os.environ.get("PROPS_DB_USE_TRUSTED_CONNECTION", "").strip().lower() in ("1", "true", "yes")
        n = insert_underdog_stage(records, server=args.db_server, database=args.database, user=args.db_user, password=args.db_password, trusted_connection=trusted)
        print(f"Staged {n} rows")
        m = upsert_underdog_from_stage(server=args.db_server, database=args.database, user=args.db_user, password=args.db_password, trusted_connection=trusted)
        print(f"Merged {m} rows into underdog_projection")
    return 0


if __name__ == "__main__":
    exit(main())
