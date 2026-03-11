"""
NBA player gamelog scraper using nba_api.
Fetches gamelog data from 2026-02-13 onward for 2025-26 season,
with a 3-day lookback to catch missed runs.
"""

import sys
from pathlib import Path
if str(Path(__file__).resolve().parent.parent) not in sys.path:
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import db_config  # noqa: F401 - load .env from repo root before DB
import argparse
import os
import time
from datetime import datetime, timedelta
from typing import Optional

# Date logic: fetch from 2 days ago through today (MERGE handles duplicates)
EARLIEST_DATE = (datetime.now().date() - timedelta(days=2))

RATE_LIMIT_SECONDS = 0.6  # Avoid NBA.com throttling
API_TIMEOUT = 120  # stats.nba.com is slow and often times out
API_RETRIES = 5
RETRY_BACKOFF_SECONDS = 15  # Wait between retries (stats.nba.com throttles)


def _get_date_range() -> tuple[str, str]:
    """Return (date_from, date_to) as YYYY-MM-DD strings."""
    today = datetime.now().date()
    date_to = today
    date_from = EARLIEST_DATE
    return date_from.strftime("%Y-%m-%d"), date_to.strftime("%Y-%m-%d")


def _parse_game_date(raw: str | None) -> str:
    """Parse GAME_DATE from API (e.g. 'JAN 15, 2025' or '2025-01-15') to YYYY-MM-DD."""
    if not raw or not str(raw).strip():
        return ""
    s = str(raw).strip()
    # Already YYYY-MM-DD
    if len(s) == 10 and s[4] == "-" and s[7] == "-":
        return s
    try:
        # Try common formats
        for fmt in ("%b %d, %Y", "%B %d, %Y", "%m/%d/%Y", "%Y-%m-%d"):
            try:
                dt = datetime.strptime(s, fmt)
                return dt.strftime("%Y-%m-%d")
            except ValueError:
                continue
    except Exception:
        pass
    return s[:20]  # Fallback: truncate to varchar(20)


def _safe_int(val, default: int = 0) -> int:
    """Coerce to int for smallint columns."""
    if val is None or val == "":
        return default
    try:
        return int(float(val))
    except (ValueError, TypeError):
        return default


def _safe_decimal(val) -> Optional[float]:
    """Coerce to float for decimal columns; None when empty/invalid/NaN/inf (SQL Server rejects these)."""
    if val is None or val == "":
        return None
    try:
        f = float(val)
        if f != f or f == float("inf") or f == float("-inf"):  # NaN or inf
            return None
        return f
    except (ValueError, TypeError):
        return None


def _get(r: dict, *keys: str):
    """First non-None value from r for any of the given keys (handles LeagueGameLog vs PlayerGameLog casing)."""
    for k in keys:
        v = r.get(k)
        if v is not None and v != "":
            return v
    return None


def _get_db_conn(
    server: str,
    database: str,
    user: str,
    password: str,
    trusted_connection: bool = False,
):
    """Get pyodbc connection to SQL Server. Use trusted_connection=True for Windows auth (no user/password)."""
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


def fetch_gamelogs_league(
    season: str,
    date_from: str,
    date_to: str,
) -> list[dict]:
    """Fetch all player gamelogs in one bulk call via LeagueGameLog.
    Returns list of row dicts (column names as returned by API).
    If the API does not return a player id column, returns [] so caller can fall back to per-player.
    """
    from nba_api.stats.endpoints import leaguegamelog

    last_err = None
    for attempt in range(API_RETRIES):
        try:
            log = leaguegamelog.LeagueGameLog(
                season=season,
                season_type_all_star="Regular Season",
                player_or_team_abbreviation="P",
                date_from_nullable=date_from,
                date_to_nullable=date_to,
                timeout=API_TIMEOUT,
            )
            df = log.get_data_frames()[0]
            if df.empty:
                return []
            # LeagueGameLog may return PLAYER_ID or Player_ID depending on API
            if any(c in df.columns for c in ("PLAYER_ID", "Player_ID", "player_id")):
                return [row.to_dict() for _, row in df.iterrows()]
            # No player id column; caller must use per-player fetch
            return []
        except Exception as e:
            last_err = e
            if attempt < API_RETRIES - 1:
                wait = RETRY_BACKOFF_SECONDS * (attempt + 1)
                print(f"  LeagueGameLog retry {attempt + 1}/{API_RETRIES} in {wait}s: {e}")
                time.sleep(wait)
            else:
                raise RuntimeError(f"LeagueGameLog failed: {last_err}") from last_err
    return []


def get_player_ids(season: str = "2025-26") -> list[int]:
    """Fetch all player IDs for the season from CommonAllPlayers."""
    from nba_api.stats.endpoints import commonallplayers

    last_err = None
    for attempt in range(API_RETRIES):
        try:
            players = commonallplayers.CommonAllPlayers(
                season=season,
                is_only_current_season=1,  # Current roster only (~500 vs 5000+)
                timeout=API_TIMEOUT,
            )
            df = players.get_data_frames()[0]
            return df["PERSON_ID"].dropna().astype(int).unique().tolist()
        except Exception as e:
            last_err = e
            if attempt < API_RETRIES - 1:
                wait = RETRY_BACKOFF_SECONDS * (attempt + 1)
                print(f"  Retry {attempt + 1}/{API_RETRIES} in {wait}s: {e}")
                time.sleep(wait)
    raise RuntimeError(f"Failed to fetch player IDs: {last_err}")


def fetch_gamelogs(
    player_ids: list[int],
    season: str,
    date_from: str,
    date_to: str,
) -> list[dict]:
    """Fetch gamelog data for given players within date range."""
    from nba_api.stats.endpoints import playergamelog

    all_rows = []
    for i, pid in enumerate(player_ids):
        if (i + 1) % 50 == 0:
            print(f"  Fetched {i + 1}/{len(player_ids)} players...")
        last_err = None
        for attempt in range(API_RETRIES):
            try:
                log = playergamelog.PlayerGameLog(
                    player_id=str(pid),
                    season=season,
                    date_from_nullable=date_from,
                    date_to_nullable=date_to,
                    timeout=API_TIMEOUT,
                )
                df = log.get_data_frames()[0]
                if df.empty:
                    break
                for _, row in df.iterrows():
                    all_rows.append(row.to_dict())
                break
            except Exception as e:
                last_err = e
                if attempt < API_RETRIES - 1:
                    time.sleep(RETRY_BACKOFF_SECONDS * (attempt + 1))
        else:
            print(f"  Warning: player {pid}: {last_err}")
        time.sleep(RATE_LIMIT_SECONDS)
    return all_rows


def parse_to_stage_records(raw_rows: list[dict]) -> list[dict]:
    """Convert nba_api gamelog rows to player_stat_stage records.
    Accepts rows from PlayerGameLog (Player_ID, Game_ID) or LeagueGameLog (PLAYER_ID, GAME_ID, etc.).
    """
    records = []
    seen = set()
    for r in raw_rows:
        player_id = _safe_int(_get(r, "Player_ID", "PLAYER_ID", "player_id"))
        game_id = str(_get(r, "Game_ID", "GAME_ID") or "").strip()[:20]
        if not player_id or not game_id:
            continue
        key = (player_id, game_id)
        if key in seen:
            continue
        seen.add(key)

        season_id = str(_get(r, "SEASON_ID") or "").strip()[:10] or ""
        game_date = _parse_game_date(_get(r, "GAME_DATE"))
        matchup = str(_get(r, "MATCHUP") or "").strip()[:20]
        wl_raw = str(_get(r, "WL") or "").strip().upper()
        wl = wl_raw[0] if wl_raw in ("W", "L") else ""

        records.append({
            "season_id": season_id,
            "player_id": player_id,
            "game_id": game_id,
            "game_date": game_date,
            "matchup": matchup,
            "wl": wl,
            "min": _safe_int(_get(r, "MIN")),
            "fgm": _safe_int(_get(r, "FGM")),
            "fga": _safe_int(_get(r, "FGA")),
            "fg_pct": _safe_decimal(_get(r, "FG_PCT")),
            "fg3m": _safe_int(_get(r, "FG3M")),
            "fg3a": _safe_int(_get(r, "FG3A")),
            "fg3_pct": _safe_decimal(_get(r, "FG3_PCT")),
            "ftm": _safe_int(_get(r, "FTM")),
            "fta": _safe_int(_get(r, "FTA")),
            "ft_pct": _safe_decimal(_get(r, "FT_PCT")),
            "oreb": _safe_int(_get(r, "OREB")),
            "dreb": _safe_int(_get(r, "DREB")),
            "reb": _safe_int(_get(r, "REB")),
            "ast": _safe_int(_get(r, "AST")),
            "stl": _safe_int(_get(r, "STL")),
            "blk": _safe_int(_get(r, "BLK")),
            "tov": _safe_int(_get(r, "TOV")),
            "pf": _safe_int(_get(r, "PF")),
            "pts": _safe_int(_get(r, "PTS")),
            "plus_minus": _safe_int(_get(r, "PLUS_MINUS")),
            "video_available": _safe_int(_get(r, "VIDEO_AVAILABLE")),
        })
    return records


def ensure_player_stat_stage_table(conn) -> None:
    """Create player_stat_stage if it does not exist."""
    cursor = conn.cursor()
    cursor.execute("""
        IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'player_stat_stage')
        BEGIN
        CREATE TABLE [dbo].[player_stat_stage](
            [season_id] [varchar](10) NOT NULL,
            [player_id] [bigint] NOT NULL,
            [game_id] [varchar](20) NOT NULL,
            [game_date] [varchar](20) NOT NULL,
            [matchup] [varchar](20) NOT NULL,
            [wl] [char](1) NOT NULL,
            [min] [smallint] NOT NULL,
            [fgm] [smallint] NOT NULL,
            [fga] [smallint] NOT NULL,
            [fg_pct] [decimal](5, 3) NULL,
            [fg3m] [smallint] NOT NULL,
            [fg3a] [smallint] NOT NULL,
            [fg3_pct] [decimal](5, 3) NULL,
            [ftm] [smallint] NOT NULL,
            [fta] [smallint] NOT NULL,
            [ft_pct] [decimal](5, 3) NULL,
            [oreb] [smallint] NOT NULL,
            [dreb] [smallint] NOT NULL,
            [reb] [smallint] NOT NULL,
            [ast] [smallint] NOT NULL,
            [stl] [smallint] NOT NULL,
            [blk] [smallint] NOT NULL,
            [tov] [smallint] NOT NULL,
            [pf] [smallint] NOT NULL,
            [pts] [smallint] NOT NULL,
            [plus_minus] [smallint] NOT NULL,
            [video_available] [smallint] NOT NULL,
            CONSTRAINT [UQ_player_stat_stage_player_game] UNIQUE NONCLUSTERED ([player_id], [game_id])
        )
        END
    """)
    conn.commit()


def insert_player_stat_stage(
    records: list[dict],
    server: str = "localhost\\SQLEXPRESS",
    database: str = "Props",
    user: str | None = None,
    password: str | None = None,
    trusted_connection: bool = False,
) -> int:
    """Truncate player_stat_stage and insert records. Returns count."""
    import pyodbc

    user = user or os.environ.get("PROPS_DB_USER", "dbadmin")
    password = password or os.environ.get("PROPS_DB_PASSWORD", "")

    conn = _get_db_conn(server, database, user, password, trusted_connection)
    ensure_player_stat_stage_table(conn)

    cols = [
        "season_id", "player_id", "game_id", "game_date", "matchup", "wl",
        "min", "fgm", "fga", "fg_pct", "fg3m", "fg3a", "fg3_pct",
        "ftm", "fta", "ft_pct", "oreb", "dreb", "reb", "ast", "stl", "blk",
        "tov", "pf", "pts", "plus_minus", "video_available",
    ]
    placeholders = ", ".join("?" * len(cols))

    count = 0
    with conn:
        cursor = conn.cursor()
        cursor.execute("TRUNCATE TABLE [dbo].[player_stat_stage]")
        for r in records:
            vals = []
            for c in cols:
                v = r.get(c)
                # SQL Server rejects NaN/inf for decimal/float; use NULL
                if isinstance(v, float) and (v != v or v == float("inf") or v == float("-inf")):
                    v = None
                vals.append(v)
            cursor.execute(
                f"INSERT INTO [dbo].[player_stat_stage] ({', '.join(cols)}) VALUES ({placeholders})",
                vals,
            )
            count += cursor.rowcount
    conn.close()
    return count


def upsert_player_stat_from_stage(
    server: str = "localhost\\SQLEXPRESS",
    database: str = "Props",
    user: str | None = None,
    password: str | None = None,
    trusted_connection: bool = False,
) -> int:
    """MERGE player_stat_stage into player_stat. Returns rows affected."""
    user = user or os.environ.get("PROPS_DB_USER", "dbadmin")
    password = password or os.environ.get("PROPS_DB_PASSWORD", "")

    conn = _get_db_conn(server, database, user, password, trusted_connection)

    merge_sql = """
        MERGE [dbo].[player_stat] AS t
        USING [dbo].[player_stat_stage] AS s
        ON t.player_id = s.player_id AND t.game_id = s.game_id
        WHEN MATCHED THEN UPDATE SET
            t.season_id = s.season_id,
            t.game_date = CAST(s.game_date AS date),
            t.matchup = s.matchup,
            t.wl = s.wl,
            t.[min] = s.[min],
            t.fgm = s.fgm,
            t.fga = s.fga,
            t.fg_pct = s.fg_pct,
            t.fg3m = s.fg3m,
            t.fg3a = s.fg3a,
            t.fg3_pct = s.fg3_pct,
            t.ftm = s.ftm,
            t.fta = s.fta,
            t.ft_pct = s.ft_pct,
            t.oreb = s.oreb,
            t.dreb = s.dreb,
            t.reb = s.reb,
            t.ast = s.ast,
            t.stl = s.stl,
            t.blk = s.blk,
            t.tov = s.tov,
            t.pf = s.pf,
            t.pts = s.pts,
            t.plus_minus = s.plus_minus,
            t.video_available = s.video_available
        WHEN NOT MATCHED BY TARGET THEN INSERT (
            season_id, player_id, game_id, game_date, matchup, wl,
            [min], fgm, fga, fg_pct, fg3m, fg3a, fg3_pct,
            ftm, fta, ft_pct, oreb, dreb, reb, ast, stl, blk,
            tov, pf, pts, plus_minus, video_available
        ) VALUES (
            s.season_id, s.player_id, s.game_id, CAST(s.game_date AS date), s.matchup, s.wl,
            s.[min], s.fgm, s.fga, s.fg_pct, s.fg3m, s.fg3a, s.fg3_pct,
            s.ftm, s.fta, s.ft_pct, s.oreb, s.dreb, s.reb, s.ast, s.stl, s.blk,
            s.tov, s.pf, s.pts, s.plus_minus, s.video_available
        );
    """
    with conn:
        cursor = conn.cursor()
        cursor.execute(merge_sql)
        count = cursor.rowcount
    conn.close()
    return count


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Scrape NBA player gamelogs (2026-02-13 onward, 3-day lookback)"
    )
    parser.add_argument(
        "--player-ids",
        metavar="ID",
        help="Comma-separated player IDs (default: all players for season)",
    )
    parser.add_argument(
        "--all-players",
        action="store_true",
        help="Fetch all players for season (default when no --player-ids)",
    )
    parser.add_argument(
        "--season",
        default="2025-26",
        help="Season (default: 2025-26)",
    )
    parser.add_argument(
        "--db",
        action="store_true",
        help="Stage and upsert to player_stat via player_stat_stage",
    )
    parser.add_argument(
        "--db-server",
        default="localhost\\SQLEXPRESS",
        help="SQL Server instance",
    )
    parser.add_argument(
        "--db-user",
        default=os.environ.get("PROPS_DB_USER", "dbadmin"),
        help="DB user",
    )
    parser.add_argument(
        "--db-password",
        default=os.environ.get("PROPS_DB_PASSWORD", ""),
        help="DB password (or set PROPS_DB_PASSWORD; use --trusted-connection for Windows auth)",
    )
    parser.add_argument(
        "--trusted-connection",
        action="store_true",
        help="Use Windows Authentication (no user/password)",
    )
    parser.add_argument(
        "-o", "--output",
        help="Output file (CSV or JSON)",
    )
    parser.add_argument(
        "--date-from",
        metavar="YYYY-MM-DD",
        help="Start date for game data (default: 2 days ago)",
    )
    parser.add_argument(
        "--date-to",
        metavar="YYYY-MM-DD",
        help="End date for game data (default: today)",
    )
    args = parser.parse_args()

    if args.date_from and args.date_to:
        date_from, date_to = args.date_from, args.date_to
    elif args.date_from:
        date_from = args.date_from
        date_to = args.date_to or datetime.now().date().strftime("%Y-%m-%d")
    elif args.date_to:
        date_from = args.date_from or _get_date_range()[0]
        date_to = args.date_to
    else:
        date_from, date_to = _get_date_range()
    print(f"Date range: {date_from} to {date_to} (season {args.season})")

    if args.player_ids:
        player_ids = [int(x.strip()) for x in args.player_ids.split(",") if x.strip()]
        print(f"Players: {len(player_ids)} specified")
    else:
        player_ids = get_player_ids(args.season)
        print(f"Players: {len(player_ids)} (all for season)")

    # Prefer bulk LeagueGameLog (1–2 API calls); fall back to per-player (many calls)
    print("Fetching gamelogs (trying bulk LeagueGameLog first)...")
    raw = fetch_gamelogs_league(args.season, date_from, date_to)
    if raw:
        print(f"  Got {len(raw)} rows from LeagueGameLog (bulk)")
    else:
        if not player_ids:
            print("No players to fetch.")
            return
        print("  LeagueGameLog unavailable or no player id; using per-player fetch...")
        raw = fetch_gamelogs(player_ids, args.season, date_from, date_to)
    records = parse_to_stage_records(raw)
    print(f"Parsed {len(records)} gamelog records")

    if not records:
        print("No records to process.")
        return

    if args.output:
        import json
        path = args.output
        if path.lower().endswith(".json"):
            with open(path, "w") as f:
                json.dump(records, f, indent=2)
        else:
            import pandas as pd
            pd.DataFrame(records).to_csv(path, index=False)
        print(f"Wrote {path}")

    if args.db:
        trusted = getattr(args, "trusted_connection", False) or os.environ.get("PROPS_DB_USE_TRUSTED_CONNECTION", "").strip().lower() in ("1", "true", "yes")
        n = insert_player_stat_stage(
            records,
            server=args.db_server,
            database="Props",
            user=args.db_user,
            password=args.db_password,
            trusted_connection=trusted,
        )
        print(f"Staged {n} rows to player_stat_stage")
        merge_count = upsert_player_stat_from_stage(
            server=args.db_server,
            database="Props",
            user=args.db_user,
            password=args.db_password,
            trusted_connection=trusted,
        )
        print(f"Upserted {merge_count} rows to player_stat")


if __name__ == "__main__":
    main()
