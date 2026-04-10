"""
Find PrizePicks NBA projections (standard odds) where the player has exceeded
the line in each of their last 5 games for that stat type.

Links PrizePicks players to NBA player_stat by name: prizepicks_player.display_name
is matched to a [player] table (full_name or first_name + last_name) if present,
otherwise to nba_api CommonAllPlayers DISPLAY_FIRST_LAST.
"""

import db_config  # noqa: F401 - load .env from repo root before DB
import argparse
import csv
import logging
import os
import re
from collections import defaultdict

logger = logging.getLogger(__name__)

# Cache for resolve_nba_player_ids: (use_api_fallback, season) -> name -> nba_id dict. Invalidated on process restart.
_nba_id_cache: dict[tuple[bool, str], dict[str, int]] = {}

# Cache for materialized last-n table: True if [dbo].[player_stat_last_n] exists, False otherwise, None = not checked yet.
_player_stat_last_n_exists: bool | None = None

# PrizePicks stat_type_name -> player_stat column name(s). Single str = one column; tuple = sum of columns.
STAT_COLUMN_MAP = {
    "Points": "pts",
    "Rebounds": "reb",
    "Assists": "ast",
    "Steals": "stl",
    "Blocks": "blk",
    "Blocked Shots": "blk",
    "Turnovers": "tov",
    "Defensive Rebounds": "dreb",
    "Offensive Rebounds": "oreb",
    "3 Pointers Made": "fg3m",
    "3 Pointers": "fg3m",
    "3-PT Made": "fg3m",
    "3-PT Attempted": "fg3a",
    "FG Made": "fgm",
    "FG Attempted": "fga",
    "Personal Fouls": "pf",
    "Blks+Stls": ("blk", "stl"),
    "Free Throws Made": "ftm",
    "Free Throws Attempted": "fta",
    "Pts+Asts": ("pts", "ast"),
    "Pts+Rebs": ("pts", "reb"),
    "Pts+Rebs+Asts": ("pts", "reb", "ast"),
    "Rebs+Asts": ("reb", "ast"),
    "Two Pointers Made": ("fgm", "fg3m", "sub"),      # fgm - fg3m
    "Two Pointers Attempted": ("fga", "fg3a", "sub"),  # fga - fg3a
}


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


def _get_db_conn_trusted(server: str, database: str):
    """Connect using Windows Authentication (Trusted_Connection=yes)."""
    import pyodbc
    conn_str = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={server};DATABASE={database};Trusted_Connection=yes;"
    )
    try:
        return pyodbc.connect(conn_str)
    except pyodbc.Error:
        conn_str = (
            f"DRIVER={{SQL Server}};"
            f"SERVER={server};DATABASE={database};Trusted_Connection=yes;"
        )
        return pyodbc.connect(conn_str)


def get_credentials_from_table(
    server: str,
    database: str,
    config_table: str,
) -> tuple[str, str, str, str]:
    """Read server, database, user, password from a config table using Windows auth.
    Table can have columns: server, database_name/database, username/user, password/pwd.
    Returns (server, database, user, password). Uses first row.
    """
    if not re.match(r"^[a-zA-Z0-9_.\[\]]+$", config_table):
        raise ValueError("config_table must be a single table name (e.g. dbo.db_config)")
    conn = _get_db_conn_trusted(server, database)
    cursor = conn.cursor()
    cursor.execute(f"SELECT TOP 1 * FROM {config_table}")
    row = cursor.fetchone()
    if not row:
        conn.close()
        raise ValueError(f"Config table {config_table} is empty")
    cols = [c[0].lower() for c in cursor.description]
    d = dict(zip(cols, row))
    conn.close()
    # Map common column names
    server_val = d.get("server") or d.get("server_name") or ""
    db_val = d.get("database_name") or d.get("database") or ""
    user_val = d.get("username") or d.get("user") or d.get("user_name") or ""
    pwd_val = d.get("password") or d.get("pwd") or ""
    return (str(server_val), str(db_val), str(user_val), str(pwd_val))


def _normalize_name(s: str) -> str:
    if not s:
        return ""
    return " ".join(re.split(r"\s+", s.strip()))


def get_projections_standard(conn, league_id: int | list[int] = 7) -> list[dict]:
    """Return standard, single-player projections. See get_projections with odds_type='standard'."""
    return get_projections(conn, league_id=league_id, odds_type="standard")


def get_projections(
    conn,
    league_id: int | list[int] | None = None,
    odds_type: str | None = None,
    active_only: bool = False,
) -> list[dict]:
    """Return single-player projections with PrizePicks display_name.
    If league_id is None, return all leagues. If odds_type is None, return all odds types.
    league_id can be a single int or a list of ints (multiple sports).
    If active_only is True, only return projections where start_time >= current time (not yet started).
    """
    league_ids = None if league_id is None else ([league_id] if isinstance(league_id, int) else league_id)
    if league_ids is not None and not league_ids:
        return []
    # start_time is stored in Central; use Central for date window and "not passed" check
    now_central = "CAST(GETUTCDATE() AT TIME ZONE 'UTC' AT TIME ZONE 'Central Standard Time' AS datetime2(0))"
    where_parts = [
        "p.player_id IS NOT NULL",
        # Exclude most generic combo markets, but keep Blks+Stls (PrizePicks sometimes labels it as a combo)
        "((p.stat_type_name NOT LIKE N'%(Combo)%' AND p.stat_type_name NOT LIKE N'%Combo%') OR p.stat_type_name LIKE N'Blks+Stls%')",
        f"p.start_time >= DATEADD(day, -30, CAST({now_central} AS DATE))",
    ]
    params = []
    if active_only:
        where_parts.append(f"p.start_time >= {now_central}")
    if odds_type is not None:
        where_parts.append("p.odds_type = ?")
        params.append(odds_type)
    if league_ids:
        placeholders = ",".join("?" * len(league_ids))
        where_parts.append(f"p.league_id IN ({placeholders})")
        params.extend(league_ids)
    where_sql = " AND ".join(where_parts)
    # Providers don't always use the exact same stat label strings as PrizePicks.
    # Normalize a small set of known-equivalent labels for joins (especially Blocks/Blocked Shots).
    stat_join_expr_p = """
        CASE
            WHEN LTRIM(RTRIM(p.stat_type_name)) IN (N'Blocks', N'Blocked Shots') THEN N'Blocks__Blocked_Shots'
            WHEN LTRIM(RTRIM(p.stat_type_name)) IN (N'Double Doubles', N'Double-Doubles', N'Double-Double') THEN N'Double_Doubles'
            WHEN LTRIM(RTRIM(p.stat_type_name)) IN (N'Triple Doubles', N'Triple-Doubles', N'Triple-Double') THEN N'Triple_Doubles'
            WHEN LTRIM(RTRIM(p.stat_type_name)) LIKE N'Blks+Stls%' THEN N'Blks_Stls'
            WHEN LTRIM(RTRIM(p.stat_type_name)) IN (N'3-PT Attempted', N'3 Pointers Attempted', N'3s Attempted') THEN N'FG3A'
            WHEN LTRIM(RTRIM(p.stat_type_name)) IN (N'3 Pointers', N'3 Pointers Made', N'3-PT Made', N'3-Pointers Made') THEN N'FG3M'
            WHEN LTRIM(RTRIM(p.stat_type_name)) IN (N'Hits+Runs+RBIs', N'Hits + Runs + RBIs') THEN N'Hits+Runs+RBIs'
            WHEN LTRIM(RTRIM(p.stat_type_name)) IN (N'Shots On Target', N'Shots on Target') THEN N'Shots On Target'
            WHEN LTRIM(RTRIM(p.stat_type_name)) IN (N'Goal + Assist', N'Goals + Assists') THEN N'Goal + Assist'
            WHEN LTRIM(RTRIM(p.stat_type_name)) IN (N'Passes Attempted', N'Passes') THEN N'Passes Attempted'
            ELSE LTRIM(RTRIM(p.stat_type_name))
        END
    """
    stat_join_expr_ud0 = """
        CASE
            WHEN LTRIM(RTRIM(ud0.stat_type_name)) IN (N'Blocks', N'Blocked Shots') THEN N'Blocks__Blocked_Shots'
            WHEN LTRIM(RTRIM(ud0.stat_type_name)) IN (N'Double Doubles', N'Double-Doubles', N'Double-Double') THEN N'Double_Doubles'
            WHEN LTRIM(RTRIM(ud0.stat_type_name)) IN (N'Triple Doubles', N'Triple-Doubles', N'Triple-Double') THEN N'Triple_Doubles'
            WHEN LTRIM(RTRIM(ud0.stat_type_name)) IN (N'Blks+Stls', N'Blocks + Steals', N'Blocks+Steals') THEN N'Blks_Stls'
            WHEN LTRIM(RTRIM(ud0.stat_type_name)) IN (N'3-PT Attempted', N'3 Pointers Attempted', N'3s Attempted') THEN N'FG3A'
            WHEN LTRIM(RTRIM(ud0.stat_type_name)) IN (N'3 Pointers', N'3 Pointers Made', N'3-PT Made', N'3-Pointers Made') THEN N'FG3M'
            WHEN LTRIM(RTRIM(ud0.stat_type_name)) IN (N'Hits+Runs+RBIs', N'Hits + Runs + RBIs') THEN N'Hits+Runs+RBIs'
            WHEN LTRIM(RTRIM(ud0.stat_type_name)) IN (N'Shots On Target', N'Shots on Target') THEN N'Shots On Target'
            WHEN LTRIM(RTRIM(ud0.stat_type_name)) IN (N'Goal + Assist', N'Goals + Assists') THEN N'Goal + Assist'
            WHEN LTRIM(RTRIM(ud0.stat_type_name)) IN (N'Passes Attempted', N'Passes') THEN N'Passes Attempted'
            ELSE LTRIM(RTRIM(ud0.stat_type_name))
        END
    """
    stat_join_expr_ud1 = """
        CASE
            WHEN LTRIM(RTRIM(ud1.stat_type_name)) IN (N'Blocks', N'Blocked Shots') THEN N'Blocks__Blocked_Shots'
            WHEN LTRIM(RTRIM(ud1.stat_type_name)) IN (N'Double Doubles', N'Double-Doubles', N'Double-Double') THEN N'Double_Doubles'
            WHEN LTRIM(RTRIM(ud1.stat_type_name)) IN (N'Triple Doubles', N'Triple-Doubles', N'Triple-Double') THEN N'Triple_Doubles'
            WHEN LTRIM(RTRIM(ud1.stat_type_name)) IN (N'Blks+Stls', N'Blocks + Steals', N'Blocks+Steals') THEN N'Blks_Stls'
            WHEN LTRIM(RTRIM(ud1.stat_type_name)) IN (N'3-PT Attempted', N'3 Pointers Attempted', N'3s Attempted') THEN N'FG3A'
            WHEN LTRIM(RTRIM(ud1.stat_type_name)) IN (N'3 Pointers', N'3 Pointers Made', N'3-PT Made', N'3-Pointers Made') THEN N'FG3M'
            WHEN LTRIM(RTRIM(ud1.stat_type_name)) IN (N'Hits+Runs+RBIs', N'Hits + Runs + RBIs') THEN N'Hits+Runs+RBIs'
            WHEN LTRIM(RTRIM(ud1.stat_type_name)) IN (N'Shots On Target', N'Shots on Target') THEN N'Shots On Target'
            WHEN LTRIM(RTRIM(ud1.stat_type_name)) IN (N'Goal + Assist', N'Goals + Assists') THEN N'Goal + Assist'
            WHEN LTRIM(RTRIM(ud1.stat_type_name)) IN (N'Passes Attempted', N'Passes') THEN N'Passes Attempted'
            ELSE LTRIM(RTRIM(ud1.stat_type_name))
        END
    """
    # Pre-aggregate Underdog lines once per (player key, game date, stat) instead of correlated OUTER APPLY
    # (avoids O(n) repeated scans of underdog_projection; TOP 1 tie-break → ORDER BY line_score).
    sql = f"""
        WITH ud_mapped_raw AS (
            SELECT
                up.underdog_player_id AS ud_player_id,
                CONVERT(date, ud0.start_time) AS game_date,
                {stat_join_expr_ud0} AS stat_norm,
                ud0.line_score,
                ROW_NUMBER() OVER (
                    PARTITION BY up.underdog_player_id, CONVERT(date, ud0.start_time), ({stat_join_expr_ud0})
                    ORDER BY ud0.line_score
                ) AS rn
            FROM [dbo].[underdog_projection] ud0
            INNER JOIN [dbo].[underdog_appearance] ua ON ua.id = ud0.appearance_id
            INNER JOIN [dbo].[underdog_player] up ON up.id = ua.player_id
        ),
        ud_mapped AS (
            SELECT ud_player_id, game_date, stat_norm, line_score
            FROM ud_mapped_raw
            WHERE rn = 1
        ),
        ud_name_raw AS (
            SELECT
                LTRIM(RTRIM(ud1.display_name)) AS display_name,
                CONVERT(date, ud1.start_time) AS game_date,
                {stat_join_expr_ud1} AS stat_norm,
                ud1.line_score,
                ROW_NUMBER() OVER (
                    PARTITION BY LTRIM(RTRIM(ud1.display_name)), CONVERT(date, ud1.start_time), ({stat_join_expr_ud1})
                    ORDER BY ud1.line_score
                ) AS rn
            FROM [dbo].[underdog_projection] ud1
        ),
        ud_by_name AS (
            SELECT display_name, game_date, stat_norm, line_score
            FROM ud_name_raw
            WHERE rn = 1
        )
        SELECT
            p.projection_id,
            p.player_id AS pp_player_id,
            p.league_id,
            p.line_score,
            p.stat_type_name,
            p.odds_type,
            p.description,
            CONVERT(NVARCHAR(50), p.start_time, 127) AS start_time,
            pp.display_name,
            pp.name AS pp_name,
            pp.team,
            pp.team_name,
            pp.position,
            pp.jersey_number,
            pp.league,
            pp.image_url,
            pp.nba_player_id,
            g.away_abbreviation,
            g.home_abbreviation,
            COALESCE(ud.line_score, udf.line_score) AS line_underdog
        FROM [dbo].[prizepicks_projection] p
        INNER JOIN [dbo].[prizepicks_player] pp
            ON pp.player_id = CAST(p.player_id AS NVARCHAR(20))
        LEFT JOIN [dbo].[player] pl
            ON pl.prizepicks_player_id = TRY_CAST(pp.prizepicks_player_id AS INT)
        LEFT JOIN ud_mapped ud
            ON ud.ud_player_id = pl.underdog_player_id
            AND ud.stat_norm = ({stat_join_expr_p})
            AND ud.game_date = CONVERT(date, p.start_time)
        LEFT JOIN ud_by_name udf
            ON udf.display_name = LTRIM(RTRIM(pp.display_name))
            AND udf.stat_norm = ({stat_join_expr_p})
            AND udf.game_date = CONVERT(date, p.start_time)
        LEFT JOIN [dbo].[prizepicks_game] g
            ON g.game_id = CAST(p.game_id AS NVARCHAR(20))
        WHERE {where_sql}
        ORDER BY pp.display_name, p.stat_type_name, p.odds_type, p.line_score
    """
    cursor = conn.cursor()
    cursor.execute(sql, params)
    columns = [c[0] for c in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


def get_nba_player_id_by_name_from_table(conn) -> dict[str, int] | None:
    """If [player] table exists with display_first_last, full_name, or (first_name, last_name), return name -> player_id map."""
    cursor = conn.cursor()
    cursor.execute("""
        SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'dbo' AND TABLE_NAME = 'player'
    """)
    if not cursor.fetchone():
        return None
    # Try display_first_last (matches PrizePicks display_name format)
    try:
        cursor.execute("""
            SELECT player_id, LTRIM(RTRIM(display_first_last)) AS full_name
            FROM [dbo].[player]
            WHERE display_first_last IS NOT NULL AND LTRIM(RTRIM(display_first_last)) <> ''
        """)
        rows = cursor.fetchall()
        if rows:
            return {_normalize_name(r[1]): int(r[0]) for r in rows}
    except Exception:
        pass
    # Try full_name
    try:
        cursor.execute("SELECT player_id, full_name FROM [dbo].[player] WHERE full_name IS NOT NULL AND full_name <> ''")
        rows = cursor.fetchall()
        if rows:
            return {_normalize_name(r[1]): int(r[0]) for r in rows}
    except Exception:
        pass
    # Try first_name + ' ' + last_name
    try:
        cursor.execute("""
            SELECT player_id, LTRIM(RTRIM(ISNULL(first_name,'') + ' ' + ISNULL(last_name,''))) AS full_name
            FROM [dbo].[player]
            WHERE first_name IS NOT NULL OR last_name IS NOT NULL
        """)
        rows = cursor.fetchall()
        if rows:
            return {_normalize_name(r[1]): int(r[0]) for r in rows}
    except Exception:
        pass
    return None


def get_nba_player_id_by_name_from_api(season: str = "2025-26") -> dict[str, int]:
    """Build display_name -> NBA PERSON_ID from nba_api CommonAllPlayers."""
    from nba_api.stats.endpoints import commonallplayers
    players = commonallplayers.CommonAllPlayers(
        season=season,
        is_only_current_season=1,
        timeout=30,
    )
    df = players.get_data_frames()[0]
    if df.empty or "DISPLAY_FIRST_LAST" not in df.columns or "PERSON_ID" not in df.columns:
        return {}
    return {
        _normalize_name(row["DISPLAY_FIRST_LAST"]): int(row["PERSON_ID"])
        for _, row in df.iterrows()
    }


def resolve_nba_player_ids(conn, use_api_fallback: bool = True, season: str = "2025-26") -> dict[str, int]:
    """Name -> NBA player_id: from [player] table if available, else from nba_api.
    Keys are normalized and lowercased for case-insensitive matching.
    Result is cached per (use_api_fallback, season) for the process lifetime.
    """
    cache_key = (use_api_fallback, season)
    if cache_key in _nba_id_cache:
        return _nba_id_cache[cache_key]
    raw: dict[str, int] = {}
    from_table = get_nba_player_id_by_name_from_table(conn)
    if from_table is not None and from_table:
        raw = from_table
    elif use_api_fallback:
        raw = get_nba_player_id_by_name_from_api(season)
    if not raw:
        if not use_api_fallback:
            logger.warning(
                "NBA player ID mapping is empty: [player] table missing or empty and use_api_fallback=False. "
                "Streak enrichment will only work for rows with prizepicks_player.nba_player_id set. "
                "Populate [player] (e.g. display_first_last) or set nba_player_id on prizepicks_player."
            )
        _nba_id_cache[cache_key] = {}
        return {}
    # Allow case-insensitive lookup: store under normalized lowercase key
    result = {_normalize_name(k).lower(): v for k, v in raw.items()}
    _nba_id_cache[cache_key] = result
    return result


def _opponent_from_matchup(matchup: str | None) -> str:
    """Return opponent city abbreviation from NBA matchup (e.g. 'LAL @ MIL' -> 'MIL', 'MIL vs. LAL' -> 'MIL')."""
    if not matchup or not isinstance(matchup, str):
        return ""
    s = matchup.strip()
    if " vs. " in s:
        return s.split(" vs. ")[0].strip()[:3]
    if " @ " in s:
        return s.split(" @ ")[-1].strip()[:3]
    return s[:3] if len(s) >= 3 else s


# All player_stat columns needed for STAT_COLUMN_MAP (single, sum, and sub).
_PLAYER_STAT_COLUMNS = (
    "pts", "reb", "ast", "stl", "blk", "tov", "dreb", "oreb",
    "fg3m", "fg3a", "fgm", "fga", "pf", "ftm", "fta",
)


def get_last_n_games_all_stats(
    conn, nba_player_id: int, n: int = 5
) -> list[dict]:
    """Fetch last n games for a player with all stat columns. Returns list of dicts with game_date, matchup, and stat keys."""
    if n < 1:
        return []
    result = get_last_n_games_all_stats_batch(conn, [nba_player_id], n)
    return result.get(nba_player_id, [])


def _get_last_n_games_from_materialized(
    conn, nba_player_ids: list[int], n: int
) -> dict[int, list[dict]] | None:
    """Read from [dbo].[player_stat_last_n] if it exists. Returns same shape as get_last_n_games_all_stats_batch, or None to fall back."""
    global _player_stat_last_n_exists
    if n < 1 or not nba_player_ids:
        return {} if n >= 1 else None
    ids = list(dict.fromkeys(i for i in nba_player_ids if i is not None))
    if not ids:
        return {}
    if _player_stat_last_n_exists is None:
        try:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = N'dbo' AND TABLE_NAME = N'player_stat_last_n'"
            )
            _player_stat_last_n_exists = cursor.fetchone() is not None
        except Exception:
            _player_stat_last_n_exists = False
    if not _player_stat_last_n_exists:
        return None
    cols = ", ".join(f"[{c}]" for c in _PLAYER_STAT_COLUMNS)
    placeholders = ",".join("?" * len(ids))
    cursor = conn.cursor()
    try:
        cursor.execute(
            f"""
            SELECT player_id, game_date, matchup, {cols}
            FROM [dbo].[player_stat_last_n]
            WHERE player_id IN ({placeholders}) AND rn <= ?
            ORDER BY player_id, rn
            """,
            (*ids, n),
        )
    except Exception:
        return None
    columns = [c[0] for c in cursor.description]
    out: dict[int, list[dict]] = {i: [] for i in ids}
    for row in cursor.fetchall():
        d = dict(zip(columns, row))
        pid = d.pop("player_id", None)
        if pid is not None:
            try:
                pid = int(pid)
            except (TypeError, ValueError):
                pass
            if pid in out:
                out[pid].append(d)
    for pid in out:
        out[pid].sort(key=lambda r: (r.get("game_date") or ""), reverse=True)
    return out


def get_last_n_games_all_stats_batch(
    conn, nba_player_ids: list[int], n: int = 5
) -> dict[int, list[dict]]:
    """Fetch last n games for many players in one query. Uses [dbo].[player_stat_last_n] if present, else live ROW_NUMBER() over player_stat."""
    if n < 1 or not nba_player_ids:
        return {}
    ids = list(dict.fromkeys(i for i in nba_player_ids if i is not None))
    if not ids:
        return {}
    result = _get_last_n_games_from_materialized(conn, ids, n)
    if result is not None:
        return result
    cols = ", ".join(f"[{c}]" for c in _PLAYER_STAT_COLUMNS)
    placeholders = ",".join("?" * len(ids))
    sql = f"""
    WITH ranked AS (
        SELECT player_id, CAST(game_date AS VARCHAR(20)) AS game_date, ISNULL(matchup, '') AS matchup, {cols},
               ROW_NUMBER() OVER (PARTITION BY player_id ORDER BY game_date DESC) AS rn
        FROM [dbo].[player_stat]
        WHERE player_id IN ({placeholders})
    )
    SELECT player_id, game_date, matchup, {cols}
    FROM ranked
    WHERE rn <= ?
    """
    cursor = conn.cursor()
    cursor.execute(sql, (*ids, n))
    columns = [c[0] for c in cursor.description]
    out = {i: [] for i in ids}
    for row in cursor.fetchall():
        d = dict(zip(columns, row))
        pid = d.pop("player_id", None)
        if pid is not None:
            try:
                pid = int(pid)
            except (TypeError, ValueError):
                pass
            if pid in out:
                out[pid].append(d)
    for pid in out:
        out[pid].sort(key=lambda r: (r.get("game_date") or ""), reverse=True)
    return out


def _stat_value_from_row(
    row: dict, stat_column: str | tuple[str, ...]
) -> int:
    """Compute stat value for one game row given stat_column (str, sum tuple, or sub tuple)."""
    allowed = set(_PLAYER_STAT_COLUMNS)
    if isinstance(stat_column, str):
        cols = [stat_column]
        sub = False
    else:
        t = tuple(stat_column)
        if len(t) == 3 and t[2] == "sub":
            cols = [t[0], t[1]]
            sub = True
        else:
            cols = list(t)
            sub = False
    if not cols or any(c not in allowed for c in cols):
        return 0
    if sub and len(cols) == 2:
        a = row.get(cols[0])
        b = row.get(cols[1])
        a = int(a) if a is not None else 0
        b = int(b) if b is not None else 0
        return a - b
    total = 0
    for c in cols:
        v = row.get(c)
        total += int(v) if v is not None else 0
    return total


def last_n_from_batch_rows(
    rows: list[dict], stat_column: str | tuple[str, ...]
) -> list[tuple[str, int, str]]:
    """Convert batch game rows to (game_date, stat_value, opponent_abbrev) list. Same shape as get_last_n_stat_values."""
    allowed = set(_PLAYER_STAT_COLUMNS)
    if isinstance(stat_column, str):
        cols = [stat_column]
        sub = False
    else:
        t = tuple(stat_column)
        if len(t) == 3 and t[2] == "sub":
            cols = [t[0], t[1]]
            sub = True
        else:
            cols = list(t)
            sub = False
    if not cols or any(c not in allowed for c in cols):
        return []
    out = []
    for r in rows:
        game_date = (r.get("game_date") or "").strip()[:20]
        matchup = r.get("matchup") or ""
        opp = _opponent_from_matchup(matchup)
        val = _stat_value_from_row(r, stat_column)
        out.append((game_date, val, opp))
    return out


def get_last_n_stat_values(
    conn, nba_player_id: int, stat_column: str | tuple[str, ...], n: int = 5
) -> list[tuple[str, int, str]]:
    """Return (game_date, stat_value, opponent_abbrev) for the player's last n games, most recent first.
    stat_column may be a single column name; a tuple of columns (summed per game); or (col1, col2, 'sub') for col1 - col2.
    """
    if n < 1:
        return []
    allowed = ("pts", "reb", "ast", "stl", "blk", "tov", "dreb", "oreb", "fg3m", "fg3a", "fgm", "fga", "pf", "ftm", "fta")
    if isinstance(stat_column, str):
        cols = [stat_column]
        sub = False
    else:
        t = tuple(stat_column)
        if len(t) == 3 and t[2] == "sub":
            cols = [t[0], t[1]]
            sub = True
        else:
            cols = list(t)
            sub = False
    if not cols or any(c not in allowed for c in cols):
        return []
    if sub and len(cols) == 2:
        expr = f"[{cols[0]}] - [{cols[1]}]"
    elif len(cols) == 1:
        expr = f"[{cols[0]}]"
    else:
        expr = " + ".join(f"[{c}]" for c in cols)
    sql = f"""
    SELECT TOP (?) CAST(game_date AS VARCHAR(20)) AS game_date, ({expr}) AS stat_value, ISNULL(matchup, '') AS matchup
    FROM [dbo].[player_stat]
    WHERE player_id = ?
    ORDER BY game_date DESC
    """
    cursor = conn.cursor()
    cursor.execute(sql, (n, nba_player_id))
    return [(r[0], int(r[1]), _opponent_from_matchup(r[2])) for r in cursor.fetchall()]


def get_last_five_stat_values(conn, nba_player_id: int, stat_column: str) -> list[tuple[str, int, str]]:
    """Return (game_date, stat_value, opponent_abbrev) for the player's last 5 games, most recent first."""
    return get_last_n_stat_values(conn, nba_player_id, stat_column, 5)


def get_historical_projection_lines(
    conn,
    pp_player_id: str | None,
    stat_type_name: str,
    game_dates: list[str],
) -> list[float | None]:
    """
    Return PrizePicks line_score for each game_date from prizepicks_projection_history.
    pp_player_id is the PrizePicks player_id (string). game_dates are date strings (e.g. '2026-03-03').
    Returns one value per game_date in the same order; None where no history row exists.
    Prefers odds_type = 'standard' when multiple rows exist for the same date.
    """
    if not pp_player_id or not stat_type_name or not game_dates:
        return [None] * len(game_dates)
    # Normalize dates to yyyy-mm-dd for comparison
    date_set = set()
    for d in game_dates:
        if d:
            dstr = str(d).strip()[:10]
            if len(dstr) >= 10:
                date_set.add(dstr)
    if not date_set:
        return [None] * len(game_dates)
    placeholders = ",".join("?" * len(date_set))
    sql = f"""
    SELECT CONVERT(VARCHAR(10), start_time, 120) AS game_date, odds_type, line_score
    FROM [dbo].[prizepicks_projection_history]
    WHERE CAST(player_id AS NVARCHAR(20)) = ?
      AND LTRIM(RTRIM(stat_type_name)) = ?
      AND CONVERT(VARCHAR(10), start_time, 120) IN ({placeholders})
    """
    cursor = conn.cursor()
    cursor.execute(sql, (str(pp_player_id).strip(), stat_type_name.strip(), *sorted(date_set)))
    # Per date: prefer 'standard' odds_type, else first row
    date_to_line: dict[str, float] = {}
    for row in cursor.fetchall():
        d, odds, line = (row[0] or "").strip()[:10], (row[1] or "").strip().lower(), row[2]
        if not d or line is None:
            continue
        try:
            line_f = float(line)
        except (TypeError, ValueError):
            continue
        if d not in date_to_line or odds == "standard":
            date_to_line[d] = line_f
    # Return in same order as game_dates (normalize each to yyyy-mm-dd for lookup)
    out = []
    for d in game_dates:
        dstr = (str(d).strip()[:10] if d else "") or ""
        out.append(date_to_line.get(dstr) if len(dstr) >= 10 else None)
    return out


def get_historical_projection_lines_batch(
    conn,
    pp_player_id: str,
    stat_type_names: list[str],
    game_dates: list[str],
) -> dict[tuple[str, str], float]:
    """
    Fetch historical line_score for one player, multiple stat types and dates, in one query.
    Returns dict (stat_type_name_normalized, game_date_yyyy_mm_dd) -> line_score.
    Prefers odds_type = 'standard' when multiple rows exist.
    """
    if not pp_player_id or not stat_type_names or not game_dates:
        return {}
    date_set = set()
    for d in game_dates:
        if d:
            dstr = str(d).strip()[:10]
            if len(dstr) >= 10:
                date_set.add(dstr)
    if not date_set:
        return {}
    stat_set = {s.strip() for s in stat_type_names if s and s.strip()}
    if not stat_set:
        return {}
    placeholders_dates = ",".join("?" * len(date_set))
    placeholders_stats = ",".join("?" * len(stat_set))
    sql = f"""
    SELECT CONVERT(VARCHAR(10), start_time, 120) AS game_date, LTRIM(RTRIM(stat_type_name)) AS stat_type_name, odds_type, line_score
    FROM [dbo].[prizepicks_projection_history]
    WHERE CAST(player_id AS NVARCHAR(20)) = ?
      AND LTRIM(RTRIM(stat_type_name)) IN ({placeholders_stats})
      AND CONVERT(VARCHAR(10), start_time, 120) IN ({placeholders_dates})
    """
    cursor = conn.cursor()
    cursor.execute(
        sql,
        (str(pp_player_id).strip(), *sorted(stat_set), *sorted(date_set)),
    )
    key_to_line: dict[tuple[str, str], float] = {}
    for row in cursor.fetchall():
        d, stat, odds, line = (row[0] or "").strip()[:10], (row[1] or "").strip(), (row[2] or "").strip().lower(), row[3]
        if not d or line is None:
            continue
        try:
            line_f = float(line)
        except (TypeError, ValueError):
            continue
        key = (stat, d)
        if key not in key_to_line or odds == "standard":
            key_to_line[key] = line_f
    return key_to_line


def _risk_from_cushion(cushion: float) -> str:
    """Return Low, Medium, or High based on cushion (min of last N minus line)."""
    if cushion >= 3:
        return "Low"
    if cushion >= 1:
        return "Medium"
    return "High"


def enrich_projections_with_streak(
    conn,
    projections: list[dict],
    name_to_nba_id: dict[str, int],
    streak_games: int = 5,
) -> list[dict]:
    """
    Enrich each projection with favored, risk, cushion, last_n_values, last_n_dates, last_n_opponents.
    Uses batched DB queries: one fetch for last N games (all stats) per nba_id, one for historical lines per pp_player_id.
    Does not close conn. Returns new list of dicts with added keys.
    """
    if streak_games < 1:
        streak_games = 5
    # Resolve nba_id for each projection and group by nba_id for batch fetch
    nba_id_to_batch_rows: dict[int, list[dict]] = {}
    proj_meta: list[tuple[dict, int | None, str, str, str | tuple]] = []

    for proj in projections:
        row = dict(proj)
        stat_type = (proj.get("stat_type_name") or "").strip()
        stat_col = STAT_COLUMN_MAP.get(stat_type)
        display_name = (proj.get("display_name") or proj.get("pp_name") or "").strip()
        line = proj.get("line_score")
        if not stat_col or not display_name or " + " in display_name or line is None:
            row["favored"] = False
            row["risk"] = None
            row["cushion"] = None
            row["last_n_values"] = []
            row["last_n_dates"] = []
            row["last_n_opponents"] = []
            row["last_n_projection_lines"] = []
            row["streak_games"] = streak_games
            proj_meta.append((row, None, stat_type, "", stat_col))
            continue
        try:
            line_val = float(line)
        except (TypeError, ValueError):
            row["favored"] = False
            row["risk"] = None
            row["cushion"] = None
            row["last_n_values"] = []
            row["last_n_dates"] = []
            row["last_n_opponents"] = []
            row["last_n_projection_lines"] = []
            row["streak_games"] = streak_games
            proj_meta.append((row, None, stat_type, "", stat_col))
            continue
        nba_id = proj.get("nba_player_id")
        if nba_id is not None:
            try:
                nba_id = int(nba_id)
            except (TypeError, ValueError):
                nba_id = None
        if nba_id is None:
            nba_id = name_to_nba_id.get(_normalize_name(display_name).lower())
        if nba_id is None:
            row["favored"] = False
            row["risk"] = None
            row["cushion"] = None
            row["last_n_values"] = []
            row["last_n_dates"] = []
            row["last_n_opponents"] = []
            row["last_n_projection_lines"] = []
            row["streak_games"] = streak_games
        _raw = proj.get("pp_player_id")
        pp_player_id = str(_raw).strip() if _raw is not None else ""
        proj_meta.append((row, nba_id, stat_type, pp_player_id, stat_col))

    # (pp_player_id, stat_type, date) -> line_score for historical lookup (keys per player so no collision)
    hist_batch_cache: dict[tuple[str, str, str], float] = {}
    # Batch fetch last N games for all players in one query
    nba_ids_to_fetch = {m[1] for m in proj_meta if m[1] is not None}
    try:
        batch_result = get_last_n_games_all_stats_batch(conn, list(nba_ids_to_fetch), streak_games)
        nba_id_to_batch_rows.update(batch_result)
    except Exception:
        pass
    # Collect dates and pp_player_ids per nba_id, then batch fetch historical lines
    for nba_id in nba_ids_to_fetch:
        batch_rows = nba_id_to_batch_rows.get(nba_id, [])
        pp_player_ids: dict[str, set[str]] = {}
        dates_for_hist: set[str] = set()
        for (row, pid, stat_type, pp_pid, stat_col) in proj_meta:
            if pid != nba_id or not stat_type or not stat_col:
                continue
            dates_for_hist.update(
                (r.get("game_date") or "").strip()[:10]
                for r in batch_rows
                if (r.get("game_date") or "").strip()[:10]
            )
            if pp_pid:
                pp_player_ids.setdefault(pp_pid, set()).add(stat_type)
        for pp_pid, stat_types in pp_player_ids.items():
            try:
                hist_map = get_historical_projection_lines_batch(
                    conn, pp_pid, list(stat_types), list(dates_for_hist)
                )
            except Exception:
                hist_map = {}
            for (stat_type_key, date_key), line_val in hist_map.items():
                hist_batch_cache[(pp_pid, stat_type_key, date_key)] = line_val

    # Build output: for each projection, derive last_n and hist_lines from batch data
    out = []
    for (row, nba_id, stat_type, pp_player_id, stat_col) in proj_meta:
        if row.get("last_n_values") is not None:
            out.append(row)
            continue
        if nba_id is None or stat_col is None or not stat_type:
            out.append(row)
            continue  # already have empty streak set for nba_id is None
        batch_rows = nba_id_to_batch_rows.get(nba_id, [])
        last_n = last_n_from_batch_rows(batch_rows, stat_col)
        dates = [d for d, _, _ in last_n]
        def _hist_for_date(d: str) -> float | None:
            dstr = (str(d).strip()[:10] if d else "") or ""
            if len(dstr) < 10:
                return None
            return hist_batch_cache.get((pp_player_id, stat_type.strip(), dstr))
        hist_lines = [_hist_for_date(d) for d in dates]
        line_val = None
        try:
            line_val = float(row.get("line_score"))
        except (TypeError, ValueError):
            pass
        if line_val is None:
            row["favored"] = False
            row["risk"] = None
            row["cushion"] = None
            row["last_n_values"] = [v for _, v, _ in last_n]
            row["last_n_dates"] = dates
            row["last_n_opponents"] = [opp for _, _, opp in last_n]
            row["last_n_projection_lines"] = hist_lines[: len(last_n)]
            row["streak_games"] = streak_games
            out.append(row)
            continue
        if len(last_n) < streak_games:
            row["favored"] = False
            row["risk"] = None
            row["cushion"] = None
            row["last_n_values"] = [v for _, v, _ in last_n]
            row["last_n_dates"] = dates
            row["last_n_opponents"] = [opp for _, _, opp in last_n]
            row["last_n_projection_lines"] = hist_lines[: len(last_n)]
            row["streak_games"] = streak_games
            out.append(row)
            continue
        values = [v for _, v, _ in last_n]
        opponents = [opp for _, _, opp in last_n]
        all_over = all(v > line_val for v in values)
        min_val = min(values)
        cushion = min_val - line_val
        row["favored"] = all_over
        row["risk"] = _risk_from_cushion(cushion) if all_over else None
        row["cushion"] = round(cushion, 2) if all_over else None
        row["last_n_values"] = values
        row["last_n_dates"] = dates
        row["last_n_opponents"] = opponents
        row["last_n_projection_lines"] = hist_lines
        row["streak_games"] = streak_games
        out.append(row)
    return out


def run(
    server: str = "localhost\\SQLEXPRESS",
    database: str = "Props",
    user: str | None = None,
    password: str | None = None,
    league_id: int = 7,
    use_api_fallback: bool = True,
    season: str = "2025-26",
    output_csv: str | None = None,
    config_table: str | None = None,
    trusted_connection: bool = False,
) -> None:
    # Resolve credentials: config table, or --trusted, or try Windows auth then SQL auth
    if config_table:
        server, database, user, password = get_credentials_from_table(
            server, database, config_table
        )
        conn = _get_db_conn(server, database, user, password)
    elif trusted_connection:
        conn = _get_db_conn_trusted(server, database)
    else:
        user = user or os.environ.get("PROPS_DB_USER", "dbadmin")
        password = password or os.environ.get("PROPS_DB_PASSWORD", "")
        # When no credentials were explicitly set, try Windows Authentication first
        if user == "dbadmin" and password == "":
            try:
                conn = _get_db_conn_trusted(server, database)
            except Exception:
                conn = _get_db_conn(server, database, user, password)
        else:
            conn = _get_db_conn(server, database, user, password)

    # Standard projections with PrizePicks display_name
    projections = get_projections_standard(conn, league_id=league_id)
    if not projections:
        print("No standard single-player projections found.")
        conn.close()
        return

    # Resolve display_name -> NBA player_id (by name)
    name_to_nba_id = resolve_nba_player_ids(conn, use_api_fallback=use_api_fallback, season=season)
    if not name_to_nba_id:
        print("Could not build name -> NBA player_id map (no [player] table and API fallback disabled or failed).")
        conn.close()
        return

    # Find projections where player exceeded line in each of last 5 games
    results = []
    for proj in projections:
        stat_type = (proj.get("stat_type_name") or "").strip()
        stat_col = STAT_COLUMN_MAP.get(stat_type)
        if not stat_col:
            continue
        display_name = (proj.get("display_name") or proj.get("pp_name") or "").strip()
        if not display_name or " + " in display_name:
            continue
        nba_id = name_to_nba_id.get(_normalize_name(display_name).lower())
        if nba_id is None:
            continue
        line = proj.get("line_score")
        if line is None:
            continue
        try:
            line_val = float(line)
        except (TypeError, ValueError):
            continue

        last5 = get_last_five_stat_values(conn, nba_id, stat_col)
        if len(last5) < 5:
            continue
        values = [v for _, v, _ in last5]
        if not all(v > line_val for v in values):
            continue

        results.append({
            "stat_type": stat_type,
            "player_name": display_name,
            "line_score": line_val,
            "last_5_values": values,
            "last_5_dates": [d for d, _, _ in last5],
            "description": proj.get("description"),
            "start_time": proj.get("start_time"),
        })

    conn.close()

    # Output: grouped by stat type
    by_stat = defaultdict(list)
    for r in results:
        by_stat[r["stat_type"]].append(r)

    if not results:
        print("No projections found where the player exceeded the line in each of the last 5 games.")
        return

    for stat_type in sorted(by_stat.keys()):
        rows = by_stat[stat_type]
        print(f"\n--- {stat_type} ({len(rows)} line(s)) ---")
        for r in rows:
            vals = ", ".join(str(v) for v in r["last_5_values"])
            print(f"  {r['player_name']}: line {r['line_score']}  last 5: [{vals}]  (all over)")

    if output_csv:
        with open(output_csv, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["stat_type", "player_name", "line_score", "last_5_values", "last_5_dates", "description", "start_time"])
            for r in results:
                w.writerow([
                    r["stat_type"],
                    r["player_name"],
                    r["line_score"],
                    "|".join(str(v) for v in r["last_5_values"]),
                    "|".join(r["last_5_dates"]),
                    r.get("description"),
                    str(r.get("start_time")),
                ])
        print(f"\nWrote {len(results)} rows to {output_csv}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="PrizePicks standard projections where player exceeded line in each of last 5 games (name-linked to NBA stats)."
    )
    parser.add_argument("--db-server", default="localhost\\SQLEXPRESS", help="SQL Server instance")
    parser.add_argument("--database", default="Props", help="Database name")
    parser.add_argument("--db-user", default=os.environ.get("PROPS_DB_USER", "dbadmin"), help="DB user")
    parser.add_argument("--db-password", default=os.environ.get("PROPS_DB_PASSWORD", ""), help="DB password (quote if it contains ! or other special chars, or use PROPS_DB_PASSWORD env)")
    parser.add_argument("--config-table", metavar="TABLE", help="Load server/database/user/password from this table (uses Windows auth to connect first). e.g. dbo.db_config")
    parser.add_argument("--trusted", action="store_true", dest="trusted_connection", help="Use Windows Authentication (no user/password)")
    parser.add_argument("--league-id", type=int, default=7, help="PrizePicks league_id (7 = NBA)")
    parser.add_argument("--no-api-fallback", action="store_true", help="Do not use nba_api for name resolution if [player] missing")
    parser.add_argument("--season", default="2025-26", help="NBA season for API fallback")
    parser.add_argument("-o", "--output", dest="output_csv", help="Output CSV path")
    args = parser.parse_args()
    run(
        server=args.db_server,
        database=args.database,
        user=args.db_user,
        password=args.db_password,
        league_id=args.league_id,
        use_api_fallback=not args.no_api_fallback,
        season=args.season,
        output_csv=args.output_csv,
        config_table=args.config_table,
        trusted_connection=args.trusted_connection,
    )


if __name__ == "__main__":
    main()
