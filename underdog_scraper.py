"""
Underdog Fantasy projections scraper.
Fetches player prop lines for matching with PrizePicks (same player + stat + game).
Uses httpx first (like PrizePicks); falls back to Playwright on 401/403.
Use --input to load from a JSON file, or --browser to force browser capture.
"""

import db_config  # noqa: F401 - load .env from repo root before DB
import argparse
import json
import os
import re
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import httpx

CHICAGO = ZoneInfo("America/Chicago")


def to_chicago_local(ts) -> str | None:
    """Convert API start_time (epoch ms/s or ISO str) to Chicago local time for datetime2 (no offset)."""
    if ts is None:
        return None
    if isinstance(ts, (int, float)):
        utc_sec = ts / 1000 if ts > 1e12 else ts
        dt = datetime.fromtimestamp(utc_sec, tz=ZoneInfo("UTC")).astimezone(CHICAGO)
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    s = str(ts).strip()
    if not s:
        return None
    try:
        if "T" in s:
            dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        else:
            dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=ZoneInfo("UTC"))
        dt = dt.astimezone(CHICAGO)
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except (ValueError, TypeError):
        return None

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
        st = to_chicago_local(st)
        if st is None:
            continue
        key = (name, stat, st)
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


# Globals to hold the most recent parsed entities from Underdog's over_under_lines API.
_LAST_UNDERDOG_PLAYERS: list[dict] = []
_LAST_UNDERDOG_APPEARANCES: list[dict] = []
_LAST_UNDERDOG_GAMES: list[dict] = []
_LAST_UNDERDOG_SOLO_GAMES: list[dict] = []
_LAST_UNDERDOG_PROJECTIONS: list[dict] = []

# Deduplication: only append to the lists above when id/key not already seen this run.
_SEEN_UNDERDOG_PLAYER_IDS: set = set()
_SEEN_UNDERDOG_APPEARANCE_IDS: set = set()
_SEEN_UNDERDOG_GAME_IDS: set = set()
_SEEN_UNDERDOG_SOLO_GAME_IDS: set = set()
_SEEN_UNDERDOG_PROJECTION_KEYS: set = set()


def _normalize_projection_record_minimal(r: dict) -> dict:
    """Expand a 5-field fallback record to full underdog_projection_stage shape (missing keys -> None)."""
    return {
        "projection_id": r.get("projection_id"),
        "display_name": r.get("display_name"),
        "stat_type_name": r.get("stat_type_name"),
        "line_score": r.get("line_score"),
        "start_time": r.get("start_time"),
        "underdog_player_id": r.get("underdog_player_id"),
        "api_id": None,
        "over_under_id": None,
        "provider_id": None,
        "stat_value": None,
        "line_type": None,
        "status": None,
        "rank": None,
        "sort_by": None,
        "stable_id": None,
        "expires_at": None,
        "updated_at": None,
        "contract_terms_url": None,
        "contract_url": None,
        "live_event": None,
        "live_event_stat": None,
        "non_discounted_stat_value": None,
        "ou_title": None,
        "ou_category": None,
        "ou_display_mode": None,
        "ou_grid_display_title": None,
        "ou_has_alternates": None,
        "ou_option_priority": None,
        "ou_prediction_market": None,
        "ou_scoring_type_id": None,
        "ou_team_divider": None,
        "appearance_stat_id": None,
        "appearance_id": None,
        "display_stat": None,
        "stat": None,
        "graded_by": None,
        "pickem_stat_id": None,
        "appearance_stat_rank": None,
        "options": None,
    }


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
    cols = [
        "projection_id",
        "display_name",
        "stat_type_name",
        "line_score",
        "start_time",
        "underdog_player_id",
        "api_id",
        "over_under_id",
        "provider_id",
        "stat_value",
        "line_type",
        "status",
        "rank",
        "sort_by",
        "stable_id",
        "expires_at",
        "updated_at",
        "contract_terms_url",
        "contract_url",
        "live_event",
        "live_event_stat",
        "non_discounted_stat_value",
        "ou_title",
        "ou_category",
        "ou_display_mode",
        "ou_grid_display_title",
        "ou_has_alternates",
        "ou_option_priority",
        "ou_prediction_market",
        "ou_scoring_type_id",
        "ou_team_divider",
        "appearance_stat_id",
        "appearance_id",
        "display_stat",
        "stat",
        "graded_by",
        "pickem_stat_id",
        "appearance_stat_rank",
        "options",
    ]
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
    """MERGE underdog_projection_stage into underdog_projection, with history. Returns rows merged."""
    import pyodbc
    user = user or os.environ.get("PROPS_DB_USER", "dbadmin")
    password = password or os.environ.get("PROPS_DB_PASSWORD", "")
    conn = _get_db_conn(server, database, user, password, trusted_connection)

    history_sql = """
    INSERT INTO [dbo].[underdog_projection_history] (
        projection_id, display_name, stat_type_name, line_score, start_time,
        underdog_player_id,
        api_id, over_under_id, provider_id, stat_value, line_type, status, rank,
        sort_by, stable_id, expires_at, updated_at, contract_terms_url, contract_url,
        live_event, live_event_stat, non_discounted_stat_value,
        ou_title, ou_category, ou_display_mode, ou_grid_display_title,
        ou_has_alternates, ou_option_priority, ou_prediction_market,
        ou_scoring_type_id, ou_team_divider,
        appearance_stat_id, appearance_id, display_stat, stat, graded_by,
        pickem_stat_id, appearance_stat_rank,
        options,
        created_at, last_modified_at
    )
    SELECT
        p.projection_id,
        p.display_name,
        p.stat_type_name,
        p.line_score,
        p.start_time,
        p.underdog_player_id,
        p.api_id,
        p.over_under_id,
        p.provider_id,
        p.stat_value,
        p.line_type,
        p.status,
        p.rank,
        p.sort_by,
        p.stable_id,
        p.expires_at,
        p.updated_at,
        p.contract_terms_url,
        p.contract_url,
        p.live_event,
        p.live_event_stat,
        p.non_discounted_stat_value,
        p.ou_title,
        p.ou_category,
        p.ou_display_mode,
        p.ou_grid_display_title,
        p.ou_has_alternates,
        p.ou_option_priority,
        p.ou_prediction_market,
        p.ou_scoring_type_id,
        p.ou_team_divider,
        p.appearance_stat_id,
        p.appearance_id,
        p.display_stat,
        p.stat,
        p.graded_by,
        p.pickem_stat_id,
        p.appearance_stat_rank,
        p.options,
        SYSUTCDATETIME() AS created_at,
        p.last_modified_at
    FROM [dbo].[underdog_projection] p
    INNER JOIN [dbo].[underdog_projection_stage] s
        ON p.projection_id = s.projection_id
    WHERE
        (p.line_score <> s.line_score
         OR (p.line_score IS NULL AND s.line_score IS NOT NULL)
         OR (p.line_score IS NOT NULL AND s.line_score IS NULL))
        AND NOT EXISTS (
            SELECT 1
            FROM [dbo].[underdog_projection_history] h
            WHERE h.projection_id = p.projection_id
              AND ((h.line_score = p.line_score)
                   OR (h.line_score IS NULL AND p.line_score IS NULL))
        );
    """

    move_expired_sql = """
    DECLARE @nowChicago datetime2(3) = CONVERT(datetime2(3), SYSUTCDATETIME() AT TIME ZONE 'UTC' AT TIME ZONE 'Central Standard Time');

    INSERT INTO [dbo].[underdog_projection_history] (
        projection_id, display_name, stat_type_name, line_score, start_time,
        underdog_player_id,
        api_id, over_under_id, provider_id, stat_value, line_type, status, rank,
        sort_by, stable_id, expires_at, updated_at, contract_terms_url, contract_url,
        live_event, live_event_stat, non_discounted_stat_value,
        ou_title, ou_category, ou_display_mode, ou_grid_display_title,
        ou_has_alternates, ou_option_priority, ou_prediction_market,
        ou_scoring_type_id, ou_team_divider,
        appearance_stat_id, appearance_id, display_stat, stat, graded_by,
        pickem_stat_id, appearance_stat_rank,
        options,
        created_at, last_modified_at
    )
    SELECT
        p.projection_id,
        p.display_name,
        p.stat_type_name,
        p.line_score,
        p.start_time,
        p.underdog_player_id,
        p.api_id,
        p.over_under_id,
        p.provider_id,
        p.stat_value,
        p.line_type,
        p.status,
        p.rank,
        p.sort_by,
        p.stable_id,
        p.expires_at,
        p.updated_at,
        p.contract_terms_url,
        p.contract_url,
        p.live_event,
        p.live_event_stat,
        p.non_discounted_stat_value,
        p.ou_title,
        p.ou_category,
        p.ou_display_mode,
        p.ou_grid_display_title,
        p.ou_has_alternates,
        p.ou_option_priority,
        p.ou_prediction_market,
        p.ou_scoring_type_id,
        p.ou_team_divider,
        p.appearance_stat_id,
        p.appearance_id,
        p.display_stat,
        p.stat,
        p.graded_by,
        p.pickem_stat_id,
        p.appearance_stat_rank,
        p.options,
        @nowChicago AS created_at,
        p.last_modified_at
    FROM [dbo].[underdog_projection] p
    WHERE p.start_time IS NOT NULL
      AND p.start_time < @nowChicago
      AND NOT EXISTS (
          SELECT 1
          FROM [dbo].[underdog_projection_history] h
          WHERE h.projection_id = p.projection_id
            AND h.start_time = p.start_time
      );

    DELETE FROM [dbo].[underdog_projection]
    WHERE start_time IS NOT NULL
      AND start_time < @nowChicago;
    """

    merge_sql = """
    MERGE [dbo].[underdog_projection] AS t
    USING [dbo].[underdog_projection_stage] AS s
      ON t.projection_id = s.projection_id
    WHEN MATCHED AND (
        ISNULL(t.line_score,-1) <> ISNULL(s.line_score,-1)
        OR ISNULL(t.start_time,'') <> ISNULL(CAST(s.start_time AS nvarchar(50)),'')
    )
      THEN UPDATE SET
        display_name = s.display_name,
        stat_type_name = s.stat_type_name,
        line_score = s.line_score,
        start_time = s.start_time,
        underdog_player_id = s.underdog_player_id,
        api_id = s.api_id,
        over_under_id = s.over_under_id,
        provider_id = s.provider_id,
        stat_value = s.stat_value,
        line_type = s.line_type,
        status = s.status,
        rank = s.rank,
        sort_by = s.sort_by,
        stable_id = s.stable_id,
        expires_at = s.expires_at,
        updated_at = s.updated_at,
        contract_terms_url = s.contract_terms_url,
        contract_url = s.contract_url,
        live_event = s.live_event,
        live_event_stat = s.live_event_stat,
        non_discounted_stat_value = s.non_discounted_stat_value,
        ou_title = s.ou_title,
        ou_category = s.ou_category,
        ou_display_mode = s.ou_display_mode,
        ou_grid_display_title = s.ou_grid_display_title,
        ou_has_alternates = s.ou_has_alternates,
        ou_option_priority = s.ou_option_priority,
        ou_prediction_market = s.ou_prediction_market,
        ou_scoring_type_id = s.ou_scoring_type_id,
        ou_team_divider = s.ou_team_divider,
        appearance_stat_id = s.appearance_stat_id,
        appearance_id = s.appearance_id,
        display_stat = s.display_stat,
        stat = s.stat,
        graded_by = s.graded_by,
        pickem_stat_id = s.pickem_stat_id,
        appearance_stat_rank = s.appearance_stat_rank,
        options = s.options,
        last_modified_at = GETUTCDATE()
    WHEN NOT MATCHED BY TARGET
      THEN INSERT (
        projection_id, display_name, stat_type_name, line_score, start_time,
        underdog_player_id,
        api_id, over_under_id, provider_id, stat_value, line_type,
        status, rank, sort_by, stable_id, expires_at, updated_at,
        contract_terms_url, contract_url, live_event, live_event_stat,
        non_discounted_stat_value, ou_title, ou_category, ou_display_mode,
        ou_grid_display_title, ou_has_alternates, ou_option_priority,
        ou_prediction_market, ou_scoring_type_id, ou_team_divider,
        appearance_stat_id, appearance_id, display_stat, stat, graded_by,
        pickem_stat_id, appearance_stat_rank, options, last_modified_at
      )
      VALUES (
        s.projection_id, s.display_name, s.stat_type_name, s.line_score, s.start_time,
        s.underdog_player_id,
        s.api_id, s.over_under_id, s.provider_id, s.stat_value, s.line_type,
        s.status, s.rank, s.sort_by, s.stable_id, s.expires_at, s.updated_at,
        s.contract_terms_url, s.contract_url, s.live_event, s.live_event_stat,
        s.non_discounted_stat_value, s.ou_title, s.ou_category, s.ou_display_mode,
        s.ou_grid_display_title, s.ou_has_alternates, s.ou_option_priority,
        s.ou_prediction_market, s.ou_scoring_type_id, s.ou_team_divider,
        s.appearance_stat_id, s.appearance_id, s.display_stat, s.stat, s.graded_by,
        s.pickem_stat_id, s.appearance_stat_rank, s.options, GETUTCDATE()
      )
    ;
    """

    with conn:
        cursor = conn.cursor()
        cursor.execute(history_sql)
        cursor.execute(merge_sql)
        merged = cursor.rowcount
        cursor.execute(move_expired_sql)
    conn.close()
    return merged


def upsert_underdog_player_from_stage(
    server: str = "localhost\\SQLEXPRESS",
    database: str = "Props",
    user: str | None = None,
    password: str | None = None,
    trusted_connection: bool = False,
) -> int:
    """MERGE underdog_player_stage into underdog_player. Returns rows affected."""
    import pyodbc

    user = user or os.environ.get("PROPS_DB_USER", "dbadmin")
    password = password or os.environ.get("PROPS_DB_PASSWORD", "")
    conn = _get_db_conn(server, database, user, password, trusted_connection)
    sql = """
    MERGE [dbo].[underdog_player] AS t
    USING [dbo].[underdog_player_stage] AS s
      ON t.id = s.id
    WHEN MATCHED THEN
      UPDATE SET
        t.first_name = s.first_name,
        t.last_name = s.last_name,
        t.position_display_name = s.position_display_name,
        t.position_id = s.position_id,
        t.position_name = s.position_name,
        t.team_id = s.team_id,
        t.sport_id = s.sport_id,
        t.jersey_number = s.jersey_number,
        t.image_url = s.image_url,
        t.dark_image_url = s.dark_image_url,
        t.light_image_url = s.light_image_url,
        t.action_path = s.action_path,
        t.country = s.country,
        t.last_modified_at = GETUTCDATE()
    WHEN NOT MATCHED BY TARGET THEN
      INSERT (
        id, first_name, last_name, position_display_name, position_id,
        position_name, team_id, sport_id, jersey_number, image_url,
        dark_image_url, light_image_url, action_path, country, last_modified_at
      )
      VALUES (
        s.id, s.first_name, s.last_name, s.position_display_name, s.position_id,
        s.position_name, s.team_id, s.sport_id, s.jersey_number, s.image_url,
        s.dark_image_url, s.light_image_url, s.action_path, s.country, GETUTCDATE()
      );
    """
    with conn:
        cursor = conn.cursor()
        cursor.execute(sql)
        count = cursor.rowcount
    conn.close()
    return count


def upsert_underdog_appearance_from_stage(
    server: str = "localhost\\SQLEXPRESS",
    database: str = "Props",
    user: str | None = None,
    password: str | None = None,
    trusted_connection: bool = False,
) -> int:
    """MERGE underdog_appearance_stage into underdog_appearance. Returns rows affected."""
    import pyodbc

    user = user or os.environ.get("PROPS_DB_USER", "dbadmin")
    password = password or os.environ.get("PROPS_DB_PASSWORD", "")
    conn = _get_db_conn(server, database, user, password, trusted_connection)
    sql = """
    MERGE [dbo].[underdog_appearance] AS t
    USING [dbo].[underdog_appearance_stage] AS s
      ON t.id = s.id
    WHEN MATCHED THEN
      UPDATE SET
        t.player_id = s.player_id,
        t.match_id = s.match_id,
        t.match_type = s.match_type,
        t.team_id = s.team_id,
        t.position_id = s.position_id,
        t.lineup_status_id = s.lineup_status_id,
        t.sort_by = s.sort_by,
        t.multiple_picks_allowed = s.multiple_picks_allowed,
        t.type = s.type,
        t.last_modified_at = GETUTCDATE()
    WHEN NOT MATCHED BY TARGET THEN
      INSERT (
        id, player_id, match_id, match_type, team_id,
        position_id, lineup_status_id, sort_by,
        multiple_picks_allowed, type, last_modified_at
      )
      VALUES (
        s.id, s.player_id, s.match_id, s.match_type, s.team_id,
        s.position_id, s.lineup_status_id, s.sort_by,
        s.multiple_picks_allowed, s.type, GETUTCDATE()
      );
    """
    with conn:
        cursor = conn.cursor()
        cursor.execute(sql)
        count = cursor.rowcount
    conn.close()
    return count


def upsert_underdog_game_from_stage(
    server: str = "localhost\\SQLEXPRESS",
    database: str = "Props",
    user: str | None = None,
    password: str | None = None,
    trusted_connection: bool = False,
) -> int:
    """MERGE underdog_game_stage into underdog_game. Returns rows affected."""
    import pyodbc

    user = user or os.environ.get("PROPS_DB_USER", "dbadmin")
    password = password or os.environ.get("PROPS_DB_PASSWORD", "")
    conn = _get_db_conn(server, database, user, password, trusted_connection)
    sql = """
    MERGE [dbo].[underdog_game] AS t
    USING [dbo].[underdog_game_stage] AS s
      ON t.id = s.id
    WHEN MATCHED THEN
      UPDATE SET
        t.scheduled_at = s.scheduled_at,
        t.home_team_id = s.home_team_id,
        t.away_team_id = s.away_team_id,
        t.title = s.title,
        t.short_title = s.short_title,
        t.abbreviated_title = s.abbreviated_title,
        t.full_team_names_title = s.full_team_names_title,
        t.status = s.status,
        t.sport_id = s.sport_id,
        t.type = s.type,
        t.period = s.period,
        t.match_progress = s.match_progress,
        t.away_team_score = s.away_team_score,
        t.home_team_score = s.home_team_score,
        t.rank = s.rank,
        t.year = s.year,
        t.season_type = s.season_type,
        t.updated_at = s.updated_at,
        t.rescheduled_from = s.rescheduled_from,
        t.title_suffix = s.title_suffix,
        t.manually_created = s.manually_created,
        t.pre_game_data = s.pre_game_data,
        t.last_modified_at = GETUTCDATE()
    WHEN NOT MATCHED BY TARGET THEN
      INSERT (
        id, scheduled_at, home_team_id, away_team_id,
        title, short_title, abbreviated_title, full_team_names_title,
        status, sport_id, type, period, match_progress,
        away_team_score, home_team_score, rank, year, season_type,
        updated_at, rescheduled_from, title_suffix, manually_created,
        pre_game_data, last_modified_at
      )
      VALUES (
        s.id, s.scheduled_at, s.home_team_id, s.away_team_id,
        s.title, s.short_title, s.abbreviated_title, s.full_team_names_title,
        s.status, s.sport_id, s.type, s.period, s.match_progress,
        s.away_team_score, s.home_team_score, s.rank, s.year, s.season_type,
        s.updated_at, s.rescheduled_from, s.title_suffix, s.manually_created,
        s.pre_game_data, GETUTCDATE()
      );
    """
    with conn:
        cursor = conn.cursor()
        cursor.execute(sql)
        count = cursor.rowcount
    conn.close()
    return count


def upsert_underdog_solo_game_from_stage(
    server: str = "localhost\\SQLEXPRESS",
    database: str = "Props",
    user: str | None = None,
    password: str | None = None,
    trusted_connection: bool = False,
) -> int:
    """MERGE underdog_solo_game_stage into underdog_solo_game. Returns rows affected."""
    import pyodbc

    user = user or os.environ.get("PROPS_DB_USER", "dbadmin")
    password = password or os.environ.get("PROPS_DB_PASSWORD", "")
    conn = _get_db_conn(server, database, user, password, trusted_connection)
    sql = """
    MERGE [dbo].[underdog_solo_game] AS t
    USING [dbo].[underdog_solo_game_stage] AS s
      ON t.id = s.id
    WHEN MATCHED THEN
      UPDATE SET
        t.scheduled_at = s.scheduled_at,
        t.home_player_id = s.home_player_id,
        t.away_player_id = s.away_player_id,
        t.title = s.title,
        t.short_title = s.short_title,
        t.abbreviated_title = s.abbreviated_title,
        t.full_title = s.full_title,
        t.status = s.status,
        t.sport_id = s.sport_id,
        t.type = s.type,
        t.competition_id = s.competition_id,
        t.rank = s.rank,
        t.period = s.period,
        t.match_progress = s.match_progress,
        t.score = s.score,
        t.updated_at = s.updated_at,
        t.manually_created = s.manually_created,
        t.sport_tournament_round_id = s.sport_tournament_round_id,
        t.pre_game_data = s.pre_game_data,
        t.last_modified_at = GETUTCDATE()
    WHEN NOT MATCHED BY TARGET THEN
      INSERT (
        id, scheduled_at, home_player_id, away_player_id,
        title, short_title, abbreviated_title, full_title,
        status, sport_id, type, competition_id, rank,
        period, match_progress, score, updated_at,
        manually_created, sport_tournament_round_id, pre_game_data,
        last_modified_at
      )
      VALUES (
        s.id, s.scheduled_at, s.home_player_id, s.away_player_id,
        s.title, s.short_title, s.abbreviated_title, s.full_title,
        s.status, s.sport_id, s.type, s.competition_id, s.rank,
        s.period, s.match_progress, s.score, s.updated_at,
        s.manually_created, s.sport_tournament_round_id, s.pre_game_data,
        GETUTCDATE()
      );
    """
    with conn:
        cursor = conn.cursor()
        cursor.execute(sql)
        count = cursor.rowcount
    conn.close()
    return count


OVER_UNDER_LINES_URL = "https://api.underdogfantasy.com/beta/v5/over_under_lines"

DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Accept": "application/json",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.underdogfantasy.com/",
    "Origin": "https://www.underdogfantasy.com",
    "Sec-Ch-Ua": '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
    "Sec-Ch-Ua-Mobile": "?0",
    "Sec-Ch-Ua-Platform": '"Windows"',
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-site",
}


def fetch_over_under_httpx(timeout: float = 30) -> tuple[list[dict], int | None]:
    """
    Fetch over_under_lines from Underdog API via httpx (no browser).
    Returns (records, None) on success, ([], status_code) on 401/403, ([], None) on other error.
    """
    try:
        with httpx.Client(headers=DEFAULT_HEADERS, timeout=timeout, follow_redirects=True) as client:
            resp = client.get(OVER_UNDER_LINES_URL)
            if resp.status_code == 401:
                return ([], 401)
            if resp.status_code == 403:
                return ([], 403)
            resp.raise_for_status()
            data = resp.json()
    except httpx.HTTPStatusError:
        return ([], None)
    except Exception:
        return ([], None)
    records = parse_underdog_over_under_api(data) if isinstance(data, dict) else []
    return (records, None)


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
    raw_solo_games = pickem_data.get("solo_games") or []
    raw_oul = pickem_data.get("over_under_lines") or []
    players = _normalize_to_dict(raw_players)
    if not players and isinstance(raw_players, list):
        players = {p.get("id"): p for p in raw_players if isinstance(p, dict)}
    games = _normalize_to_dict(raw_games)
    if not games and isinstance(raw_games, list):
        games = {str(g.get("id")): g for g in raw_games if isinstance(g, dict)}
    appearances = _normalize_to_list(raw_appearances)
    over_under_lines = _normalize_to_list(raw_oul)
    solo_games = _normalize_to_list(raw_solo_games)

    # Build stage lists for players, appearances, games, solo_games (dedupe by id per run)
    global _LAST_UNDERDOG_PLAYERS, _LAST_UNDERDOG_APPEARANCES, _LAST_UNDERDOG_GAMES, _LAST_UNDERDOG_SOLO_GAMES, _LAST_UNDERDOG_PROJECTIONS
    global _SEEN_UNDERDOG_PLAYER_IDS, _SEEN_UNDERDOG_APPEARANCE_IDS, _SEEN_UNDERDOG_GAME_IDS, _SEEN_UNDERDOG_SOLO_GAME_IDS, _SEEN_UNDERDOG_PROJECTION_KEYS
    for p in players.values():
        if not isinstance(p, dict):
            continue
        pid = p.get("id")
        if pid is None or pid in _SEEN_UNDERDOG_PLAYER_IDS:
            continue
        _SEEN_UNDERDOG_PLAYER_IDS.add(pid)
        _LAST_UNDERDOG_PLAYERS.append(
            {
                "id": pid,
                "first_name": (p.get("first_name") or "").strip(),
                "last_name": (p.get("last_name") or "").strip(),
                "position_display_name": p.get("position_display_name"),
                "position_id": p.get("position_id"),
                "position_name": p.get("position_name"),
                "team_id": p.get("team_id"),
                "sport_id": p.get("sport_id"),
                "jersey_number": p.get("jersey_number"),
                "image_url": p.get("image_url"),
                "dark_image_url": p.get("dark_image_url"),
                "light_image_url": p.get("light_image_url"),
                "action_path": p.get("action_path"),
                "country": p.get("country"),
            }
        )

    appearance_info = {}
    for a in appearances:
        if not isinstance(a, dict):
            continue
        aid = a.get("id") or a.get("appearance_id")
        pid = a.get("player_id")
        gid = a.get("game_id") or a.get("match_id")
        if gid is not None and gid not in games:
            gid = str(gid)
        start_time = ""
        if gid is not None and gid in games:
            g = games.get(gid)
            st = (g or {}).get("start_time") or (g or {}).get("starts_at") or (g or {}).get("scheduled_at")
            if st is not None:
                start_time = to_chicago_local(st) or ""
        name = ""
        if pid and pid in players:
            p = players[pid]
            first = (p.get("first_name") or "").strip()
            last = (p.get("last_name") or "").strip()
            name = f"{first} {last}".strip() or (p.get("full_name") or p.get("display_name") or "")
        if aid is not None:
            appearance_info[aid] = (name, start_time, pid)
            if aid not in _SEEN_UNDERDOG_APPEARANCE_IDS:
                _SEEN_UNDERDOG_APPEARANCE_IDS.add(aid)
                _LAST_UNDERDOG_APPEARANCES.append(
                    {
                        "id": aid,
                        "player_id": pid,
                        "match_id": gid,
                        "match_type": a.get("match_type"),
                        "team_id": a.get("team_id"),
                        "position_id": a.get("position_id"),
                        "lineup_status_id": a.get("lineup_status_id"),
                        "sort_by": a.get("sort_by"),
                        "multiple_picks_allowed": a.get("multiple_picks_allowed"),
                        "type": a.get("type"),
                    }
                )

    for g in games.values():
        if not isinstance(g, dict):
            continue
        gid = g.get("id")
        if gid is None or gid in _SEEN_UNDERDOG_GAME_IDS:
            continue
        _SEEN_UNDERDOG_GAME_IDS.add(gid)
        _LAST_UNDERDOG_GAMES.append(
            {
                "id": gid,
                "scheduled_at": g.get("scheduled_at"),
                "home_team_id": g.get("home_team_id"),
                "away_team_id": g.get("away_team_id"),
                "title": g.get("title"),
                "short_title": g.get("short_title"),
                "abbreviated_title": g.get("abbreviated_title"),
                "full_team_names_title": g.get("full_team_names_title"),
                "status": g.get("status"),
                "sport_id": g.get("sport_id"),
                "type": g.get("type"),
                "period": g.get("period"),
                "match_progress": g.get("match_progress"),
                "away_team_score": g.get("away_team_score"),
                "home_team_score": g.get("home_team_score"),
                "rank": g.get("rank"),
                "year": g.get("year"),
                "season_type": g.get("season_type"),
                "updated_at": g.get("updated_at"),
                "rescheduled_from": g.get("rescheduled_from"),
                "title_suffix": g.get("title_suffix"),
                "manually_created": g.get("manually_created"),
                "pre_game_data": json.dumps(g.get("pre_game_data")) if g.get("pre_game_data") is not None else None,
            }
        )

    for sg in solo_games:
        if not isinstance(sg, dict):
            continue
        sgid = sg.get("id")
        if sgid is None or sgid in _SEEN_UNDERDOG_SOLO_GAME_IDS:
            continue
        _SEEN_UNDERDOG_SOLO_GAME_IDS.add(sgid)
        _LAST_UNDERDOG_SOLO_GAMES.append(
            {
                "id": sgid,
                "scheduled_at": sg.get("scheduled_at"),
                "home_player_id": sg.get("home_player_id"),
                "away_player_id": sg.get("away_player_id"),
                "title": sg.get("title"),
                "short_title": sg.get("short_title"),
                "abbreviated_title": sg.get("abbreviated_title"),
                "full_title": sg.get("full_title"),
                "status": sg.get("status"),
                "sport_id": sg.get("sport_id"),
                "type": sg.get("type"),
                "competition_id": sg.get("competition_id"),
                "rank": sg.get("rank"),
                "period": sg.get("period"),
                "match_progress": sg.get("match_progress"),
                "score": sg.get("score"),
                "updated_at": sg.get("updated_at"),
                "manually_created": sg.get("manually_created"),
                "sport_tournament_round_id": sg.get("sport_tournament_round_id"),
                "pre_game_data": json.dumps(sg.get("pre_game_data")) if sg.get("pre_game_data") is not None else None,
            }
        )

    for oul in over_under_lines:
        if not isinstance(oul, dict):
            continue
        appearance_stat = (oul.get("over_under") or {}).get("appearance_stat") or {}
        aid = appearance_stat.get("appearance_id")
        stat = (appearance_stat.get("display_stat") or appearance_stat.get("stat") or "").strip()
        stat = _normalize_stat(stat) or stat
        if not stat:
            stat = "Unknown"
        info = appearance_info.get(aid) or ("", "", None)
        name = info[0] if len(info) > 0 else ""
        start_time = info[1] if len(info) > 1 else ""
        # API gives appearance.player_id (Underdog GUID). We do not store it on projection; join uses
        # underdog_projection.appearance_id -> underdog_appearance -> underdog_player.id, then
        # underdog_player.underdog_player_id = [player].underdog_player_id (internal id).
        underdog_player_id = None
        # Line can be on the over_under_line (stat_value) or on each option (line/line_score)
        default_line = oul.get("stat_value")
        if default_line is not None:
            try:
                default_line = float(default_line)
            except (TypeError, ValueError):
                default_line = None
        options = oul.get("options") or []
        ou = oul.get("over_under") or {}
        options_json = json.dumps(options) if options else None

        for opt in options:
            if not isinstance(opt, dict):
                continue
            line = opt.get("line") or opt.get("line_score")
            if line is None:
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
            if line is None and default_line is not None:
                line = default_line
            if line is None:
                continue

            key = (name, stat, start_time, line)
            if key in _SEEN_UNDERDOG_PROJECTION_KEYS:
                continue
            _SEEN_UNDERDOG_PROJECTION_KEYS.add(key)

            projection_id = hash(key) & 0x7FFFFFFF
            if projection_id < 0:
                projection_id = -projection_id

            record = {
                "projection_id": projection_id,
                "display_name": (name or "")[:100],
                "stat_type_name": stat[:100],
                "line_score": line,
                "start_time": start_time or datetime.now(CHICAGO).strftime("%Y-%m-%d %H:%M:%S"),
                "underdog_player_id": underdog_player_id,
                # Expanded over_under_lines fields
                "api_id": oul.get("id"),
                "over_under_id": oul.get("over_under_id"),
                "provider_id": oul.get("provider_id"),
                "stat_value": float(oul["stat_value"]) if isinstance(oul.get("stat_value"), (int, float, str)) and str(oul.get("stat_value")).replace(".", "", 1).lstrip("-").isdigit() else None,
                "line_type": oul.get("line_type"),
                "status": oul.get("status"),
                "rank": oul.get("rank"),
                "sort_by": oul.get("sort_by"),
                "stable_id": oul.get("stable_id"),
                "expires_at": oul.get("expires_at"),
                "updated_at": oul.get("updated_at"),
                "contract_terms_url": oul.get("contract_terms_url"),
                "contract_url": oul.get("contract_url"),
                "live_event": oul.get("live_event"),
                "live_event_stat": oul.get("live_event_stat"),
                "non_discounted_stat_value": oul.get("non_discounted_stat_value"),
                # Nested over_under
                "ou_title": ou.get("title"),
                "ou_category": ou.get("category"),
                "ou_display_mode": ou.get("display_mode"),
                "ou_grid_display_title": ou.get("grid_display_title"),
                "ou_has_alternates": ou.get("has_alternates"),
                "ou_option_priority": ou.get("option_priority"),
                "ou_prediction_market": ou.get("prediction_market"),
                "ou_scoring_type_id": ou.get("scoring_type_id"),
                "ou_team_divider": ou.get("team_divider"),
                # Nested appearance_stat
                "appearance_stat_id": appearance_stat.get("id"),
                "appearance_id": appearance_stat.get("appearance_id"),
                "display_stat": appearance_stat.get("display_stat"),
                "stat": appearance_stat.get("stat"),
                "graded_by": appearance_stat.get("graded_by"),
                "pickem_stat_id": appearance_stat.get("pickem_stat_id"),
                "appearance_stat_rank": appearance_stat.get("rank"),
                # Options JSON
                "options": options_json,
            }
            records.append(record)
            _LAST_UNDERDOG_PROJECTIONS.append(record)

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
        # api_records is already the parsed projections from parse_underdog_over_under_api
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

    # Clear global caches and dedupe sets for this run so multiple parse_underdog_over_under_api()
    # calls accumulate consistently without duplicate entity rows in stage tables.
    global _LAST_UNDERDOG_PLAYERS, _LAST_UNDERDOG_APPEARANCES, _LAST_UNDERDOG_GAMES, _LAST_UNDERDOG_SOLO_GAMES, _LAST_UNDERDOG_PROJECTIONS
    global _SEEN_UNDERDOG_PLAYER_IDS, _SEEN_UNDERDOG_APPEARANCE_IDS, _SEEN_UNDERDOG_GAME_IDS, _SEEN_UNDERDOG_SOLO_GAME_IDS, _SEEN_UNDERDOG_PROJECTION_KEYS
    _LAST_UNDERDOG_PLAYERS = []
    _LAST_UNDERDOG_APPEARANCES = []
    _LAST_UNDERDOG_GAMES = []
    _LAST_UNDERDOG_SOLO_GAMES = []
    _LAST_UNDERDOG_PROJECTIONS = []
    _SEEN_UNDERDOG_PLAYER_IDS = set()
    _SEEN_UNDERDOG_APPEARANCE_IDS = set()
    _SEEN_UNDERDOG_GAME_IDS = set()
    _SEEN_UNDERDOG_SOLO_GAME_IDS = set()
    _SEEN_UNDERDOG_PROJECTION_KEYS = set()

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
        # Try httpx first (same as PrizePicks)
        records, err = fetch_over_under_httpx()
        did_fallback = False
        if err in (401, 403):
            print(f"API returned {err}; falling back to browser capture.")
            did_fallback = True
            user_data_dir = args.user_data_dir or os.environ.get("PROPS_UNDERDOG_USER_DATA_DIR")
            if user_data_dir:
                print(f"Using profile: {user_data_dir}")
            records = fetch_with_playwright(
                save_path="underdog_captured.json",
                user_data_dir=user_data_dir,
                connect_url=args.connect,
                headed=args.headed,
                debug=args.debug,
            )
        if records:
            print(f"Fetched {len(records)} projection records via API.")
        elif err is None:
            print("Fetched 0 projection records via API.")
        elif did_fallback:
            print("No records after browser fallback (session may be expired). Try --browser --headed to log in, or --input <file.json>.")
        else:
            print("No records. Use --browser to capture from the app, or --input <file.json>.")

    if not records:
        print("No records to load. Use --input <file.json> with projection data, or check underdog_captured.json for API shape.")
        if args.db:
            print("Skipping DB update (0 records).")
        return 0

    if args.db:
        trusted = getattr(args, "trusted_connection", False) or os.environ.get("PROPS_DB_USE_TRUSTED_CONNECTION", "").strip().lower() in ("1", "true", "yes")

        # Insert projections into stage and merge. Use all records (API + fallback), normalized
        # to full column set so fallback 5-field records don't cause insert failures or get dropped.
        projections_to_stage = [
            r if "api_id" in r else _normalize_projection_record_minimal(r)
            for r in records
        ]
        n = insert_underdog_stage(
            projections_to_stage,
            server=args.db_server,
            database=args.database,
            user=args.db_user,
            password=args.db_password,
            trusted_connection=trusted,
        )
        print(f"Staged {n} projection rows")
        m = upsert_underdog_from_stage(
            server=args.db_server,
            database=args.database,
            user=args.db_user,
            password=args.db_password,
            trusted_connection=trusted,
        )
        print(f"Merged {m} rows into underdog_projection")

        # Insert supporting entities into their stage tables if we have them
        try:
            from typing import Iterable  # noqa: F401  (only for type checkers)
        except ImportError:
            pass

        def _insert_stage_helper(table: str, cols: list[str], rows: list[dict]) -> int:
            if not rows:
                return 0
            import pyodbc

            user = args.db_user or os.environ.get("PROPS_DB_USER", "dbadmin")
            password = args.db_password or os.environ.get("PROPS_DB_PASSWORD", "")
            conn = _get_db_conn(args.db_server, args.database, user, password, trusted)
            placeholders = ", ".join("?" * len(cols))
            with conn:
                cursor = conn.cursor()
                cursor.execute(f"TRUNCATE TABLE [dbo].[{table}]")
                for r in rows:
                    cursor.execute(
                        f"INSERT INTO [dbo].[{table}] ({', '.join(cols)}) VALUES ({placeholders})",
                        [r.get(c) for c in cols],
                    )
            conn.close()
            return len(rows)

        # Players
        player_cols = [
            "id",
            "first_name",
            "last_name",
            "position_display_name",
            "position_id",
            "position_name",
            "team_id",
            "sport_id",
            "jersey_number",
            "image_url",
            "dark_image_url",
            "light_image_url",
            "action_path",
            "country",
        ]
        pn = _insert_stage_helper("underdog_player_stage", player_cols, _LAST_UNDERDOG_PLAYERS)
        if pn:
            print(f"Staged {pn} player rows into underdog_player_stage")

        # Appearances
        appearance_cols = [
            "id",
            "player_id",
            "match_id",
            "match_type",
            "team_id",
            "position_id",
            "lineup_status_id",
            "sort_by",
            "multiple_picks_allowed",
            "type",
        ]
        an = _insert_stage_helper("underdog_appearance_stage", appearance_cols, _LAST_UNDERDOG_APPEARANCES)
        if an:
            print(f"Staged {an} appearance rows into underdog_appearance_stage")

        # Games
        game_cols = [
            "id",
            "scheduled_at",
            "home_team_id",
            "away_team_id",
            "title",
            "short_title",
            "abbreviated_title",
            "full_team_names_title",
            "status",
            "sport_id",
            "type",
            "period",
            "match_progress",
            "away_team_score",
            "home_team_score",
            "rank",
            "year",
            "season_type",
            "updated_at",
            "rescheduled_from",
            "title_suffix",
            "manually_created",
            "pre_game_data",
        ]
        gn = _insert_stage_helper("underdog_game_stage", game_cols, _LAST_UNDERDOG_GAMES)
        if gn:
            print(f"Staged {gn} game rows into underdog_game_stage")

        # Solo games
        solo_cols = [
            "id",
            "scheduled_at",
            "home_player_id",
            "away_player_id",
            "title",
            "short_title",
            "abbreviated_title",
            "full_title",
            "status",
            "sport_id",
            "type",
            "competition_id",
            "rank",
            "period",
            "match_progress",
            "score",
            "updated_at",
            "manually_created",
            "sport_tournament_round_id",
            "pre_game_data",
        ]
        sn = _insert_stage_helper("underdog_solo_game_stage", solo_cols, _LAST_UNDERDOG_SOLO_GAMES)
        if sn:
            print(f"Staged {sn} solo game rows into underdog_solo_game_stage")

        # Merge stage tables into main Underdog tables
        player_merged = upsert_underdog_player_from_stage(
            server=args.db_server,
            database=args.database,
            user=args.db_user,
            password=args.db_password,
            trusted_connection=trusted,
        )
        print(f"Merged {player_merged} rows into underdog_player")

        appearance_merged = upsert_underdog_appearance_from_stage(
            server=args.db_server,
            database=args.database,
            user=args.db_user,
            password=args.db_password,
            trusted_connection=trusted,
        )
        print(f"Merged {appearance_merged} rows into underdog_appearance")

        game_merged = upsert_underdog_game_from_stage(
            server=args.db_server,
            database=args.database,
            user=args.db_user,
            password=args.db_password,
            trusted_connection=trusted,
        )
        print(f"Merged {game_merged} rows into underdog_game")

        solo_merged = upsert_underdog_solo_game_from_stage(
            server=args.db_server,
            database=args.database,
            user=args.db_user,
            password=args.db_password,
            trusted_connection=trusted,
        )
        print(f"Merged {solo_merged} rows into underdog_solo_game")

    return 0


if __name__ == "__main__":
    exit(main())
