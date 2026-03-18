"""
Parlay Play projections scraper.
Fetches player prop lines for matching with PrizePicks (same player + stat + game).
Uses httpx first (like PrizePicks/Underdog); falls back to Playwright on 401/403 or when no API URL.
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
CHICAGO_FMT = "%m/%d/%Y %H:%M:%S"  # 03/17/2026 17:06:00


def _chicago_str_to_db_datetimeoffset(s: str) -> str | None:
    """Convert Chicago-format 'MM/DD/YYYY HH:MM:SS' to SQL Server datetimeoffset string (Central)."""
    if not s or not isinstance(s, str):
        return None
    s = s.strip()
    if not s:
        return None
    try:
        dt = datetime.strptime(s, CHICAGO_FMT)
        dt = dt.replace(tzinfo=CHICAGO)
        # SQL Server datetimeoffset: 'YYYY-MM-DD HH:MM:SS -06:00' (Python %z is -0600)
        z = dt.strftime("%z")
        offset = (z[:3] + ":" + z[3:]) if len(z) == 5 else " -06:00"
        return dt.strftime("%Y-%m-%d %H:%M:%S") + offset
    except ValueError:
        return None


def _to_chicago_datetime(value) -> str | None:
    """
    Parse a datetime value (ISO string, timestamp, or date string) and return
    America/Chicago time formatted as MM/DD/YYYY HH:MM:SS (e.g. 03/17/2026 17:06:00).
    Returns None if value cannot be parsed.
    """
    if value is None:
        return None
    dt = None
    if isinstance(value, (int, float)):
        try:
            ts = value / 1000 if value > 1e12 else value
            dt = datetime.utcfromtimestamp(ts).replace(tzinfo=ZoneInfo("UTC"))
        except (ValueError, OSError):
            return None
    elif isinstance(value, str):
        s = value.strip().replace("Z", "+00:00")
        try:
            dt = datetime.fromisoformat(s)
        except ValueError:
            return None
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=ZoneInfo("UTC"))
    else:
        return None
    chicago = dt.astimezone(CHICAGO)
    return chicago.strftime(CHICAGO_FMT)

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
    "3ptmade": "3 Pointers Made",
    "oreb": "Offensive Rebounds",
    "dreb": "Defensive Rebounds",
    "ptsreb": "Pts+Rebs",
    "ptsast": "Pts+Asts",
    "ptsrebast": "Pts+Rebs+Asts",
    "rebast": "Rebs+Asts",
    "fantasypoints": "Fantasy Points",
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
        start_time = _to_chicago_datetime(st)
        if not start_time:
            continue
        key = (name, stat, start_time)
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
            "start_time": start_time,
        })
    return records


# From HAR: parlayplay.io uses GET /api/v1/crossgame/search/ with query params.
# Override with PROPS_PARLAYPLAY_API_URL for a full URL, or use defaults below.
PARLAY_PLAY_API_BASE = os.environ.get(
    "PROPS_PARLAYPLAY_API_URL",
    "https://parlayplay.io/api/v1/crossgame/search/",
).strip()
# If env is a full URL with query string, use as-is; else append default params.
if "?" in PARLAY_PLAY_API_BASE:
    PARLAY_PLAY_API_URL = PARLAY_PLAY_API_BASE
else:
    PARLAY_PLAY_API_URL = (
        PARLAY_PLAY_API_BASE.rstrip("/")
        + "/?sport=All&league=&period=FG&includeAlt=true&version=2&includeBoost=true&includeSports=true"
    )

DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://parlayplay.io/",
    "Origin": "https://parlayplay.io",
    "Sec-Ch-Ua": '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
    "Sec-Ch-Ua-Mobile": "?0",
    "Sec-Ch-Ua-Platform": '"Windows"',
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-origin",
    "x-csrftoken": "1",
    "x-parlay-request": "1",
    "x-parlayplay-native-platform": "web",
    "x-parlayplay-platform": "web",
    "x-requested-with": "XMLHttpRequest",
}


def _parse_crossgame_response(data) -> list[dict]:
    """
    Parse Parlay Play crossgame/search API response into stage records.
    Handles common shapes: list at top level, or data/results/props/events array, with
    items having player name (player_name, display_name, name, player), stat, line, start_time.
    """
    if not isinstance(data, (dict, list)):
        return []
    items = []
    if isinstance(data, list):
        items = data
    else:
        for key in ("data", "results", "props", "events", "projections", "lines"):
            val = data.get(key)
            if isinstance(val, list):
                items = val
                break
        if not items and isinstance(data.get("data"), dict):
            # e.g. data: { results: [...] }
            inner = data["data"]
            for key in ("results", "props", "events", "projections", "lines", "items"):
                val = inner.get(key) if isinstance(inner, dict) else None
                if isinstance(val, list):
                    items = val
                    break
    records = []
    seen = set()
    for item in items:
        if not isinstance(item, dict):
            continue
        raw_player = item.get("player")
        if isinstance(raw_player, dict):
            name = raw_player.get("display_name") or raw_player.get("name") or raw_player.get("player_name") or ""
        else:
            name = item.get("player_name") or item.get("display_name") or item.get("name") or (raw_player or "")
        name = (name or "").strip()
        stat = (
            item.get("stat_type_name")
            or item.get("stat_type")
            or item.get("stat")
            or item.get("market")
            or item.get("display_stat")
            or ""
        )
        stat = _normalize_stat(stat) or (stat or "Unknown").strip()
        line = item.get("line_score") or item.get("line") or item.get("stat_line") or item.get("value")
        if line is not None:
            try:
                line = float(line)
            except (TypeError, ValueError):
                line = None
        raw_game = item.get("game")
        if isinstance(raw_game, dict):
            st = raw_game.get("start_time") or raw_game.get("startTime") or raw_game.get("game_date")
        else:
            st = item.get("start_time") or item.get("game_date") or item.get("startTime") or item.get("game_time")
        if st is None:
            continue
        start_time = _to_chicago_datetime(st)
        if not start_time:
            continue
        key = (name, stat, start_time)
        if key in seen:
            continue
        seen.add(key)
        projection_id = hash(key) & 0x7FFFFFFF
        if projection_id < 0:
            projection_id = -projection_id
        records.append({
            "projection_id": projection_id,
            "display_name": (name or "")[:100],
            "stat_type_name": (stat or "Unknown")[:100],
            "line_score": line,
            "start_time": start_time,
        })
    return records


def _parse_crossgame_players(body: dict) -> list[dict]:
    """
    Parse Parlay Play crossgame/search response when body has "players" array.
    Each entry: { "match": { "matchDate": "..." }, "player": { "fullName": "..." }, "stats": [ { "challengeName": "...", "statValue": 19.5 } ] }.
    Uses main line (statValue) only; normalizes stat names to match PrizePicks.
    """
    if not isinstance(body, dict):
        return []
    players = body.get("players")
    if not isinstance(players, list):
        return []
    records = []
    seen = set()
    for entry in players:
        if not isinstance(entry, dict):
            continue
        match = entry.get("match") or {}
        player = entry.get("player") or {}
        stats_list = entry.get("stats")
        if not isinstance(stats_list, list):
            continue
        name = player.get("fullName") or (
            (player.get("firstName") or "").strip() + " " + (player.get("lastName") or "").strip()
        ).strip()
        if not name:
            continue
        match_date = match.get("matchDate")
        if not match_date:
            continue
        start_time = _to_chicago_datetime(match_date)
        if not start_time:
            continue
        for stat_obj in stats_list:
            if not isinstance(stat_obj, dict):
                continue
            line = stat_obj.get("statValue")
            if line is None:
                continue
            try:
                line = float(line)
            except (TypeError, ValueError):
                continue
            raw_stat = (stat_obj.get("challengeName") or "").strip()
            stat = _normalize_stat(raw_stat) or raw_stat or "Unknown"
            key = (name, stat, start_time)
            if key in seen:
                continue
            seen.add(key)
            projection_id = hash(key) & 0x7FFFFFFF
            if projection_id < 0:
                projection_id = -projection_id
            records.append({
                "projection_id": projection_id,
                "display_name": (name or "")[:100],
                "stat_type_name": (stat or "Unknown")[:100],
                "line_score": line,
                "start_time": start_time,
            })
    return records


def _int_or_none(v):
    if v is None:
        return None
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


def _id_from_ref(ref) -> int | None:
    """Get id from API ref that may be a dict (e.g. {id: 2}) or just the int id."""
    if ref is None:
        return None
    if isinstance(ref, dict):
        return _int_or_none(ref.get("id"))
    return _int_or_none(ref)


def extract_crossgame_etl(body: dict) -> tuple[dict, dict, dict, dict, dict, dict, list[dict]]:
    """
    Parse body.players into entities and projection rows for the new schema.
    Returns (sports, leagues, teams, matches, players, stat_types, projections).
    Each of the first six is id -> row dict (or challenge_option -> row for stat_types).
    projections is a list of projection_stage rows with match_id, player_id, challenge_option, etc.
    """
    if not isinstance(body, dict):
        return {}, {}, {}, {}, {}, {}, []
    players = body.get("players")
    if not isinstance(players, list):
        return {}, {}, {}, {}, {}, {}, []
    sports, leagues, teams, matches, players_by_id, stat_types = {}, {}, {}, {}, {}, {}
    projections = []
    for entry in players:
        if not isinstance(entry, dict):
            continue
        raw_match = entry.get("match")
        raw_player = entry.get("player")
        match_id = _id_from_ref(raw_match)
        player_id = _id_from_ref(raw_player)
        if match_id is None or player_id is None:
            continue
        match = raw_match if isinstance(raw_match, dict) else {}
        player = raw_player if isinstance(raw_player, dict) else {}
        stats_list = entry.get("stats")
        if not isinstance(stats_list, list):
            continue
        # Sport (from match) - may be dict or int id
        sport = match.get("sport")
        sid = _id_from_ref(sport)
        if sid is not None and sid not in sports:
            sports[sid] = {
                "id": sid,
                "sport_name": (sport.get("sportName") or "")[:100] if isinstance(sport, dict) else None,
                "slug": (sport.get("slug") or "")[:50] if isinstance(sport, dict) else None,
                "symbol": (sport.get("symbol") or "")[:500] if isinstance(sport, dict) else None,
                "illustration": (sport.get("illustration") or "")[:500] if isinstance(sport, dict) else None,
                "popularity": (sport.get("popularity") or "")[:20] if isinstance(sport, dict) else None,
            }
        # League - may be dict or int id
        league = match.get("league")
        lid = _id_from_ref(league)
        if lid is not None and lid not in leagues:
            leagues[lid] = {
                "id": lid,
                "sport_id": _id_from_ref(league.get("sport") if isinstance(league, dict) else None) or _id_from_ref(sport),
                "league_name": (league.get("leagueName") or "")[:100] if isinstance(league, dict) else None,
                "league_name_short": (league.get("leagueNameShort") or "")[:20] if isinstance(league, dict) else None,
                "slug": (league.get("slug") or "")[:50] if isinstance(league, dict) else None,
                "popularity": (league.get("popularity") or "")[:20] if isinstance(league, dict) else None,
                "allowed_players_per_match": _int_or_none(league.get("allowedPlayersPerMatch")) if isinstance(league, dict) else None,
            }
        # Teams - may be dict or int id
        for team_key in ("homeTeam", "awayTeam"):
            team = match.get(team_key)
            tid = _id_from_ref(team)
            if tid is not None and tid not in teams:
                teams[tid] = {
                    "id": tid,
                    "sport_id": _id_from_ref(team.get("sport") if isinstance(team, dict) else None) or _id_from_ref(sport),
                    "league_id": _id_from_ref(team.get("league") if isinstance(team, dict) else None) or _id_from_ref(league),
                    "teamname": (team.get("teamname") or "")[:100] if isinstance(team, dict) else None,
                    "teamname_abbr": (team.get("teamnameAbbr") or "")[:50] if isinstance(team, dict) else None,
                    "team_abbreviation": (team.get("teamAbbreviation") or "")[:20] if isinstance(team, dict) else None,
                    "slug": (team.get("slug") or "")[:100] if isinstance(team, dict) else None,
                    "venue": (team.get("venue") or "")[:200] if isinstance(team, dict) else None,
                    "logo": (team.get("logo") or "")[:500] if isinstance(team, dict) else None,
                    "conference": (team.get("conference") or "")[:100] if isinstance(team, dict) else None,
                    "rank": _int_or_none(team.get("rank")) if isinstance(team, dict) else None,
                    "record": (team.get("record") or "")[:20] if isinstance(team, dict) else None,
                }
        # Match
        if match_id not in matches:
            home_team = match.get("homeTeam")
            away_team = match.get("awayTeam")
            match_date = match.get("matchDate")
            match_date_conv = _to_chicago_datetime(match_date) if match_date else None
            matches[match_id] = {
                "id": match_id,
                "sport_id": _id_from_ref(sport),
                "league_id": _id_from_ref(league),
                "home_team_id": _id_from_ref(home_team),
                "away_team_id": _id_from_ref(away_team),
                "slug": (match.get("slug") or "")[:150],
                "match_date": _chicago_str_to_db_datetimeoffset(match_date_conv) if match_date_conv else None,
                "match_type": (match.get("matchType") or "")[:50],
                "match_status": _int_or_none(match.get("matchStatus")),
                "match_period": (match.get("matchPeriod") or "")[:20],
                "score_home": _int_or_none(match.get("scoreHome")),
                "score_away": _int_or_none(match.get("scoreAway")),
                "time_left": (match.get("timeLeft") or "")[:20],
                "time_to_start": (match.get("timeToStart") or "")[:20],
                "time_to_start_min": _int_or_none(match.get("timeToStartMin")),
                "home_win_prob": (str(match.get("homeWinProb") or ""))[:20],
                "away_win_prob": (str(match.get("awayWinProb") or ""))[:20],
                "draw_prob": (str(match.get("drawProb") or ""))[:20],
            }
        # Player
        if player_id not in players_by_id:
            players_by_id[player_id] = {
                "id": player_id,
                "sport_id": _id_from_ref(player.get("sport")),
                "team_id": _id_from_ref(player.get("team")),
                "first_name": (player.get("firstName") or "")[:100],
                "last_name": (player.get("lastName") or "")[:100],
                "full_name": (player.get("fullName") or ((player.get("firstName") or "") + " " + (player.get("lastName") or "")).strip())[:150],
                "name_initial": (player.get("nameInitial") or "")[:50],
                "image": (player.get("image") or "")[:500],
                "position": (player.get("position") or "")[:20],
                "gender": (player.get("gender") or "")[:10],
                "popularity": (str(player.get("popularity") or ""))[:20],
                "show_alt_lines": bool(player.get("showAltLines")) if player.get("showAltLines") is not None else None,
            }
        name = (player.get("fullName") or (players_by_id[player_id]["full_name"] or "")).strip() or "Unknown"
        match_date = match.get("matchDate")
        start_time_str = _to_chicago_datetime(match_date) if match_date else None
        if not start_time_str:
            continue
        stat_type_name = _normalize_stat("") or "Unknown"
        for stat_obj in stats_list:
            if not isinstance(stat_obj, dict):
                continue
            challenge_option = (stat_obj.get("challengeOption") or "").strip() or (stat_obj.get("challengeName") or "").strip()
            if not challenge_option:
                continue
            challenge_option = challenge_option[:50]
            if challenge_option not in stat_types:
                stat_types[challenge_option] = {
                    "challenge_option": challenge_option,
                    "challenge_name": (stat_obj.get("challengeName") or "")[:100],
                    "challenge_units": (stat_obj.get("challengeUnits") or "")[:20],
                }
            raw_stat = (stat_obj.get("challengeName") or "").strip()
            stat_type_name = _normalize_stat(raw_stat) or raw_stat or "Unknown"
            # Main line
            main_line = stat_obj.get("statValue")
            if main_line is not None:
                try:
                    main_line = float(main_line)
                except (TypeError, ValueError):
                    main_line = None
            if main_line is not None:
                proj_id = (hash((match_id, player_id, challenge_option, main_line, True)) & 0x7FFFFFFF) or 1
                if proj_id < 0:
                    proj_id = -proj_id
                projections.append(_projection_row(
                    projection_id=proj_id,
                    match_id=match_id,
                    player_id=player_id,
                    challenge_option=challenge_option,
                    line_score=main_line,
                    is_main_line=True,
                    decimal_price_over=_float_or_none(stat_obj.get("defaultMultiplier")),
                    decimal_price_under=None,
                    market_name=(stat_obj.get("altLines") or {}).get("market") if isinstance(stat_obj.get("altLines"), dict) else None,
                    match_period=(stat_obj.get("matchPeriods") or [None])[0] if isinstance(stat_obj.get("matchPeriods"), list) else None,
                    display_name=name[:100],
                    stat_type_name=stat_type_name[:100],
                    start_time_str=start_time_str,
                    stat_obj=stat_obj,
                    alt_line_count=_int_or_none(stat_obj.get("altLineCount")),
                ))
            # Alt lines
            alt_lines = stat_obj.get("altLines") or {}
            values = alt_lines.get("values") if isinstance(alt_lines, dict) else []
            if isinstance(values, list):
                for idx, alt in enumerate(values):
                    if not isinstance(alt, dict):
                        continue
                    pts = alt.get("selectionPoints")
                    if pts is None:
                        continue
                    try:
                        pts = float(pts)
                    except (TypeError, ValueError):
                        continue
                    is_main = bool(alt.get("isMainLine"))
                    proj_id = (hash((match_id, player_id, challenge_option, pts, is_main, idx)) & 0x7FFFFFFF) or 1
                    if proj_id < 0:
                        proj_id = -proj_id
                    ct = alt.get("challengeType")
                    co = (ct.get("challengeOption") or challenge_option) if isinstance(ct, dict) else challenge_option
                    market_name = (alt.get("marketName") or (alt_lines.get("market") if isinstance(alt_lines, dict) else "") or "")[:150]
                    projections.append(_projection_row(
                        projection_id=proj_id,
                        match_id=match_id,
                        player_id=player_id,
                        challenge_option=(co or "")[:50],
                        line_score=pts,
                        is_main_line=is_main,
                        decimal_price_over=_float_or_none(alt.get("decimalPriceOver")),
                        decimal_price_under=_float_or_none(alt.get("decimalPriceUnder")),
                        market_name=market_name,
                        match_period=(alt.get("matchPeriod") or "")[:20],
                        show_default=bool(alt.get("showDefault")) if alt.get("showDefault") is not None else None,
                        display_name=name[:100],
                        stat_type_name=stat_type_name[:100],
                        start_time_str=start_time_str,
                        stat_obj=stat_obj,
                    ))
    return sports, leagues, teams, matches, players_by_id, stat_types, projections


def _float_or_none(v):
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _projection_row(
    projection_id, match_id, player_id, challenge_option, line_score, is_main_line,
    decimal_price_over, decimal_price_under, market_name, match_period, display_name, stat_type_name, start_time_str,
    stat_obj, show_default=None, alt_line_count=None,
):
    start_db = _chicago_str_to_db_datetimeoffset(start_time_str)
    promo = stat_obj.get("promoDeadline")
    promo_db = _chicago_str_to_db_datetimeoffset(_to_chicago_datetime(promo)) if promo else None
    return {
        "projection_id": projection_id,
        "match_id": match_id,
        "player_id": player_id,
        "challenge_option": challenge_option[:50],
        "line_score": line_score,
        "is_main_line": bool(is_main_line),
        "decimal_price_over": decimal_price_over,
        "decimal_price_under": decimal_price_under,
        "market_name": (market_name or "")[:150] if market_name else None,
        "match_period": (match_period or "")[:20] if match_period else None,
        "show_default": show_default,
        "display_name": display_name[:100],
        "stat_type_name": stat_type_name[:100],
        "start_time": start_db,
        "promo_deadline": promo_db,
        "promo_max_entry": _float_or_none(stat_obj.get("promoMaxEntry")),
        "player_promo_id": _int_or_none(stat_obj.get("playerPromoId")),
        "player_promo_type": (stat_obj.get("playerPromoType") or "")[:50],
        "is_boosted_payout": bool(stat_obj.get("isBoostedPayout")) if stat_obj.get("isBoostedPayout") is not None else None,
        "is_player_promo": bool(stat_obj.get("isPlayerPromo")) if stat_obj.get("isPlayerPromo") is not None else None,
        "default_multiplier": _float_or_none(stat_obj.get("defaultMultiplier")),
        "promo_multiplier": None,
        "payout_boost_selection": None,
        "is_public": None,
        "is_slashed_line": bool(stat_obj.get("isSlashedLine")) if stat_obj.get("isSlashedLine") is not None else None,
        "alt_line_count": alt_line_count,
    }


def fetch_parlay_play_httpx(timeout: float = 30) -> tuple[list[dict], int | None, dict | None]:
    """
    Fetch projections from Parlay Play API via httpx (no browser).
    Returns (records, None, data) on success (data for ETL when "players" in data),
    ([], status_code, None) on 401/403, ([], None, None) on other error.
    """
    if not PARLAY_PLAY_API_URL.strip():
        return ([], None, None)
    try:
        with httpx.Client(headers=DEFAULT_HEADERS, timeout=timeout, follow_redirects=True) as client:
            resp = client.get(PARLAY_PLAY_API_URL)
            if resp.status_code == 401:
                return ([], 401, None)
            if resp.status_code == 403:
                return ([], 403, None)
            if resp.status_code != 200:
                return ([], None, None)
            data = resp.json()
    except Exception:
        return ([], None, None)
    if isinstance(data, dict) and "players" in data:
        records = _parse_crossgame_players(data)
    else:
        records = _parse_crossgame_response(data)
    if not records:
        records = parse_records_from_json(data)
    return (records, None, data if isinstance(data, dict) else None)


def load_parlay_play_etl(
    bodies: list[dict],
    server: str = "localhost\\SQLEXPRESS",
    database: str = "Props",
    user: str | None = None,
    password: str | None = None,
    trusted_connection: bool = False,
) -> int:
    """Extract from crossgame bodies, load all stage tables, run MERGEs. Returns projection rows merged."""
    import pyodbc
    user = user or os.environ.get("PROPS_DB_USER", "dbadmin")
    password = password or os.environ.get("PROPS_DB_PASSWORD", "")
    sports, leagues, teams, matches, players, stat_types = {}, {}, {}, {}, {}, {}
    all_projections = []
    for body in bodies:
        if not isinstance(body, dict) or "players" not in body:
            continue
        s, lg, t, m, p, st, proj = extract_crossgame_etl(body)
        sports.update(s)
        leagues.update(lg)
        teams.update(t)
        matches.update(m)
        players.update(p)
        stat_types.update(st)
        all_projections.extend(proj)
    if not all_projections:
        return 0
    conn = _get_db_conn(server, database, user, password, trusted_connection)
    cursor = conn.cursor()
    try:
        # Load stages (order: sport, league, team, match, player, stat_type, projection)
        def run_truncate_insert(table: str, cols: list[str], rows: list[dict]):
            if not rows:
                return
            cursor.execute(f"TRUNCATE TABLE [dbo].[{table}]")
            ph = ", ".join("?" * len(cols))
            for r in rows:
                cursor.execute(
                    f"INSERT INTO [dbo].[{table}] ({', '.join(cols)}) VALUES ({ph})",
                    [r.get(c) for c in cols],
                )
        sport_cols = ["id", "sport_name", "slug", "symbol", "illustration", "popularity"]
        run_truncate_insert("parlay_play_sport_stage", sport_cols, list(sports.values()))
        league_cols = ["id", "sport_id", "league_name", "league_name_short", "slug", "popularity", "allowed_players_per_match"]
        run_truncate_insert("parlay_play_league_stage", league_cols, list(leagues.values()))
        team_cols = ["id", "sport_id", "league_id", "teamname", "teamname_abbr", "team_abbreviation", "slug", "venue", "logo", "conference", "rank", "record"]
        run_truncate_insert("parlay_play_team_stage", team_cols, list(teams.values()))
        match_cols = ["id", "sport_id", "league_id", "home_team_id", "away_team_id", "slug", "match_date", "match_type", "match_status", "match_period", "score_home", "score_away", "time_left", "time_to_start", "time_to_start_min", "home_win_prob", "away_win_prob", "draw_prob"]
        run_truncate_insert("parlay_play_match_stage", match_cols, list(matches.values()))
        player_cols = ["id", "sport_id", "team_id", "first_name", "last_name", "full_name", "name_initial", "image", "position", "gender", "popularity", "show_alt_lines"]
        run_truncate_insert("parlay_play_player_stage", player_cols, list(players.values()))
        st_cols = ["challenge_option", "challenge_name", "challenge_units"]
        run_truncate_insert("parlay_play_stat_type_stage", st_cols, list(stat_types.values()))
        proj_cols = ["projection_id", "match_id", "player_id", "challenge_option", "line_score", "is_main_line", "decimal_price_over", "decimal_price_under", "market_name", "match_period", "show_default", "display_name", "stat_type_name", "start_time", "promo_deadline", "promo_max_entry", "player_promo_id", "player_promo_type", "is_boosted_payout", "is_player_promo", "default_multiplier", "promo_multiplier", "payout_boost_selection", "is_public", "is_slashed_line", "alt_line_count"]
        run_truncate_insert("parlay_play_projection_stage", proj_cols, all_projections)
        # MERGE in dependency order (inline, same as prizepicks/underdog; ODBC requires trailing ;)
        _merge_sport = """
        MERGE [dbo].[parlay_play_sport] AS t
        USING [dbo].[parlay_play_sport_stage] AS s ON t.id = s.id
        WHEN MATCHED THEN UPDATE SET
            t.sport_name = s.sport_name, t.slug = s.slug, t.symbol = s.symbol,
            t.illustration = s.illustration, t.popularity = s.popularity, t.last_modified_at = GETUTCDATE()
        WHEN NOT MATCHED BY TARGET THEN INSERT (id, sport_name, slug, symbol, illustration, popularity, last_modified_at)
        VALUES (s.id, s.sport_name, s.slug, s.symbol, s.illustration, s.popularity, GETUTCDATE());
        """
        _merge_league = """
        MERGE [dbo].[parlay_play_league] AS t
        USING [dbo].[parlay_play_league_stage] AS s ON t.id = s.id
        WHEN MATCHED THEN UPDATE SET
            t.sport_id = s.sport_id, t.league_name = s.league_name, t.league_name_short = s.league_name_short,
            t.slug = s.slug, t.popularity = s.popularity, t.allowed_players_per_match = s.allowed_players_per_match, t.last_modified_at = GETUTCDATE()
        WHEN NOT MATCHED BY TARGET THEN INSERT (id, sport_id, league_name, league_name_short, slug, popularity, allowed_players_per_match, last_modified_at)
        VALUES (s.id, s.sport_id, s.league_name, s.league_name_short, s.slug, s.popularity, s.allowed_players_per_match, GETUTCDATE());
        """
        _merge_team = """
        MERGE [dbo].[parlay_play_team] AS t
        USING [dbo].[parlay_play_team_stage] AS s ON t.id = s.id
        WHEN MATCHED THEN UPDATE SET
            t.sport_id = s.sport_id, t.league_id = s.league_id, t.teamname = s.teamname, t.teamname_abbr = s.teamname_abbr,
            t.team_abbreviation = s.team_abbreviation, t.slug = s.slug, t.venue = s.venue, t.logo = s.logo,
            t.conference = s.conference, t.rank = s.rank, t.record = s.record, t.last_modified_at = GETUTCDATE()
        WHEN NOT MATCHED BY TARGET THEN INSERT (id, sport_id, league_id, teamname, teamname_abbr, team_abbreviation, slug, venue, logo, conference, rank, record, last_modified_at)
        VALUES (s.id, s.sport_id, s.league_id, s.teamname, s.teamname_abbr, s.team_abbreviation, s.slug, s.venue, s.logo, s.conference, s.rank, s.record, GETUTCDATE());
        """
        _merge_match = """
        MERGE [dbo].[parlay_play_match] AS t
        USING [dbo].[parlay_play_match_stage] AS s ON t.id = s.id
        WHEN MATCHED THEN UPDATE SET
            t.sport_id = s.sport_id, t.league_id = s.league_id, t.home_team_id = s.home_team_id, t.away_team_id = s.away_team_id,
            t.slug = s.slug, t.match_date = s.match_date, t.match_type = s.match_type, t.match_status = s.match_status,
            t.match_period = s.match_period, t.score_home = s.score_home, t.score_away = s.score_away,
            t.time_left = s.time_left, t.time_to_start = s.time_to_start, t.time_to_start_min = s.time_to_start_min,
            t.home_win_prob = s.home_win_prob, t.away_win_prob = s.away_win_prob, t.draw_prob = s.draw_prob, t.last_modified_at = GETUTCDATE()
        WHEN NOT MATCHED BY TARGET THEN INSERT (id, sport_id, league_id, home_team_id, away_team_id, slug, match_date, match_type, match_status, match_period, score_home, score_away, time_left, time_to_start, time_to_start_min, home_win_prob, away_win_prob, draw_prob, last_modified_at)
        VALUES (s.id, s.sport_id, s.league_id, s.home_team_id, s.away_team_id, s.slug, s.match_date, s.match_type, s.match_status, s.match_period, s.score_home, s.score_away, s.time_left, s.time_to_start, s.time_to_start_min, s.home_win_prob, s.away_win_prob, s.draw_prob, GETUTCDATE());
        """
        _merge_player = """
        MERGE [dbo].[parlay_play_player] AS t
        USING [dbo].[parlay_play_player_stage] AS s ON t.id = s.id
        WHEN MATCHED THEN UPDATE SET
            t.sport_id = s.sport_id, t.team_id = s.team_id, t.first_name = s.first_name, t.last_name = s.last_name,
            t.full_name = s.full_name, t.name_initial = s.name_initial, t.image = s.image, t.position = s.position,
            t.gender = s.gender, t.popularity = s.popularity, t.show_alt_lines = s.show_alt_lines, t.last_modified_at = GETUTCDATE()
        WHEN NOT MATCHED BY TARGET THEN INSERT (id, sport_id, team_id, first_name, last_name, full_name, name_initial, image, position, gender, popularity, show_alt_lines, last_modified_at)
        VALUES (s.id, s.sport_id, s.team_id, s.first_name, s.last_name, s.full_name, s.name_initial, s.image, s.position, s.gender, s.popularity, s.show_alt_lines, GETUTCDATE());
        """
        _merge_stat_type = """
        MERGE [dbo].[parlay_play_stat_type] AS t
        USING [dbo].[parlay_play_stat_type_stage] AS s ON t.challenge_option = s.challenge_option
        WHEN MATCHED THEN UPDATE SET
            t.challenge_name = s.challenge_name, t.challenge_units = s.challenge_units, t.last_modified_at = GETUTCDATE()
        WHEN NOT MATCHED BY TARGET THEN INSERT (challenge_option, challenge_name, challenge_units, last_modified_at)
        VALUES (s.challenge_option, s.challenge_name, s.challenge_units, GETUTCDATE());
        """
        _merge_projection = """
        MERGE [dbo].[parlay_play_projection] AS t
        USING (
            SELECT *
            FROM (
                SELECT
                    s.*,
                    ROW_NUMBER() OVER (
                        PARTITION BY s.projection_id
                        ORDER BY
                            CASE WHEN s.is_main_line = 1 THEN 0 ELSE 1 END,
                            ISNULL(s.alt_line_count, 0) DESC,
                            ISNULL(s.line_score, -1) DESC,
                            ISNULL(s.match_id, 0) ASC,
                            ISNULL(s.player_id, 0) ASC
                    ) AS rn
                FROM [dbo].[parlay_play_projection_stage] s
            ) x
            WHERE x.rn = 1
        ) AS s ON t.projection_id = s.projection_id
        WHEN MATCHED THEN UPDATE SET
            t.match_id = s.match_id, t.player_id = s.player_id, t.challenge_option = s.challenge_option,
            t.line_score = s.line_score, t.is_main_line = s.is_main_line, t.decimal_price_over = s.decimal_price_over, t.decimal_price_under = s.decimal_price_under,
            t.market_name = s.market_name, t.match_period = s.match_period, t.show_default = s.show_default,
            t.display_name = s.display_name, t.stat_type_name = s.stat_type_name, t.start_time = s.start_time,
            t.promo_deadline = s.promo_deadline, t.promo_max_entry = s.promo_max_entry, t.player_promo_id = s.player_promo_id, t.player_promo_type = s.player_promo_type,
            t.is_boosted_payout = s.is_boosted_payout, t.is_player_promo = s.is_player_promo, t.default_multiplier = s.default_multiplier, t.promo_multiplier = s.promo_multiplier,
            t.payout_boost_selection = s.payout_boost_selection, t.is_public = s.is_public, t.is_slashed_line = s.is_slashed_line, t.alt_line_count = s.alt_line_count, t.last_modified_at = GETUTCDATE()
        WHEN NOT MATCHED BY TARGET THEN INSERT (projection_id, match_id, player_id, challenge_option, line_score, is_main_line, decimal_price_over, decimal_price_under, market_name, match_period, show_default, display_name, stat_type_name, start_time, promo_deadline, promo_max_entry, player_promo_id, player_promo_type, is_boosted_payout, is_player_promo, default_multiplier, promo_multiplier, payout_boost_selection, is_public, is_slashed_line, alt_line_count, last_modified_at)
        VALUES (s.projection_id, s.match_id, s.player_id, s.challenge_option, s.line_score, s.is_main_line, s.decimal_price_over, s.decimal_price_under, s.market_name, s.match_period, s.show_default, s.display_name, s.stat_type_name, s.start_time, s.promo_deadline, s.promo_max_entry, s.player_promo_id, s.player_promo_type, s.is_boosted_payout, s.is_player_promo, s.default_multiplier, s.promo_multiplier, s.payout_boost_selection, s.is_public, s.is_slashed_line, s.alt_line_count, GETUTCDATE());
        """
        merges = (
            ("sport", _merge_sport),
            ("league", _merge_league),
            ("team", _merge_team),
            ("match", _merge_match),
            ("player", _merge_player),
            ("stat_type", _merge_stat_type),
            ("projection", _merge_projection),
        )
        for merge_name, s in merges:
            s = s.strip().rstrip(";") + ";"
            cursor.execute(s)
        count = cursor.rowcount
        conn.commit()
    finally:
        conn.close()
    return count


def insert_parlay_play_stage(
    records: list[dict],
    server: str = "localhost\\SQLEXPRESS",
    database: str = "Props",
    user: str | None = None,
    password: str | None = None,
    trusted_connection: bool = False,
) -> int:
    """Legacy: 5-column stage insert (old schema). Use load_parlay_play_etl for new schema."""
    import pyodbc
    user = user or os.environ.get("PROPS_DB_USER", "dbadmin")
    password = password or os.environ.get("PROPS_DB_PASSWORD", "")
    conn = _get_db_conn(server, database, user, password, trusted_connection)
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
    trusted_connection: bool = False,
) -> int:
    """MERGE parlay_play_projection_stage into parlay_play_projection. Returns rows merged."""
    import pyodbc
    user = user or os.environ.get("PROPS_DB_USER", "dbadmin")
    password = password or os.environ.get("PROPS_DB_PASSWORD", "")
    conn = _get_db_conn(server, database, user, password, trusted_connection)
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
    # ODBC Driver 17 requires MERGE to end with exactly ;
    sql = sql.strip().rstrip(";") + ";"
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


USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"


def fetch_with_playwright(
    save_path: str | None = None,
    user_data_dir: str | None = None,
    connect_url: str | None = None,
    headed: bool = False,
    debug: bool = False,
) -> list[dict]:
    """Load Parlay Play in browser and capture API responses. Returns parsed records or [].
    Use user_data_dir for persistent profile (login saved), or connect_url to attach to existing browser.
    Use debug=True to capture every JSON response from parlayplay URLs."""
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        raise RuntimeError(
            "Playwright is required for browser capture. Install with: "
            "pip install playwright && playwright install chromium"
        ) from None
    captured = []
    board_url = "https://parlayplay.io"
    close_browser = True

    with sync_playwright() as p:
        if connect_url:
            try:
                browser = p.chromium.connect_over_cdp(connect_url)
                context = browser.contexts[0] if browser.contexts else browser.new_context()
                close_browser = False
            except Exception as e:
                print(f"Could not connect to browser at {connect_url} ({e}). Launching Chromium instead.")
                browser = p.chromium.launch(headless=not headed)
                context = browser.new_context(
                    user_agent=USER_AGENT,
                    viewport={"width": 1280, "height": 720},
                )
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
                if not debug and "parlayplay" not in u.lower():
                    return
                body = response.json()
                captured.append({"url": u, "body": body})
            except Exception:
                pass

        page.on("response", on_response)
        try:
            page.goto(board_url, wait_until="domcontentloaded", timeout=20000)
        except Exception:
            pass
        if headed:
            input("Log in in the browser window if needed. Press Enter here when done to continue capturing... ")
        if not user_data_dir:
            try:
                page.goto(board_url, wait_until="domcontentloaded", timeout=20000)
            except Exception:
                pass
        page.wait_for_timeout(8000)

        # If we have an API URL, try fetching it from page context (uses page cookies)
        api_records = []
        if PARLAY_PLAY_API_URL.strip():
            try:
                api_data = page.evaluate(
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
                    PARLAY_PLAY_API_URL,
                    timeout=20000,
                )
                if api_data and isinstance(api_data, dict) and api_data.get("_error") is None:
                    captured.append({"url": PARLAY_PLAY_API_URL, "body": api_data})
                    if "players" in api_data:
                        api_records = _parse_crossgame_players(api_data)
                    else:
                        api_records = _parse_crossgame_response(api_data)
                    if not api_records:
                        api_records = parse_records_from_json(api_data)
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
    if api_records:
        records = api_records
    else:
        for c in captured:
            body = c.get("body") or {}
            if isinstance(body, dict) and "players" in body:
                recs = _parse_crossgame_players(body)
            else:
                recs = _parse_crossgame_response(body)
            if not recs:
                recs = parse_records_from_json(body)
            if not recs:
                candidates = []
                _extract_from_nested(body, candidates)
                recs = parse_records_from_json(candidates)
            records.extend(recs)
    etl_bodies = [c["body"] for c in captured if isinstance(c.get("body"), dict) and "players" in c.get("body", {})]
    return (records, etl_bodies)


def main():
    parser = argparse.ArgumentParser(description="Parlay Play projections scraper")
    parser.add_argument("--input", metavar="JSON", help="Load projections from JSON file (list or { data: [] })")
    parser.add_argument("--browser", action="store_true", help="Capture from Parlay Play in browser (saves parlayplay_captured.json)")
    parser.add_argument("--user-data-dir", metavar="DIR", help="Use persistent browser profile (e.g. .playwright-parlayplay)")
    parser.add_argument(
        "--connect",
        metavar="WS_URL",
        default=os.environ.get("PROPS_BROWSER_CDP"),
        help="Connect to existing browser via CDP (e.g. http://localhost:9222); use Cursor Browser or Chrome with --remote-debugging-port=9222. Default: PROPS_BROWSER_CDP",
    )
    parser.add_argument("--headed", action="store_true", help="Show browser window (use with --user-data-dir to log in)")
    parser.add_argument("--debug", action="store_true", help="Capture every JSON response from parlayplay URLs; see parlayplay_captured.json")
    parser.add_argument("--db", action="store_true", help="Insert to stage and MERGE into parlay_play_projection")
    parser.add_argument("--db-server", default="localhost\\SQLEXPRESS")
    parser.add_argument("--db-user", default=os.environ.get("PROPS_DB_USER", "dbadmin"))
    parser.add_argument("--db-password", default=os.environ.get("PROPS_DB_PASSWORD", ""))
    parser.add_argument("--trusted-connection", action="store_true", help="Use Windows Authentication")
    parser.add_argument("--database", default="Props")
    args = parser.parse_args()

    records = []
    etl_bodies = []
    if args.input:
        with open(args.input) as f:
            data = json.load(f)
        # Captured format: list of { "url": "...", "body": {...} } from --browser --debug
        if isinstance(data, list) and data and isinstance(data[0], dict) and "url" in data[0] and "body" in data[0]:
            for entry in data:
                body = entry.get("body") or {}
                if isinstance(body, dict) and "players" in body:
                    etl_bodies.append(body)
                    recs = _parse_crossgame_players(body)
                else:
                    recs = _parse_crossgame_response(body)
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
        path = "parlayplay_captured.json"
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
        # Try httpx first (same as Underdog/PrizePicks)
        records, err, data = fetch_parlay_play_httpx()
        etl_bodies = [data] if (data and isinstance(data, dict) and "players" in data) else []
        did_fallback = False
        if err in (401, 403):
            print(f"API returned {err}; falling back to browser capture.")
            did_fallback = True
            user_data_dir = args.user_data_dir or os.environ.get("PROPS_PARLAYPLAY_USER_DATA_DIR")
            connect_url = args.connect or os.environ.get("PROPS_BROWSER_CDP")
            if user_data_dir:
                print(f"Using profile: {user_data_dir}")
            if connect_url:
                print(f"Using browser: {connect_url[:50]}...")
            records, etl_bodies = fetch_with_playwright(
                save_path="parlayplay_captured.json",
                user_data_dir=user_data_dir,
                connect_url=connect_url,
                headed=args.headed,
                debug=args.debug,
            )
        if records:
            print(f"Fetched {len(records)} projection records via {'browser' if did_fallback else 'API'}.")
        elif err is None:
            print("Fetched 0 projection records via API.")
        elif did_fallback:
            print("No records after browser fallback (session may be expired). Try --browser --headed to log in, or --input <file.json>.")
        else:
            print("No records. Use --browser to capture from the app, or --input <file.json>.")

    if not records:
        print("No records to load. Use --input <file.json> with projection data, or check parlayplay_captured.json for API shape.")
        if args.db:
            print("Skipping DB update (0 records).")
        return 0

    if args.db:
        trusted = getattr(args, "trusted_connection", False) or os.environ.get("PROPS_DB_USE_TRUSTED_CONNECTION", "").strip().lower() in ("1", "true", "yes")
        if etl_bodies:
            m = load_parlay_play_etl(
                etl_bodies,
                server=args.db_server,
                database=args.database,
                user=args.db_user,
                password=args.db_password,
                trusted_connection=trusted,
            )
            print(f"Staged and merged {m} rows into parlay_play_projection")
        else:
            n = insert_parlay_play_stage(
                records,
                server=args.db_server,
                database=args.database,
                user=args.db_user,
                password=args.db_password,
                trusted_connection=trusted,
            )
            print(f"Staged {n} rows")
            m = upsert_parlay_play_from_stage(
                server=args.db_server,
                database=args.database,
                user=args.db_user,
                password=args.db_password,
                trusted_connection=trusted,
            )
            print(f"Merged {m} rows into parlay_play_projection")
    return 0


if __name__ == "__main__":
    exit(main())
