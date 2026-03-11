"""
Soccer player per-game stats scraper using soccerdata (FBref or SofaScore).
Fetches player match stats (goals, assists, shots, etc.) and loads into soccer_player_stat.
Use --source fbref (default) or --source sofascore. SofaScore uses api.sofascore.com and often avoids FBref 403.
"""

import sys
from pathlib import Path
if str(Path(__file__).resolve().parent.parent) not in sys.path:
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import db_config  # noqa: F401 - load .env from repo root before DB
import argparse
import hashlib
import os
import re
import time
from datetime import datetime
from typing import Any, Optional

# Default: last 2 days through today for current season
RATE_LIMIT_SECONDS = 1.0  # be nice to FBref

# FBref league IDs (soccerdata) - use FBref.available_leagues() for full list
DEFAULT_LEAGUES = ["ENG-Premier League", "ESP-La Liga", "ITA-Serie A", "GER-Bundesliga", "FRA-Ligue 1"]
# Current season = season end year (e.g. 2025-26 season → 2026)
DEFAULT_SEASON = str(datetime.now().year)  # e.g. "2026" for 2025-26


def _get_date_range(days_back: int = 2) -> tuple[str, str]:
    today = datetime.now().date()
    date_from = today.replace(day=max(1, today.day - days_back)) if days_back else today
    return date_from.strftime("%Y-%m-%d"), today.strftime("%Y-%m-%d")


def _parse_game_date(raw: Any) -> Optional[str]:
    """Parse date to YYYY-MM-DD."""
    if raw is None or (isinstance(raw, float) and raw != raw):
        return None
    s = str(raw).strip()
    if not s:
        return None
    if len(s) == 10 and s[4] == "-" and s[7] == "-":
        return s
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y", "%b %d, %Y", "%d %b %Y"):
        try:
            dt = datetime.strptime(s[:20], fmt)
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


def _safe_int(val: Any, default: int = 0) -> int:
    if val is None or val == "" or (isinstance(val, float) and val != val):
        return default
    try:
        return int(float(val))
    except (ValueError, TypeError):
        return default


def _get(r: dict, *keys: str) -> Any:
    for k in keys:
        v = r.get(k)
        if v is not None and v != "" and not (isinstance(v, float) and v != v):
            return v
    return None


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


SOFASCORE_API = "https://api.sofascore.com/api/v1"


SOFASCORE_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Referer": "https://www.sofascore.com/",
    "Accept": "application/json",
    "Accept-Language": "en-US,en;q=0.9",
}


def _sofascore_fetch(
    path: str,
    proxy: str | dict | None,
) -> dict | None:
    """Fetch JSON from SofaScore API. path is e.g. 'event/12345/lineups'. Returns dict or None on 403/error."""
    url = f"{SOFASCORE_API}/{path}"
    try:
        import httpx
    except ImportError:
        import json as _json
        import urllib.request
        req = urllib.request.Request(url, headers=SOFASCORE_HEADERS)
        try:
            if isinstance(proxy, dict):
                from urllib.request import ProxyHandler, build_opener
                opener = build_opener(ProxyHandler(proxy))
                resp = opener.open(req, timeout=15)
            else:
                resp = urllib.request.urlopen(req, timeout=15)
            return _json.loads(resp.read().decode())
        except urllib.error.HTTPError as e:
            if e.code == 403:
                return None
            raise
    proxies = None
    if proxy == "tor":
        proxies = "socks5://127.0.0.1:9050"
    elif isinstance(proxy, dict):
        proxies = proxy.get("https") or proxy.get("http")
    try:
        with httpx.Client(proxy=proxies, timeout=15.0, follow_redirects=True) as client:
            r = client.get(url, headers=SOFASCORE_HEADERS)
            r.raise_for_status()
            return r.json()
    except httpx.HTTPStatusError as e:
        if e.response.status_code == 403:
            return None
        raise


def _sofascore_schedule_fallback(
    sofa: Any,
    proxy: str | dict | None,
) -> list[dict]:
    """Build schedule by calling SofaScore API when soccerdata read_schedule() bugs (e.g. empty df)."""
    from datetime import timezone
    df_seasons = sofa.read_seasons()
    out: list[dict] = []
    for (lkey, skey), season in df_seasons.iterrows():
        league_id = season["league_id"]
        season_id = season["season_id"]
        time.sleep(max(0.2, RATE_LIMIT_SECONDS))
        rounds_data = _sofascore_fetch(f"unique-tournament/{league_id}/season/{season_id}/rounds", proxy)
        if not rounds_data or "rounds" not in rounds_data:
            continue
        for round_obj in rounds_data["rounds"]:
            rnd = round_obj.get("round")
            if rnd is None:
                continue
            time.sleep(max(0.2, RATE_LIMIT_SECONDS))
            events_data = _sofascore_fetch(
                f"unique-tournament/{league_id}/season/{season_id}/events/round/{rnd}", proxy
            )
            if not events_data or "events" not in events_data:
                continue
            for _match in events_data["events"]:
                if _match.get("status", {}).get("code") not in (0, 100):
                    continue
                ts = _match.get("startTimestamp")
                date_val = datetime.fromtimestamp(ts, tz=timezone.utc) if ts else None
                date_str = date_val.strftime("%Y-%m-%d") if date_val else ""
                out.append({
                    "league": lkey,
                    "season": skey,
                    "game_id": _match.get("id"),
                    "date": date_val,
                    "home_team": (_match.get("homeTeam") or {}).get("name", ""),
                    "away_team": (_match.get("awayTeam") or {}).get("name", ""),
                })
    return out


def _season_for_sofascore(season: str) -> str | list[str]:
    """SofaScore/soccerdata expects season like 2025/2026 or 2025-26 for multi-year leagues. If user passes 2026 (end year), try that and 2025/2026."""
    s = str(season).strip()
    if not s:
        return season
    if len(s) == 4 and s.isdigit():
        y = int(s)
        prev = str(y - 1)
        return [f"{prev}/{s}", f"{prev}-{s[-2:]}", s]
    return season


def fetch_player_match_stats_sofascore(
    leagues: list[str],
    season: str,
    proxy: str | dict | None = None,
    force_cache: bool = False,
) -> list[dict]:
    """Fetch per-game player stats from SofaScore via soccerdata schedule + API. Returns list of row dicts."""
    import soccerdata as sd
    season_candidates = _season_for_sofascore(season)
    if not isinstance(season_candidates, list):
        season_candidates = [season_candidates]
    kwargs: dict = {"leagues": leagues, "seasons": season_candidates}
    if proxy is not None:
        kwargs["proxy"] = proxy
    sofa = sd.Sofascore(**kwargs)
    try:
        df_schedule = sofa.read_schedule(force_cache=force_cache)
        if df_schedule is None or df_schedule.empty:
            schedule_list = _sofascore_schedule_fallback(sofa, proxy)
        else:
            df_schedule = df_schedule.reset_index()
            schedule_list = df_schedule.to_dict("records")
    except ValueError as e:
        if "Cannot set a DataFrame" in str(e) or "game" in str(e):
            schedule_list = _sofascore_schedule_fallback(sofa, proxy)
        else:
            raise
    if not schedule_list:
        return []
    rows: list[dict] = []
    for row in schedule_list:
        game_id = row.get("game_id")
        if game_id is None:
            continue
        league_val = row.get("league", leagues[0] if leagues else "Unknown")
        season_val = str(row.get("season", season))[:20]
        date_val = row.get("date")
        if hasattr(date_val, "strftime"):
            date_str = date_val.strftime("%Y-%m-%d")
        else:
            date_str = _parse_game_date(date_val) or ""
        if not date_str:
            continue
        home_team = (row.get("home_team") or "")[:100]
        away_team = (row.get("away_team") or "")[:100]
        game_id_str = f"sofascore_{game_id}"[:80]
        time.sleep(max(0.2, RATE_LIMIT_SECONDS))
        lineups = _sofascore_fetch(f"event/{game_id}/lineups", proxy)
        if not lineups:
            continue  # 403 or empty; skip this match
        for team_key, team_label, opp in (
            ("home", home_team, away_team),
            ("away", away_team, home_team),
        ):
            team_data = lineups.get(team_key)
            if not isinstance(team_data, dict):
                continue
            players = team_data.get("players") or []
            for p in players:
                pinfo = p.get("player") or p
                name = (pinfo.get("name") or pinfo.get("shortName") or "Unknown").strip()[:255]
                if not name or name == "Unknown":
                    continue
                player_id = hashlib.md5(f"sofascore|{league_val}|{season_val}|{name}|{team_label}".encode()).hexdigest()[:32]
                rows.append({
                    "league": league_val[:80],
                    "season": season_val,
                    "player_id": player_id,
                    "player_name": name,
                    "game_id": game_id_str,
                    "game_date": date_str,
                    "team": team_label,
                    "opponent": opp,
                    "minutes": None,
                    "goals": 0,
                    "assists": 0,
                    "shots": None,
                    "shots_on_target": None,
                    "penalty_goals": None,
                    "penalty_attempted": None,
                    "cards_yellow": None,
                    "cards_red": None,
                    "dribbles": None,
                    "touches": None,
                    "blocked_shots": None,
                    "corners": None,
                    "free_kicks": None,
                    "passes": None,
                    "passes_attempted": None,
                })
        incidents = _sofascore_fetch(f"event/{game_id}/incidents", proxy)
        scorer_goals: dict[str, int] = {}
        if incidents:
            for inc in (incidents.get("incidents") or []):
                if inc.get("incidentType") == "goal" or (inc.get("type") or "").lower() == "goal":
                    for p in (inc.get("players") or []):
                        pl = p.get("player") or p
                        name = (pl.get("name") or pl.get("shortName") or "").strip()
                        if name:
                            scorer_goals[name] = scorer_goals.get(name, 0) + 1
            for r in rows:
                if r["game_id"] == game_id_str:
                    r["goals"] = scorer_goals.get(r["player_name"], 0)
    return rows


def fetch_player_match_stats(
    leagues: list[str],
    season: str,
    proxy: str | dict | None = None,
    force_cache: bool = False,
) -> list[dict]:
    """Fetch per-game player stats from FBref via soccerdata. Returns list of row dicts.
    Use proxy='tor' (Tor on port 9050) or proxy={'http':'...','https':'...'} to avoid 403.
    Use force_cache=True to use only cached data (no new requests); needs existing cache.
    """
    import soccerdata as sd
    kwargs = {"leagues": leagues, "seasons": season}
    if proxy is not None:
        kwargs["proxy"] = proxy
    fbref = sd.FBref(**kwargs)
    df = fbref.read_player_match_stats(stat_type="summary", force_cache=force_cache)
    if df is None or df.empty:
        return []
    # Normalize column names to lowercase for lookup
    df = df.rename(columns={c: c.strip().lower().replace(" ", "_") for c in df.columns})
    rows = []
    for _, row in df.iterrows():
        r = row.to_dict()
        date_str = _parse_game_date(_get(r, "date", "datetime"))
        if not date_str:
            continue
        player_name = _get(r, "player", "player_name", "name") or "Unknown"
        team = _get(r, "team", "squad") or ""
        opponent = _get(r, "opponent", "opp") or ""
        # Stable player_id: hash of league+season+player_name+team (FBref has no numeric player id in summary)
        player_id = hashlib.md5(f"{leagues[0]}|{season}|{player_name}|{team}".encode()).hexdigest()[:32]
        # game_id: match identifier (date + team + opponent)
        game_id = f"{date_str}_{team}_{opponent}"[:80]
        minutes = _safe_int(_get(r, "min", "minutes", "min_played"), 0)
        goals = _safe_int(_get(r, "gls", "goals"), 0)
        assists = _safe_int(_get(r, "ast", "assists"), 0)
        shots = _safe_int(_get(r, "sh", "shots"), None) if _get(r, "sh", "shots") is not None else None
        sot = _safe_int(_get(r, "sot", "shots_on_target", "shots_on_target"), None) if _get(r, "sot", "shots_on_target") is not None else None
        pk = _safe_int(_get(r, "pk", "penalty_goals", "pk_goals"), None) if _get(r, "pk") is not None else None
        pkatt = _safe_int(_get(r, "pkatt", "penalty_attempted", "pk_attempted"), None) if _get(r, "pkatt") is not None else None
        crdy = _safe_int(_get(r, "crdy", "yellow_cards", "cards_yellow"), None) if _get(r, "crdy") is not None else None
        crdr = _safe_int(_get(r, "crdr", "red_cards", "cards_red"), None) if _get(r, "crdr") is not None else None
        dribbles = _safe_int(_get(r, "dribbles", "carries", "carries_total"), None) if _get(r, "dribbles", "carries") is not None else None
        touches = _safe_int(_get(r, "touches", "touches_total"), None) if _get(r, "touches") is not None else None
        blocked_shots = _safe_int(_get(r, "blocked", "blocks", "blocked_shots"), None) if _get(r, "blocked", "blocks") is not None else None
        corners = _safe_int(_get(r, "corners", "corner_kicks", "ck"), None) if _get(r, "corners", "corner_kicks", "ck") is not None else None
        free_kicks = _safe_int(_get(r, "fk", "free_kicks", "freekicks"), None) if _get(r, "fk", "free_kicks") is not None else None
        passes = _safe_int(_get(r, "passes", "passes_completed", "cmp"), None) if _get(r, "passes", "passes_completed", "cmp") is not None else None
        passes_attempted = _safe_int(_get(r, "passes_attempted", "pass_attempted", "att", "total_passes"), None) if _get(r, "passes_attempted", "pass_attempted", "att") is not None else None
        league_val = leagues[0] if leagues else "Unknown"
        rows.append({
            "league": league_val[:80],
            "season": str(season)[:20],
            "player_id": player_id,
            "player_name": (player_name or "")[:255],
            "game_id": game_id[:80],
            "game_date": date_str,
            "team": (team or "")[:100],
            "opponent": (opponent or "")[:100],
            "minutes": minutes if minutes >= 0 else None,
            "goals": goals,
            "assists": assists,
            "shots": shots,
            "shots_on_target": sot,
            "penalty_goals": pk,
            "penalty_attempted": pkatt,
            "cards_yellow": crdy,
            "cards_red": crdr,
            "dribbles": dribbles,
            "touches": touches,
            "blocked_shots": blocked_shots,
            "corners": corners,
            "free_kicks": free_kicks,
            "passes": passes,
            "passes_attempted": passes_attempted,
        })
    return rows


def ensure_soccer_player_stat_stage_table(conn) -> None:
    """Create soccer_player_stat_stage and soccer_player_stat if they do not exist."""
    cursor = conn.cursor()
    with open(
        os.path.join(os.path.dirname(__file__), "..", "schema", "soccer_player_stat_stage.sql"),
        encoding="utf-8",
    ) as f:
        sql = f.read()
    # Run each batch (GO-separated)
    for batch in re.split(r"\s*GO\s*", sql, flags=re.I):
        batch = batch.strip()
        if batch and not batch.startswith("--"):
            cursor.execute(batch)
    conn.commit()


def insert_player_stat_soccer_stage(
    records: list[dict],
    server: str = "localhost\\SQLEXPRESS",
    database: str = "Props",
    user: str | None = None,
    password: str | None = None,
) -> int:
    """Insert records into soccer_player_stat_stage (append; no truncate so multiple leagues can be staged). Returns count."""
    import pyodbc
    user = user or os.environ.get("PROPS_DB_USER", "dbadmin")
    password = password or os.environ.get("PROPS_DB_PASSWORD", "")

    conn = _get_db_conn(server, database, user, password)
    ensure_soccer_player_stat_stage_table(conn)

    cols = [
        "league", "season", "player_id", "player_name", "game_id", "game_date",
        "team", "opponent", "minutes", "goals", "assists", "shots", "shots_on_target",
        "penalty_goals", "penalty_attempted", "cards_yellow", "cards_red",
        "dribbles", "touches", "blocked_shots", "corners", "free_kicks", "passes", "passes_attempted",
    ]
    placeholders = ", ".join("?" * len(cols))
    count = 0
    with conn:
        cursor = conn.cursor()
        for r in records:
            vals = [r.get(c) for c in cols]
            try:
                cursor.execute(
                    f"INSERT INTO [dbo].[player_stat_soccer_stage] ({', '.join(cols)}) VALUES ({placeholders})",
                    vals,
                )
                count += cursor.rowcount
            except pyodbc.IntegrityError:
                # Duplicate (player_id, game_id) - skip or update; skip for simplicity
                pass
    conn.close()
    return count


def upsert_soccer_player_stat_from_stage(
    server: str = "localhost\\SQLEXPRESS",
    database: str = "Props",
    user: str | None = None,
    password: str | None = None,
) -> int:
    """MERGE soccer_player_stat_stage into soccer_player_stat. Returns rows affected."""
    user = user or os.environ.get("PROPS_DB_USER", "dbadmin")
    password = password or os.environ.get("PROPS_DB_PASSWORD", "")

    conn = _get_db_conn(server, database, user, password)
    merge_sql = """
        MERGE [dbo].[soccer_player_stat] AS t
        USING [dbo].[soccer_player_stat_stage] AS s
        ON t.player_id = s.player_id AND t.game_id = s.game_id
        WHEN MATCHED THEN UPDATE SET
            t.league = s.league,
            t.season = s.season,
            t.player_name = s.player_name,
            t.game_date = CAST(s.game_date AS date),
            t.team = s.team,
            t.opponent = s.opponent,
            t.minutes = s.minutes,
            t.goals = s.goals,
            t.assists = s.assists,
            t.shots = s.shots,
            t.shots_on_target = s.shots_on_target,
            t.penalty_goals = s.penalty_goals,
            t.penalty_attempted = s.penalty_attempted,
            t.cards_yellow = s.cards_yellow,
            t.cards_red = s.cards_red,
            t.dribbles = s.dribbles,
            t.touches = s.touches,
            t.blocked_shots = s.blocked_shots,
            t.corners = s.corners,
            t.free_kicks = s.free_kicks,
            t.passes = s.passes,
            t.passes_attempted = s.passes_attempted
        WHEN NOT MATCHED BY TARGET THEN INSERT (
            league, season, player_id, player_name, game_id, game_date,
            team, opponent, minutes, goals, assists, shots, shots_on_target,
            penalty_goals, penalty_attempted, cards_yellow, cards_red,
            dribbles, touches, blocked_shots, corners, free_kicks, passes, passes_attempted
        ) VALUES (
            s.league, s.season, s.player_id, s.player_name, s.game_id, CAST(s.game_date AS date),
            s.team, s.opponent, s.minutes, s.goals, s.assists, s.shots, s.shots_on_target,
            s.penalty_goals, s.penalty_attempted, s.cards_yellow, s.cards_red,
            s.dribbles, s.touches, s.blocked_shots, s.corners, s.free_kicks, s.passes, s.passes_attempted
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
        description="Scrape soccer player per-game stats (soccerdata: FBref or SofaScore)."
    )
    parser.add_argument(
        "--source",
        choices=("fbref", "sofascore"),
        default="fbref",
        help="Data source: fbref (default) or sofascore. For SofaScore, use --proxy if event/lineups return 403.",
    )
    parser.add_argument(
        "--league",
        default="ENG-Premier League",
        help="League ID (default: ENG-Premier League). Same IDs for FBref and soccerdata SofaScore. Use comma for multiple.",
    )
    parser.add_argument(
        "--season",
        default=DEFAULT_SEASON,
        help="Season end year (default: 2024)",
    )
    parser.add_argument(
        "--db",
        action="store_true",
        help="Stage and upsert to soccer_player_stat",
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
        help="DB password",
    )
    parser.add_argument(
        "-o", "--output",
        help="Output CSV path",
    )
    parser.add_argument(
        "--tor",
        action="store_true",
        help="Use Tor proxy (Tor must be running on port 9050). Helps avoid 403 from FBref.",
    )
    parser.add_argument(
        "--proxy",
        metavar="URL",
        help="HTTP(S) proxy URL, e.g. http://127.0.0.1:8080. Use if FBref returns 403.",
    )
    parser.add_argument(
        "--force-cache",
        action="store_true",
        help="Use only cached data (no new requests). Use after a successful run with --proxy so later runs work without proxy.",
    )
    args = parser.parse_args()

    leagues = [x.strip() for x in args.league.split(",") if x.strip()]
    if not leagues:
        leagues = ["ENG-Premier League"]

    proxy = None
    if args.tor:
        proxy = "tor"
        print("Using Tor proxy (ensure Tor is running on port 9050).")
    elif args.proxy:
        proxy = {"http": args.proxy, "https": args.proxy}
        print(f"Using proxy: {args.proxy}")

    if args.force_cache:
        print("Using cached data only (--force-cache).")

    print(f"Fetching player match stats for leagues={leagues}, season={args.season} (source={args.source})...")
    if args.source == "sofascore":
        records = fetch_player_match_stats_sofascore(leagues, args.season, proxy=proxy, force_cache=args.force_cache)
    else:
        records = fetch_player_match_stats(leagues, args.season, proxy=proxy, force_cache=args.force_cache)
    print(f"Fetched {len(records)} rows")

    if not records:
        print("No records to process.")
        return

    if args.output:
        import pandas as pd
        pd.DataFrame(records).to_csv(args.output, index=False)
        print(f"Wrote {args.output}")

    if args.db:
        n = insert_player_stat_soccer_stage(
            records,
            server=args.db_server,
            database="Props",
            user=args.db_user,
            password=args.db_password,
        )
        print(f"Inserted {n} rows into soccer_player_stat_stage")
        merge_count = upsert_soccer_player_stat_from_stage(
            server=args.db_server,
            database="Props",
            user=args.db_user,
            password=args.db_password,
        )
        print(f"Upserted {merge_count} rows to soccer_player_stat")


if __name__ == "__main__":
    main()
