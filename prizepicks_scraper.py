"""
Prize Picks API scraper - fetches player projections and prop lines.
Uses the public API endpoint (api.prizepicks.com) for reliable, fast data.
"""

import db_config  # noqa: F401 - load .env from repo root before DB
import httpx
import json
import argparse
import os
import time
from pathlib import Path
from datetime import datetime
from zoneinfo import ZoneInfo

# Common league IDs (from Prize Picks)
LEAGUES = {
    "nba": 7,
    "nfl": 9,
    "nhl": 8,
    "mlb": 2,
    "cfb": 15,
    "cbb": 20,
    "wnba": 3,
    "pga": 1,
    "tennis": 5,
    "mma": 12,
    "epl": 14,
    "soccer": 82,
}

BASE_URL = "https://api.prizepicks.com"

DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Accept": "application/json",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Referer": "https://app.prizepicks.com/",
    "Origin": "https://app.prizepicks.com",
    "Sec-Ch-Ua": '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
    "Sec-Ch-Ua-Mobile": "?0",
    "Sec-Ch-Ua-Platform": '"Windows"',
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-site",
    "Priority": "u=1, i",
}


def _fetch_with_playwright(
    url: str,
    params: dict,
    cookies_path: str | None = None,
    headed: bool = False,
    connect_url: str | None = None,
    user_data_dir: str | None = None,
    debug: bool = False,
) -> dict:
    """Fetch using Playwright. Loads app, intercepts projections API response."""
    from playwright.sync_api import sync_playwright

    league_id = params.get("league_id", 7)
    close_browser = True

    with sync_playwright() as p:
        if connect_url:
            # Use existing Chrome with remote debugging (user's logged-in session)
            browser = p.chromium.connect_over_cdp(connect_url)
            context = browser.contexts[0] if browser.contexts else browser.new_context()
            close_browser = False  # Don't close user's browser - it's the user's Chrome
        elif user_data_dir:
            # Persistent profile - login saved between runs
            try:
                context = p.chromium.launch_persistent_context(
                    user_data_dir,
                    headless=not headed,
                    user_agent=DEFAULT_HEADERS["User-Agent"],
                    viewport={"width": 1280, "height": 720},
                    channel="chrome",
                    args=["--disable-blink-features=AutomationControlled"],
                )
            except Exception:
                context = p.chromium.launch_persistent_context(
                    user_data_dir,
                    headless=not headed,
                    user_agent=DEFAULT_HEADERS["User-Agent"],
                    viewport={"width": 1280, "height": 720},
                    args=["--disable-blink-features=AutomationControlled"],
                )
            browser = None
        else:
            try:
                browser = p.chromium.launch(headless=not headed, channel="chrome")
            except Exception:
                browser = p.chromium.launch(headless=not headed)
            context = browser.new_context(
                user_agent=DEFAULT_HEADERS["User-Agent"],
                viewport={"width": 1280, "height": 720},
            )
            if cookies_path and Path(cookies_path).exists():
                cookies_data = json.loads(Path(cookies_path).read_text())
                cookies = (
                    cookies_data
                    if isinstance(cookies_data, list)
                    else cookies_data.get("cookies", [])
                )
                if cookies:
                    context.add_cookies(cookies)

        # New page/tab - shares session with existing tabs when using --connect
        page = context.new_page()

        captured: list[dict] = []

        def on_response(response):
            if debug and "api.prizepicks" in response.url:
                print(f"  [debug] {response.status} {response.url[:80]}...")
            if "projections" in response.url and response.status == 200:
                try:
                    captured.append(response.json())
                except Exception:
                    pass

        page.on("response", on_response)

        def is_projections(r):
            return "projections" in r.url and r.status == 200

        board_url = "https://app.prizepicks.com/board"
        try:
            with page.expect_response(is_projections, timeout=45000) as resp_info:
                page.goto(
                    board_url,
                    wait_until="domcontentloaded",
                    timeout=30000,
                )
                time.sleep(10)
            resp = resp_info.value
            captured.append(resp.json())
        except Exception:
            time.sleep(5)

        if close_browser and browser:
            browser.close()
        elif user_data_dir:
            context.close()

    if captured:
        return captured[0]
    raise RuntimeError(
        "No projections response captured. Make sure you're logged in to Prize Picks in the "
        "Chrome window that was started with --remote-debugging-port=9222. "
        "Try: --debug to see which API calls are made."
    )


def fetch_projections(
    league_id: int | None = None,
    per_page: int = 250,
    single_stat: bool = True,
    client: httpx.Client | None = None,
    use_browser: bool = False,
    cookies_path: str | None = None,
    headed: bool = False,
    connect_url: str | None = None,
    user_data_dir: str | None = None,
    debug: bool = False,
) -> dict:
    """Fetch projections from Prize Picks API. Pass league_id=None to get all leagues in one request."""
    params: dict = {
        "per_page": per_page,
        "single_stat": str(single_stat).lower(),
        "game_mode": "pickem",
    }
    if league_id is not None:
        params["league_id"] = league_id
    url = f"{BASE_URL}/projections"

    if use_browser or connect_url or user_data_dir:
        return _fetch_with_playwright(
            url, params, cookies_path, headed, connect_url, user_data_dir, debug
        )

    close_client = False
    if client is None:
        client = httpx.Client(
            headers=DEFAULT_HEADERS,
            timeout=30,
            follow_redirects=True,
        )
        close_client = True

    try:
        resp = client.get(url, params=params)
        resp.raise_for_status()
        return resp.json()
    except httpx.HTTPStatusError as e:
        if e.response.status_code == 403:
            print("403 Forbidden - retrying with browser (install: pip install playwright && playwright install chromium)")
            try:
                return _fetch_with_playwright(
                    url, params, cookies_path, False, connect_url, user_data_dir, debug
                )
            except ImportError:
                raise RuntimeError(
                    "API returned 403. Install Playwright and run with --browser: "
                    "pip install playwright && playwright install chromium"
                ) from e
        raise
    finally:
        if close_client:
            client.close()


def parse_projections(response: dict) -> list[dict]:
    """Parse JSON:API response into flat list of projections.

    API returns: { "data": [projections], "included": [new_player, stat_type, ...] }
    Projection attributes: line_score, stat_type, description, start_time
    Player comes from included via relationships.new_player.data.id
    """
    data = response.get("data") or []
    included = response.get("included") or []

    # Build lookup: "type_id" -> entity
    by_type_id: dict[str, dict] = {}
    for item in included:
        if isinstance(item, dict):
            tid = f"{item.get('type', '')}_{item.get('id', '')}"
            by_type_id[tid] = item

    projections = []
    for item in data:
        if not isinstance(item, dict) or item.get("type") != "projection":
            continue

        attrs = item.get("attributes") or {}
        rels = item.get("relationships") or {}

        # Player name and team from new_player relationship (in included)
        player_name = None
        team = None
        for rel_name in ("new_player", "member", "player"):
            rel_data = rels.get(rel_name, {})
            if not isinstance(rel_data, dict):
                continue
            d = rel_data.get("data")
            if isinstance(d, dict):
                pid, ptype = d.get("id"), d.get("type", "new_player")
            elif isinstance(d, list) and d:
                pid, ptype = d[0].get("id"), d[0].get("type", "new_player")
            else:
                continue
            if pid:
                p_ent = by_type_id.get(f"{ptype}_{pid}") or by_type_id.get(f"new_player_{pid}")
                if p_ent:
                    p_attrs = p_ent.get("attributes") or {}
                    player_name = p_attrs.get("display_name") or p_attrs.get("name")
                    team = p_attrs.get("team") or p_attrs.get("team_name")
                break

        # Stat type and line come from projection attributes
        stat_type = attrs.get("stat_type", "Unknown")
        line_score = attrs.get("line_score")

        # Odds / payout info: goblin (easier, lower payout), demon (harder, higher payout), standard
        odds_type = attrs.get("odds_type", "standard")
        adjusted_odds = attrs.get("adjusted_odds", False)
        flash_sale_line = attrs.get("flash_sale_line_score")
        is_promo = attrs.get("is_promo", False)

        # Human-readable pick type
        if odds_type == "goblin":
            pick_type = "Goblin (easier, lower payout)"
        elif odds_type == "demon":
            pick_type = "Demon (harder, higher payout)"
        else:
            pick_type = "Standard"
        if flash_sale_line is not None:
            pick_type += " [Flash Sale]"
        if is_promo:
            pick_type += " [Promo]"

        projections.append({
            "player_name": player_name or "Unknown",
            "stat_type": stat_type,
            "line": line_score,
            "description": attrs.get("description", ""),
            "start_time": attrs.get("start_time"),
            "team": team,
            "odds_type": odds_type,
            "adjusted_odds": adjusted_odds,
            "pick_type": pick_type,
            "flash_sale_line": flash_sale_line,
            "is_promo": is_promo,
        })

    return projections


def _rel_id(rels: dict, name: str, alt_names: list[str] | None = None) -> int | None:
    """Extract numeric id from relationship data."""
    names = [name] + (alt_names or [])
    for n in names:
        d = rels.get(n, {}) or {}
        data = d.get("data") if isinstance(d, dict) else None
        if isinstance(data, dict) and data.get("id") is not None:
            try:
                return int(data["id"])
            except (ValueError, TypeError):
                pass
        if isinstance(data, list) and data and isinstance(data[0], dict):
            try:
                return int(data[0].get("id", 0))
            except (ValueError, TypeError):
                pass
    return None


def parse_to_projection_stage_records(response: dict) -> list[dict]:
    """Parse API response into records for prizepicks_projection_stage table."""
    data = response.get("data") or []
    records = []
    for item in data:
        if not isinstance(item, dict) or item.get("type") != "projection":
            continue

        attrs = item.get("attributes") or {}
        rels = item.get("relationships") or {}

        try:
            projection_id = int(item.get("id", 0))
        except (ValueError, TypeError):
            continue

        # Parse nullable decimals and datetimes
        def _dec(v):
            if v is None:
                return None
            try:
                return float(v)
            except (ValueError, TypeError):
                return None

        def _dto(v):
            """Parse ISO datetime (with or without offset), convert to Central time, store as naive datetime."""
            if v is None or v == "":
                return None
            s = (v if isinstance(v, str) else str(v)).strip()
            if not s:
                return None
            try:
                # e.g. "2026-03-07T18:10:00.000-05:00" (API often sends Eastern)
                dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
                if dt.tzinfo is not None:
                    # Convert to Central (America/Chicago), then store as naive
                    dt = dt.astimezone(ZoneInfo("America/Chicago")).replace(tzinfo=None)
                return dt
            except (ValueError, TypeError):
                return None

        records.append({
            "projection_id": projection_id,
            "projection_type": (attrs.get("projection_type") or "")[:50],
            "adjusted_odds": bool(attrs.get("adjusted_odds")) if attrs.get("adjusted_odds") is not None else None,
            "board_time": _dto(attrs.get("board_time")),
            "custom_image": (attrs.get("custom_image") or "")[:500] or None,
            "description": (attrs.get("description") or "")[:100] or None,
            "end_time": _dto(attrs.get("end_time")),
            "event_type": (attrs.get("event_type") or "")[:50] or None,
            "flash_sale_line_score": _dec(attrs.get("flash_sale_line_score")),
            "game_id": (attrs.get("game_id") or "")[:100] or None,
            "group_key": (attrs.get("group_key") or "")[:150] or None,
            "hr_20": bool(attrs.get("hr_20")) if attrs.get("hr_20") is not None else None,
            "in_game": bool(attrs.get("in_game")) if attrs.get("in_game") is not None else None,
            "is_live": bool(attrs.get("is_live")) if attrs.get("is_live") is not None else None,
            "is_live_scored": bool(attrs.get("is_live_scored")) if attrs.get("is_live_scored") is not None else None,
            "is_promo": bool(attrs.get("is_promo")) if attrs.get("is_promo") is not None else None,
            "line_score": _dec(attrs.get("line_score")),
            "odds_type": (attrs.get("odds_type") or "")[:50] or None,
            "projection_display_type": (attrs.get("projection_type") or "")[:100] or None,
            "rank": int(attrs.get("rank")) if attrs.get("rank") is not None else None,
            "refundable": bool(attrs.get("refundable")) if attrs.get("refundable") is not None else None,
            "start_time": _dto(attrs.get("start_time")),
            "stat_display_name": (attrs.get("stat_display_name") or "")[:100] or None,
            "stat_type_name": (attrs.get("stat_type") or "")[:100] or None,
            "status": (attrs.get("status") or "")[:50] or None,
            "today": bool(attrs.get("today")) if attrs.get("today") is not None else None,
            "tv_channel": (attrs.get("tv_channel") or "")[:50] or None,
            "updated_at": _dto(attrs.get("updated_at")),
            "duration_id": _rel_id(rels, "duration"),
            "game_rel_id": _rel_id(rels, "game"),
            "league_id": _rel_id(rels, "league"),
            "player_id": _rel_id(rels, "new_player", ["member", "player"]),
            "projection_type_id": _rel_id(rels, "projection_type"),
            "score_id": _rel_id(rels, "score"),
            "stat_type_id": _rel_id(rels, "stat_type"),
        })

    return records


def parse_to_stage_records(response: dict, league_id: int | None) -> list[dict]:
    """Parse API response into records for prizepicks_player_stage table.

    Each projection becomes one row with player info + stat_type as market.
    """
    data = response.get("data") or []
    included = response.get("included") or []

    by_type_id: dict[str, dict] = {}
    for item in included:
        if isinstance(item, dict):
            tid = f"{item.get('type', '')}_{item.get('id', '')}"
            by_type_id[tid] = item

    records = []
    seen = set()  # (player_id, market) to avoid dupes from same projection type

    for item in data:
        if not isinstance(item, dict) or item.get("type") != "projection":
            continue

        attrs = item.get("attributes") or {}
        rels = item.get("relationships") or {}

        # Resolve new_player from included
        player_ent = None
        player_id = None
        for rel_name in ("new_player", "member", "player"):
            rel_data = rels.get(rel_name, {})
            if not isinstance(rel_data, dict):
                continue
            d = rel_data.get("data")
            if isinstance(d, dict):
                pid, ptype = d.get("id"), d.get("type", "new_player")
            elif isinstance(d, list) and d:
                pid, ptype = d[0].get("id"), d[0].get("type", "new_player")
            else:
                continue
            if pid:
                player_id = str(pid)
                player_ent = by_type_id.get(f"{ptype}_{pid}") or by_type_id.get(f"new_player_{pid}")
                break

        if not player_ent:
            continue

        p_attrs = player_ent.get("attributes") or {}
        p_rels = player_ent.get("relationships") or {}

        # team_id from team_data relationship
        team_id = None
        td = p_rels.get("team_data", {})
        if isinstance(td.get("data"), dict):
            team_id = str(td["data"].get("id", "")) or None
        elif isinstance(td.get("data"), list) and td["data"]:
            team_id = str(td["data"][0].get("id", "")) or None

        # market = team city (e.g. Denver) for player table
        market = (p_attrs.get("market") or "")[:100]

        # Table PK is player_id only - one row per player; keep first seen
        if player_id in seen:
            continue
        seen.add(player_id)

        league_val = p_attrs.get("league") or ""
        if league_id is None:
            league_id_val = str(p_attrs.get("league_id") or "")
        else:
            league_id_val = str(p_attrs.get("league_id", league_id) or league_id)

        ppid_raw = p_attrs.get("ppid")
        ppid = (str(ppid_raw).strip()[:200] or None) if ppid_raw is not None else None

        records.append({
            "player_id": player_id,
            "combo": bool(p_attrs.get("combo", False)),
            "display_name": (p_attrs.get("display_name") or "")[:255],
            "image_url": (p_attrs.get("image_url") or "")[:2000],
            "jersey_number": (str(p_attrs.get("jersey_number", "") or ""))[:10],
            "league": league_val[:50],
            "market": market,
            "name": (p_attrs.get("name") or "")[:255],
            "position": (p_attrs.get("position") or "")[:50],
            "team": (p_attrs.get("team") or "")[:10],
            "team_name": (p_attrs.get("team_name") or "")[:100],
            "league_id": league_id_val[:20],
            "team_id": (team_id or "")[:20],
            "ppid": ppid,
        })

    return records


# Columns for prizepicks_game_stage / prizepicks_game (game_id PK on stage; prizepicks_game_id IDENTITY on main)
PRIZEPICKS_GAME_STAGE_COLS = [
    "game_id", "external_game_id", "created_at", "end_time", "start_time", "updated_at",
    "is_live", "status", "away_team_id", "home_team_id", "league_name", "metadata_status",
    "away_abbreviation", "home_abbreviation", "abbreviation", "market", "name",
    "primary_color", "secondary_color", "tertiary_color", "lfg_ignored_leagues", "rank", "combo",
    "display_name", "image_url", "jersey_number", "league", "league_id", "position", "team", "team_name",
    "active", "f2p_enabled", "has_live_projections", "icon", "last_five_games_enabled",
    "league_icon_id", "parent_id", "parent_name", "projections_count", "show_trending",
    "metadata_json",
]

PRIZEPICKS_LEAGUE_STAGE_COLS = [
    "league_id", "active", "f2p_enabled", "has_live_projections", "icon", "image_url",
    "last_five_games_enabled", "league_icon_id", "name", "parent_id", "parent_name",
    "projections_count", "rank", "show_trending", "projection_filters_json",
]
PRIZEPICKS_TEAM_STAGE_COLS = [
    "team_id", "abbreviation", "market", "name", "primary_color", "secondary_color", "tertiary_color",
]
PRIZEPICKS_STAT_TYPE_STAGE_COLS = ["stat_type_id", "name", "rank", "lfg_ignored_leagues_json"]
PRIZEPICKS_DURATION_STAGE_COLS = ["duration_id", "name"]
PRIZEPICKS_PROJECTION_TYPE_STAGE_COLS = ["projection_type_id", "name"]


def _game_attr_str(val, max_len: int = 50) -> str | None:
    if val is None:
        return None
    s = (str(val) or "").strip()
    return s[:max_len] if s else None


def _game_metadata_json(attrs: dict) -> str | None:
    meta = attrs.get("metadata")
    if meta is None:
        return None
    if isinstance(meta, (dict, list)):
        out = json.dumps(meta, separators=(",", ":"), default=str)
        return out if out else None
    s = (str(meta) or "").strip()
    return s if s else None


# API may use "game", "games", "Game", etc. for game entities in included
GAME_INCLUDED_TYPES = frozenset({"game", "games", "Game", "Games"})


def parse_to_game_stage_records(response: dict) -> list[dict]:
    """Parse API response included array for type 'game' (or variants) into records for prizepicks_game_stage.
    Only game-level attributes are mapped; player/team-only columns are left None."""
    included = response.get("included") or []
    by_type_id = {}
    for item in included:
        if isinstance(item, dict):
            tid = f"{item.get('type', '')}_{item.get('id', '')}"
            by_type_id[tid] = item

    records = []
    for item in included:
        if not isinstance(item, dict):
            continue
        itype = item.get("type") or ""
        attrs = item.get("attributes") or {}
        # Accept known game types, or any type with game-like attributes (start_time + team ids or description)
        is_game_type = itype in GAME_INCLUDED_TYPES
        has_start = attrs.get("start_time") is not None
        rels_check = item.get("relationships") or {}
        has_teams = (
            attrs.get("away_team_id") is not None
            or attrs.get("home_team_id") is not None
            or rels_check.get("away_team")
            or rels_check.get("home_team")
            or rels_check.get("away_team_data")
            or rels_check.get("home_team_data")
        )
        is_game_like = has_start and (has_teams or attrs.get("description"))
        if not is_game_type and not is_game_like:
            continue
        gid = item.get("id")
        if gid is None:
            continue
        game_id = str(gid).strip()[:20]
        if not game_id:
            continue
        attrs = item.get("attributes") or {}
        rels = item.get("relationships") or {}

        def _dt_str(v):
            if v is None or v == "":
                return None
            return (str(v) or "").strip()[:50] or None

        # API uses away_team_data / home_team_data (not away_team / home_team)
        away_rel = rels.get("away_team_data") or rels.get("away_team") or {}
        home_rel = rels.get("home_team_data") or rels.get("home_team") or {}
        away_data = away_rel.get("data") if isinstance(away_rel, dict) else None
        home_data = home_rel.get("data") if isinstance(home_rel, dict) else None
        if isinstance(away_data, list) and away_data:
            away_data = away_data[0]
        if isinstance(home_data, list) and home_data:
            home_data = home_data[0]
        # Raw IDs (full length) for by_type_id lookup; truncated for DB column
        away_team_id_raw = attrs.get("away_team_id") or (away_data.get("id") if isinstance(away_data, dict) else None)
        home_team_id_raw = attrs.get("home_team_id") or (home_data.get("id") if isinstance(home_data, dict) else None)
        away_team_id_raw = (str(away_team_id_raw).strip() or None) if away_team_id_raw is not None else None
        home_team_id_raw = (str(home_team_id_raw).strip() or None) if home_team_id_raw is not None else None
        away_team_id = _game_attr_str(away_team_id_raw, 20)
        home_team_id = _game_attr_str(home_team_id_raw, 20)
        away_type = away_data.get("type", "team") if isinstance(away_data, dict) else "team"
        home_type = home_data.get("type", "team") if isinstance(home_data, dict) else "team"

        away_abbrev = _game_attr_str(attrs.get("away_abbreviation"), 10)
        home_abbrev = _game_attr_str(attrs.get("home_abbreviation"), 10)
        if away_abbrev is None and away_team_id_raw:
            away_ent = by_type_id.get(f"team_{away_team_id_raw}") or by_type_id.get(f"team_data_{away_team_id_raw}") or by_type_id.get(f"{away_type}_{away_team_id_raw}")
            if away_ent:
                away_abbrev = _game_attr_str((away_ent.get("attributes") or {}).get("abbreviation"), 10)
        if home_abbrev is None and home_team_id_raw:
            home_ent = by_type_id.get(f"team_{home_team_id_raw}") or by_type_id.get(f"team_data_{home_team_id_raw}") or by_type_id.get(f"{home_type}_{home_team_id_raw}")
            if home_ent:
                home_abbrev = _game_attr_str((home_ent.get("attributes") or {}).get("abbreviation"), 10)

        def _b(v):
            if v is None:
                return None
            return bool(v)

        records.append({
            "game_id": game_id,
            "external_game_id": _game_attr_str(attrs.get("external_game_id"), 100),
            "created_at": _dt_str(attrs.get("created_at")),
            "end_time": _dt_str(attrs.get("end_time")),
            "start_time": _dt_str(attrs.get("start_time")),
            "updated_at": _dt_str(attrs.get("updated_at")),
            "is_live": _b(attrs.get("is_live")),
            "status": _game_attr_str(attrs.get("status"), 50),
            "away_team_id": away_team_id,
            "home_team_id": home_team_id,
            "league_name": _game_attr_str(attrs.get("league_name"), 50),
            "metadata_status": _game_attr_str(attrs.get("metadata_status"), 50),
            "away_abbreviation": away_abbrev,
            "home_abbreviation": home_abbrev,
            "abbreviation": None,
            "market": _game_attr_str(attrs.get("market"), 100),
            "name": _game_attr_str(attrs.get("name"), 100),
            "primary_color": _game_attr_str(attrs.get("primary_color"), 10),
            "secondary_color": _game_attr_str(attrs.get("secondary_color"), 10),
            "tertiary_color": _game_attr_str(attrs.get("tertiary_color"), 10),
            "lfg_ignored_leagues": _game_attr_str(attrs.get("lfg_ignored_leagues"), 500),
            "rank": int(attrs.get("rank")) if attrs.get("rank") is not None else None,
            "combo": _b(attrs.get("combo")),
            "display_name": None,
            "image_url": None,
            "jersey_number": None,
            "league": _game_attr_str(attrs.get("league"), 50),
            "league_id": _game_attr_str(attrs.get("league_id"), 20),
            "position": None,
            "team": None,
            "team_name": None,
            "active": _b(attrs.get("active")),
            "f2p_enabled": _b(attrs.get("f2p_enabled")),
            "has_live_projections": _b(attrs.get("has_live_projections")),
            "icon": _game_attr_str(attrs.get("icon"), 50),
            "last_five_games_enabled": _b(attrs.get("last_five_games_enabled")),
            "league_icon_id": int(attrs.get("league_icon_id")) if attrs.get("league_icon_id") is not None else None,
            "parent_id": _game_attr_str(attrs.get("parent_id"), 20),
            "parent_name": _game_attr_str(attrs.get("parent_name"), 100),
            "projections_count": int(attrs.get("projections_count")) if attrs.get("projections_count") is not None else None,
            "show_trending": _b(attrs.get("show_trending")),
            "metadata_json": _game_metadata_json(attrs),
        })

    return records


def parse_included_reference_records(resp: dict) -> dict[str, list[dict]]:
    """Extract league/team/stat_type/duration/projection_type rows from JSON:API included (last wins per id)."""
    included = resp.get("included") or []
    leagues: dict[str, dict] = {}
    teams: dict[str, dict] = {}
    stat_types: dict[str, dict] = {}
    durations: dict[str, dict] = {}
    proj_types: dict[str, dict] = {}

    def _bit(v):
        if v is None:
            return None
        return bool(v)

    def _safe_int(v):
        if v is None:
            return None
        try:
            return int(v)
        except (TypeError, ValueError):
            return None

    for item in included:
        if not isinstance(item, dict):
            continue
        itype = item.get("type") or ""
        raw_id = item.get("id")
        if raw_id is None:
            continue
        sid = str(raw_id).strip()[:20]
        attrs = item.get("attributes") or {}

        if itype == "league":
            rels = item.get("relationships") or {}
            pf = rels.get("projection_filters") or {}
            pdata = pf.get("data")
            # JSON:API: data may be null, a single resource object (dict), or an array (list).
            if pdata is None:
                pf_json = "null"
            elif isinstance(pdata, (list, dict)):
                pf_json = json.dumps(pdata)
            else:
                pf_json = json.dumps(pdata, default=str)
            parent_id = attrs.get("parent_id")
            leagues[sid] = {
                "league_id": sid,
                "active": _bit(attrs.get("active")),
                "f2p_enabled": _bit(attrs.get("f2p_enabled")),
                "has_live_projections": _bit(attrs.get("has_live_projections")),
                "icon": (str(attrs.get("icon") or "")[:50] or None),
                "image_url": (str(attrs.get("image_url") or "")[:2000] or None),
                "last_five_games_enabled": _bit(attrs.get("last_five_games_enabled")),
                "league_icon_id": _safe_int(attrs.get("league_icon_id")),
                "name": str(attrs.get("name") or sid)[:100],
                "parent_id": str(parent_id)[:20] if parent_id is not None else None,
                "parent_name": (str(attrs.get("parent_name") or "")[:100] or None),
                "projections_count": _safe_int(attrs.get("projections_count")),
                "rank": _safe_int(attrs.get("rank")),
                "show_trending": _bit(attrs.get("show_trending")),
                "projection_filters_json": pf_json,
            }
        elif itype == "team":
            teams[sid] = {
                "team_id": sid,
                "abbreviation": (str(attrs.get("abbreviation") or "")[:10] or None),
                "market": (str(attrs.get("market") or "")[:100] or None),
                "name": (str(attrs.get("name") or "")[:100] or None),
                "primary_color": (str(attrs.get("primary_color") or "")[:20] or None),
                "secondary_color": (str(attrs.get("secondary_color") or "")[:20] or None),
                "tertiary_color": (str(attrs.get("tertiary_color") or "")[:20] or None),
            }
        elif itype == "stat_type":
            lfg = attrs.get("lfg_ignored_leagues")
            lfg_json = json.dumps(lfg) if isinstance(lfg, list) else "[]"
            stat_types[sid] = {
                "stat_type_id": sid,
                "name": str(attrs.get("name") or sid)[:200],
                "rank": _safe_int(attrs.get("rank")),
                "lfg_ignored_leagues_json": lfg_json,
            }
        elif itype == "duration":
            durations[sid] = {
                "duration_id": sid,
                "name": str(attrs.get("name") or sid)[:100],
            }
        elif itype == "projection_type":
            proj_types[sid] = {
                "projection_type_id": sid,
                "name": str(attrs.get("name") or sid)[:100],
            }

    return {
        "league": list(leagues.values()),
        "team": list(teams.values()),
        "stat_type": list(stat_types.values()),
        "duration": list(durations.values()),
        "projection_type": list(proj_types.values()),
    }


def insert_game_stage(
    records: list[dict],
    server: str = "localhost\\SQLEXPRESS",
    database: str = "Props",
    user: str | None = None,
    password: str | None = None,
    trusted_connection: bool = False,
) -> int:
    """Truncate prizepicks_game_stage and insert game records. Returns count. Table must exist."""
    import pyodbc
    user = user or os.environ.get("PROPS_DB_USER", "dbadmin")
    password = password or os.environ.get("PROPS_DB_PASSWORD", "")

    conn = _get_db_conn(server, database, user, password, trusted_connection)
    cols = [c for c in PRIZEPICKS_GAME_STAGE_COLS]
    placeholders = ", ".join("?" * len(cols))
    sql_truncate = "TRUNCATE TABLE [dbo].[prizepicks_game_stage]"
    sql_insert = f"INSERT INTO [dbo].[prizepicks_game_stage] ({', '.join(cols)}) VALUES ({placeholders})"
    count = 0
    with conn:
        cursor = conn.cursor()
        cursor.execute(sql_truncate)
        for r in records:
            cursor.execute(sql_insert, [r.get(c) for c in cols])
            count += cursor.rowcount
    conn.close()
    return count


def upsert_game_from_stage(
    server: str = "localhost\\SQLEXPRESS",
    database: str = "Props",
    user: str | None = None,
    password: str | None = None,
    trusted_connection: bool = False,
) -> int:
    """MERGE prizepicks_game_stage into prizepicks_game on game_id. Returns rows affected."""
    import pyodbc
    user = user or os.environ.get("PROPS_DB_USER", "dbadmin")
    password = password or os.environ.get("PROPS_DB_PASSWORD", "")

    conn = _get_db_conn(server, database, user, password, trusted_connection)
    cols = [c for c in PRIZEPICKS_GAME_STAGE_COLS]
    cols_comma = ", ".join(cols)
    set_clauses = ", ".join(f"t.[{c}] = s.[{c}]" for c in cols if c != "game_id")
    merge_sql = f"""
        MERGE [dbo].[prizepicks_game] AS t
        USING [dbo].[prizepicks_game_stage] AS s
        ON t.game_id = s.game_id
        WHEN MATCHED THEN
            UPDATE SET {set_clauses}
        WHEN NOT MATCHED BY TARGET THEN
            INSERT ({cols_comma}) VALUES ({", ".join("s.[" + c + "]" for c in cols)});
    """
    with conn:
        cursor = conn.cursor()
        cursor.execute(merge_sql)
        count = cursor.rowcount
    conn.close()
    return count


def insert_included_reference_stages(
    ref: dict[str, list[dict]],
    server: str = "localhost\\SQLEXPRESS",
    database: str = "Props",
    user: str | None = None,
    password: str | None = None,
    trusted_connection: bool = False,
) -> dict[str, int]:
    """Truncate and load league/team/stat_type/duration/projection_type stage tables only."""
    user = user or os.environ.get("PROPS_DB_USER", "dbadmin")
    password = password or os.environ.get("PROPS_DB_PASSWORD", "")

    conn = _get_db_conn(server, database, user, password, trusted_connection)
    specs = [
        ("prizepicks_league_stage", PRIZEPICKS_LEAGUE_STAGE_COLS, ref["league"]),
        ("prizepicks_team_stage", PRIZEPICKS_TEAM_STAGE_COLS, ref["team"]),
        ("prizepicks_stat_type_stage", PRIZEPICKS_STAT_TYPE_STAGE_COLS, ref["stat_type"]),
        ("prizepicks_duration_stage", PRIZEPICKS_DURATION_STAGE_COLS, ref["duration"]),
        ("prizepicks_projection_type_stage", PRIZEPICKS_PROJECTION_TYPE_STAGE_COLS, ref["projection_type"]),
    ]
    counts: dict[str, int] = {}
    with conn:
        cursor = conn.cursor()
        for table, cols, rows in specs:
            try:
                cursor.execute(f"TRUNCATE TABLE [dbo].[{table}]")
            except Exception:
                counts[table] = -1
                continue
            if not rows:
                counts[table] = 0
                continue
            ph = ", ".join("?" * len(cols))
            sql = f"INSERT INTO [dbo].[{table}] ({', '.join(cols)}) VALUES ({ph})"
            n = 0
            for r in rows:
                cursor.execute(sql, [r.get(c) for c in cols])
                n += cursor.rowcount
            counts[table] = n
    conn.close()
    return counts


def merge_included_reference_from_stage(
    server: str = "localhost\\SQLEXPRESS",
    database: str = "Props",
    user: str | None = None,
    password: str | None = None,
    trusted_connection: bool = False,
) -> None:
    """Run MergePrizepicksIncludedReferenceFromStage (requires prizepicks_included_reference.sql applied)."""
    user = user or os.environ.get("PROPS_DB_USER", "dbadmin")
    password = password or os.environ.get("PROPS_DB_PASSWORD", "")
    conn = _get_db_conn(server, database, user, password, trusted_connection)
    with conn:
        cursor = conn.cursor()
        cursor.execute("EXEC [dbo].[MergePrizepicksIncludedReferenceFromStage]")
    conn.close()


def _get_db_conn(
    server: str,
    database: str,
    user: str,
    password: str,
    trusted_connection: bool = False,
):
    """Get pyodbc connection to SQL Server. Use trusted_connection=True for Windows auth."""
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


def ensure_projection_stage_table(conn) -> None:
    """Create prizepicks_projection_stage if it does not exist."""
    cursor = conn.cursor()
    cursor.execute("""
        IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'prizepicks_projection_stage')
        BEGIN
        CREATE TABLE [dbo].[prizepicks_projection_stage](
            [projection_id] [bigint] NOT NULL,
            [projection_type] [varchar](50) NOT NULL,
            [adjusted_odds] [bit] NULL,
            [board_time] [datetime2](0) NULL,
            [custom_image] [varchar](500) NULL,
            [description] [varchar](100) NULL,
            [end_time] [datetime2](0) NULL,
            [event_type] [varchar](50) NULL,
            [flash_sale_line_score] [decimal](10, 2) NULL,
            [game_id] [varchar](100) NULL,
            [group_key] [varchar](150) NULL,
            [hr_20] [bit] NULL,
            [in_game] [bit] NULL,
            [is_live] [bit] NULL,
            [is_live_scored] [bit] NULL,
            [is_promo] [bit] NULL,
            [line_score] [decimal](10, 2) NULL,
            [odds_type] [varchar](50) NULL,
            [projection_display_type] [varchar](100) NULL,
            [rank] [int] NULL,
            [refundable] [bit] NULL,
            [start_time] [datetime2](0) NULL,
            [stat_display_name] [varchar](100) NULL,
            [stat_type_name] [varchar](100) NULL,
            [status] [varchar](50) NULL,
            [today] [bit] NULL,
            [tv_channel] [varchar](50) NULL,
            [updated_at] [datetime2](0) NULL,
            [duration_id] [int] NULL,
            [game_rel_id] [int] NULL,
            [league_id] [int] NULL,
            [player_id] [int] NULL,
            [projection_type_id] [int] NULL,
            [score_id] [int] NULL,
            [stat_type_id] [int] NULL
        )
        END
    """)
    conn.commit()


def insert_projection_stage(
    records: list[dict],
    server: str = "localhost\\SQLEXPRESS",
    database: str = "Props",
    user: str | None = None,
    password: str | None = None,
    trusted_connection: bool = False,
) -> int:
    """Insert records into prizepicks_projection_stage. Truncates first. Returns count."""
    import pyodbc
    user = user or os.environ.get("PROPS_DB_USER", "dbadmin")
    password = password or os.environ.get("PROPS_DB_PASSWORD", "")

    conn = _get_db_conn(server, database, user, password, trusted_connection)
    ensure_projection_stage_table(conn)
    cols = [
        "projection_id", "projection_type", "adjusted_odds", "board_time", "custom_image",
        "description", "end_time", "event_type", "flash_sale_line_score", "game_id",
        "group_key", "hr_20", "in_game", "is_live", "is_live_scored", "is_promo",
        "line_score", "odds_type", "projection_display_type", "rank", "refundable",
        "start_time", "stat_display_name", "stat_type_name", "status", "today",
        "tv_channel", "updated_at", "duration_id", "game_rel_id", "league_id",
        "player_id", "projection_type_id", "score_id", "stat_type_id",
    ]
    placeholders = ", ".join("?" * len(cols))
    sql = f"""
        TRUNCATE TABLE [dbo].[prizepicks_projection_stage];
        INSERT INTO [dbo].[prizepicks_projection_stage] ({", ".join(cols)})
        VALUES ({placeholders})
    """
    count = 0
    with conn:
        cursor = conn.cursor()
        cursor.execute("TRUNCATE TABLE [dbo].[prizepicks_projection_stage]")
        for r in records:
            cursor.execute(
                f"INSERT INTO [dbo].[prizepicks_projection_stage] ({', '.join(cols)}) VALUES ({placeholders})",
                [r.get(c) for c in cols],
            )
            count += cursor.rowcount
    conn.close()
    return count


def upsert_projection_from_stage(
    server: str = "localhost\\SQLEXPRESS",
    database: str = "Props",
    user: str | None = None,
    password: str | None = None,
    trusted_connection: bool = False,
) -> tuple[int, int, int, int]:
    """Upsert from prizepicks_projection_stage to prizepicks_projection.
    When line_score changes, copies current projection row to prizepicks_projection_history.
    Moves rows whose start_time has passed to history and deletes them from projection.
    Returns (merge_rowcount, history_line_change_count, moved_to_history_count, deleted_count).
    """
    import pyodbc
    user = user or os.environ.get("PROPS_DB_USER", "dbadmin")
    password = password or os.environ.get("PROPS_DB_PASSWORD", "")

    conn = _get_db_conn(server, database, user, password, trusted_connection)

    # 1. Copy current projection to history where line_score changed and not already in history
    history_sql = """
        INSERT INTO [dbo].[prizepicks_projection_history] (
            projection_id, projection_type, adjusted_odds, board_time, custom_image,
            description, end_time, event_type, flash_sale_line_score, game_id,
            group_key, hr_20, in_game, is_live, is_live_scored, is_promo,
            line_score, odds_type, projection_display_type, rank, refundable,
            start_time, stat_display_name, stat_type_name, status, today,
            tv_channel, updated_at, duration_id, game_rel_id, league_id,
            player_id, projection_type_id, score_id, stat_type_id,
            created_at, last_modified_at
        )
        SELECT
            p.projection_id, p.projection_type, p.adjusted_odds, p.board_time, p.custom_image,
            p.description, p.end_time, p.event_type, p.flash_sale_line_score, p.game_id,
            p.group_key, p.hr_20, p.in_game, p.is_live, p.is_live_scored, p.is_promo,
            p.line_score, p.odds_type, p.projection_display_type, p.rank, p.refundable,
            p.start_time, p.stat_display_name, p.stat_type_name, p.status, p.today,
            p.tv_channel, p.updated_at, p.duration_id, p.game_rel_id, p.league_id,
            p.player_id, p.projection_type_id, p.score_id, p.stat_type_id,
            p.created_at, p.last_modified_at
        FROM [dbo].[prizepicks_projection] p
        INNER JOIN [dbo].[prizepicks_projection_stage] s ON p.projection_id = s.projection_id
        WHERE (p.line_score <> s.line_score OR (p.line_score IS NULL AND s.line_score IS NOT NULL) OR (p.line_score IS NOT NULL AND s.line_score IS NULL))
          AND NOT EXISTS (SELECT 1 FROM [dbo].[prizepicks_projection_history] h WHERE h.projection_id = p.projection_id)
    """

    # 2. Move to history any projection whose start_time has passed (then delete from projection)
    # start_time is stored in Central; compare to current Central time
    now_central_sql = "CAST(GETUTCDATE() AT TIME ZONE 'UTC' AT TIME ZONE 'Central Standard Time' AS datetime2(0))"
    move_passed_sql = f"""
        INSERT INTO [dbo].[prizepicks_projection_history] (
            projection_id, projection_type, adjusted_odds, board_time, custom_image,
            description, end_time, event_type, flash_sale_line_score, game_id,
            group_key, hr_20, in_game, is_live, is_live_scored, is_promo,
            line_score, odds_type, projection_display_type, rank, refundable,
            start_time, stat_display_name, stat_type_name, status, today,
            tv_channel, updated_at, duration_id, game_rel_id, league_id,
            player_id, projection_type_id, score_id, stat_type_id,
            created_at, last_modified_at
        )
        SELECT
            p.projection_id, p.projection_type, p.adjusted_odds, p.board_time, p.custom_image,
            p.description, p.end_time, p.event_type, p.flash_sale_line_score, p.game_id,
            p.group_key, p.hr_20, p.in_game, p.is_live, p.is_live_scored, p.is_promo,
            p.line_score, p.odds_type, p.projection_display_type, p.rank, p.refundable,
            p.start_time, p.stat_display_name, p.stat_type_name, p.status, p.today,
            p.tv_channel, p.updated_at, p.duration_id, p.game_rel_id, p.league_id,
            p.player_id, p.projection_type_id, p.score_id, p.stat_type_id,
            p.created_at, p.last_modified_at
        FROM [dbo].[prizepicks_projection] p
        WHERE p.start_time < {now_central_sql}
          AND NOT EXISTS (SELECT 1 FROM [dbo].[prizepicks_projection_history] h WHERE h.projection_id = p.projection_id);
    """
    delete_passed_sql = f"""
        DELETE FROM [dbo].[prizepicks_projection]
        WHERE start_time < {now_central_sql};
    """

    # 3. MERGE stage into projection
    merge_sql = f"""
        MERGE [dbo].[prizepicks_projection] AS t
        USING [dbo].[prizepicks_projection_stage] AS s
        ON t.projection_id = s.projection_id
        WHEN MATCHED THEN UPDATE SET
            t.projection_type = s.projection_type,
            t.adjusted_odds = s.adjusted_odds,
            t.board_time = s.board_time,
            t.custom_image = s.custom_image,
            t.description = s.description,
            t.end_time = s.end_time,
            t.event_type = s.event_type,
            t.flash_sale_line_score = s.flash_sale_line_score,
            t.game_id = s.game_id,
            t.group_key = s.group_key,
            t.hr_20 = s.hr_20,
            t.in_game = s.in_game,
            t.is_live = s.is_live,
            t.is_live_scored = s.is_live_scored,
            t.is_promo = s.is_promo,
            t.line_score = s.line_score,
            t.odds_type = s.odds_type,
            t.projection_display_type = s.projection_display_type,
            t.rank = s.rank,
            t.refundable = s.refundable,
            t.start_time = s.start_time,
            t.stat_display_name = s.stat_display_name,
            t.stat_type_name = s.stat_type_name,
            t.status = s.status,
            t.today = s.today,
            t.tv_channel = s.tv_channel,
            t.updated_at = s.updated_at,
            t.duration_id = s.duration_id,
            t.game_rel_id = s.game_rel_id,
            t.league_id = s.league_id,
            t.player_id = s.player_id,
            t.projection_type_id = s.projection_type_id,
            t.score_id = s.score_id,
            t.stat_type_id = s.stat_type_id,
            t.last_modified_at = SYSDATETIME()
        WHEN NOT MATCHED BY TARGET THEN INSERT (
            projection_id, projection_type, adjusted_odds, board_time, custom_image,
            description, end_time, event_type, flash_sale_line_score, game_id,
            group_key, hr_20, in_game, is_live, is_live_scored, is_promo,
            line_score, odds_type, projection_display_type, rank, refundable,
            start_time, stat_display_name, stat_type_name, status, today,
            tv_channel, updated_at, duration_id, game_rel_id, league_id,
            player_id, projection_type_id, score_id, stat_type_id,
            created_at, last_modified_at
        ) VALUES (
            s.projection_id, s.projection_type, s.adjusted_odds, s.board_time, s.custom_image,
            s.description, s.end_time, s.event_type, s.flash_sale_line_score, s.game_id,
            s.group_key, s.hr_20, s.in_game, s.is_live, s.is_live_scored, s.is_promo,
            s.line_score, s.odds_type, s.projection_display_type, s.rank, s.refundable,
            s.start_time, s.stat_display_name, s.stat_type_name, s.status, s.today,
            s.tv_channel, s.updated_at, s.duration_id, s.game_rel_id, s.league_id,
            s.player_id, s.projection_type_id, s.score_id, s.stat_type_id,
            SYSDATETIME(), SYSDATETIME()
        );
    """

    with conn:
        cursor = conn.cursor()
        cursor.execute(history_sql)
        history_count = cursor.rowcount
        cursor.execute(merge_sql)
        merge_count = cursor.rowcount
        cursor.execute(move_passed_sql)
        moved_count = cursor.rowcount
        cursor.execute(delete_passed_sql)
        deleted_count = cursor.rowcount
    conn.close()
    return (merge_count, history_count, moved_count, deleted_count)


def ensure_player_stage_table(conn) -> None:
    """Create prizepicks_player_stage if it does not exist."""
    cursor = conn.cursor()
    cursor.execute("""
        IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'prizepicks_player_stage')
        BEGIN
        CREATE TABLE [dbo].[prizepicks_player_stage](
            [player_id] [nvarchar](20) NOT NULL,
            [combo] [bit] NOT NULL,
            [display_name] [nvarchar](255) NOT NULL,
            [image_url] [nvarchar](2000) NULL,
            [jersey_number] [nvarchar](10) NULL,
            [league] [nvarchar](50) NULL,
            [market] [nvarchar](100) NULL,
            [name] [nvarchar](255) NOT NULL,
            [position] [nvarchar](50) NULL,
            [team] [nvarchar](10) NULL,
            [team_name] [nvarchar](100) NULL,
            [league_id] [nvarchar](20) NULL,
            [team_id] [nvarchar](20) NULL,
            [ppid] [nvarchar](200) NULL,
            CONSTRAINT [PK_prizepicks_player_stage] PRIMARY KEY ([player_id])
        )
        END
    """)
    cursor.execute("""
        IF EXISTS (SELECT 1 FROM sys.tables WHERE name = 'prizepicks_player_stage')
           AND NOT EXISTS (
               SELECT 1 FROM sys.columns
               WHERE object_id = OBJECT_ID(N'[dbo].[prizepicks_player_stage]') AND name = N'ppid'
           )
        BEGIN
            ALTER TABLE [dbo].[prizepicks_player_stage] ADD [ppid] [nvarchar](200) NULL;
        END
    """)
    conn.commit()


def insert_player_stage(
    records: list[dict],
    server: str = "localhost\\SQLEXPRESS",
    database: str = "Props",
    user: str | None = None,
    password: str | None = None,
    trusted_connection: bool = False,
) -> int:
    """Insert records into prizepicks_player_stage. Truncates first. Returns count."""
    user = user or os.environ.get("PROPS_DB_USER", "dbadmin")
    password = password or os.environ.get("PROPS_DB_PASSWORD", "")

    conn = _get_db_conn(server, database, user, password, trusted_connection)
    ensure_player_stage_table(conn)
    cols = [
        "player_id", "combo", "display_name", "image_url", "jersey_number",
        "league", "market", "name", "position", "team", "team_name",
        "league_id", "team_id", "ppid",
    ]
    placeholders = ", ".join("?" * len(cols))
    count = 0
    with conn:
        cursor = conn.cursor()
        cursor.execute("TRUNCATE TABLE [dbo].[prizepicks_player_stage]")
        for r in records:
            cursor.execute(
                f"INSERT INTO [dbo].[prizepicks_player_stage] ({', '.join(cols)}) VALUES ({placeholders})",
                [r.get(c) for c in cols],
            )
            count += cursor.rowcount
    conn.close()
    return count


def upsert_player_from_stage(
    server: str = "localhost\\SQLEXPRESS",
    database: str = "Props",
    user: str | None = None,
    password: str | None = None,
    trusted_connection: bool = False,
) -> int:
    """Upsert from prizepicks_player_stage to prizepicks_player. No history. Returns rows affected."""
    user = user or os.environ.get("PROPS_DB_USER", "dbadmin")
    password = password or os.environ.get("PROPS_DB_PASSWORD", "")

    conn = _get_db_conn(server, database, user, password, trusted_connection)
    merge_sql = """
        MERGE [dbo].[prizepicks_player] AS t
        USING [dbo].[prizepicks_player_stage] AS s
        ON t.player_id = s.player_id
        WHEN MATCHED THEN UPDATE SET
            t.combo = s.combo,
            t.display_name = s.display_name,
            t.image_url = s.image_url,
            t.jersey_number = s.jersey_number,
            t.league = s.league,
            t.market = s.market,
            t.name = s.name,
            t.position = s.position,
            t.team = s.team,
            t.team_name = s.team_name,
            t.league_id = s.league_id,
            t.team_id = s.team_id,
            t.ppid = s.ppid
        WHEN NOT MATCHED BY TARGET THEN INSERT (
            player_id, combo, display_name, image_url, jersey_number,
            league, market, name, position, team, team_name,
            league_id, team_id, ppid
        ) VALUES (
            s.player_id, s.combo, s.display_name, s.image_url, s.jersey_number,
            s.league, s.market, s.name, s.position, s.team, s.team_name,
            s.league_id, s.team_id, s.ppid
        );
    """
    with conn:
        cursor = conn.cursor()
        cursor.execute("""
            IF EXISTS (SELECT 1 FROM sys.tables WHERE name = 'prizepicks_player')
               AND NOT EXISTS (
                   SELECT 1 FROM sys.columns
                   WHERE object_id = OBJECT_ID(N'[dbo].[prizepicks_player]') AND name = N'ppid'
               )
            BEGIN
                ALTER TABLE [dbo].[prizepicks_player] ADD [ppid] [nvarchar](200) NULL;
            END
        """)
        cursor.execute(merge_sql)
        count = cursor.rowcount
    conn.close()
    return count


def fetch_and_parse(league_id: int, per_page: int = 250) -> list[dict]:
    """Fetch and parse projections in one call."""
    resp = fetch_projections(league_id, per_page=per_page)
    return parse_projections(resp)


def main():
    parser = argparse.ArgumentParser(description="Scrape Prize Picks projections")
    parser.add_argument(
        "-l", "--league",
        default="nba",
        choices=list(LEAGUES.keys()),
        help="League to scrape (default: nba). Ignored if --all-leagues.",
    )
    parser.add_argument(
        "--all-leagues",
        action="store_true",
        help="Fetch projections from every league (nba, nfl, nhl, mlb, cfb, cbb, wnba, pga, tennis, mma, epl, soccer) and upsert.",
    )
    parser.add_argument(
        "--league-id",
        type=int,
        help="Override with raw league ID (e.g. 7 for NBA)",
    )
    parser.add_argument(
        "-o", "--output",
        help="Output file path (CSV or JSON)",
    )
    parser.add_argument(
        "-n", "--per-page",
        type=int,
        default=250,
        help="Results per page (default: 250)",
    )
    parser.add_argument(
        "--browser",
        action="store_true",
        help="Use Playwright browser (use if you get 403; requires: pip install playwright && playwright install chromium)",
    )
    parser.add_argument(
        "--cookies",
        metavar="FILE",
        help="JSON file with browser cookies (use with --browser when app requires login)",
    )
    parser.add_argument(
        "--headed",
        action="store_true",
        help="Show browser window (use with --browser to log in manually)",
    )
    parser.add_argument(
        "--connect",
        metavar="URL",
        default=os.environ.get("PROPS_BROWSER_CDP"),
        help="Connect to existing Chrome: chrome.exe --remote-debugging-port=9222, then use --connect http://localhost:9222",
    )
    parser.add_argument(
        "--persistent",
        action="store_true",
        help="Use persistent profile (.playwright-prizepicks) - login saved between runs",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Print API URLs seen (for troubleshooting 403/capture issues)",
    )
    parser.add_argument(
        "--save-response",
        metavar="FILE",
        help="Save raw API response (data + included) to a JSON file (e.g. response.json)",
    )
    parser.add_argument(
        "--db",
        action="store_true",
        help="Upsert projections and players to DB (projection→stage→projection with history; player→stage→player no history)",
    )
    parser.add_argument(
        "--db-server",
        default="localhost\\SQLEXPRESS",
        help="SQL Server instance (default: localhost\\SQLEXPRESS)",
    )
    parser.add_argument(
        "--db-user",
        default=os.environ.get("PROPS_DB_USER", "dbadmin"),
        help="Database user (default: dbadmin or PROPS_DB_USER)",
    )
    parser.add_argument(
        "--db-password",
        default=os.environ.get("PROPS_DB_PASSWORD", ""),
        help="Database password (set PROPS_DB_PASSWORD or use --trusted-connection for Windows auth)",
    )
    parser.add_argument(
        "--trusted-connection",
        action="store_true",
        help="Use Windows Authentication (no user/password)",
    )
    args = parser.parse_args()

    connect_url = args.connect if args.connect else None
    user_data_dir = ".playwright-prizepicks" if args.persistent else None

    if args.all_leagues:
        # One request with no league_id to get all projections (no per-league filtering)
        print("Fetching all projections (single request, no league filter)...")
        try:
            resp = fetch_projections(
                league_id=None,
                per_page=args.per_page,
                use_browser=args.browser,
                cookies_path=args.cookies,
                headed=args.headed,
                connect_url=connect_url,
                user_data_dir=user_data_dir,
                debug=args.debug,
            )
        except Exception as e:
            print(f"Single-request failed: {e}")
            resp = None
        if not resp:
            print("No response.")
            return 1
        projections = parse_projections(resp)
        all_projection_records = parse_to_projection_stage_records(resp)
        # league_id=None: parser uses league from each projection/player in response
        player_records_list = parse_to_stage_records(resp, None)
        all_player_records = {pr.get("player_id", ""): pr for pr in player_records_list}
        if not projections:
            print("No projections in response (API may require league_id).")
            return 1
        print(f"Total: {len(projections)} projections, {len(all_player_records)} unique players")
    else:
        league_id = args.league_id or LEAGUES.get(args.league, 7)
        print(f"Fetching projections for league_id={league_id} ({args.league})...")
        resp = fetch_projections(
            league_id,
            per_page=args.per_page,
            use_browser=args.browser,
            cookies_path=args.cookies,
            headed=args.headed,
            connect_url=connect_url,
            user_data_dir=user_data_dir,
            debug=args.debug,
        )
        projections = parse_projections(resp)
        if not projections:
            print("No projections found. The API structure may have changed.")
            print("Raw response sample (first 500 chars):")
            resp = fetch_projections(
                league_id,
                per_page=10,
                use_browser=args.browser,
                cookies_path=args.cookies,
                headed=args.headed,
                connect_url=connect_url,
                user_data_dir=user_data_dir,
                debug=args.debug,
            )
            print(json.dumps(resp, indent=2)[:500])
            return 1
        print(f"Found {len(projections)} projections")

    if getattr(args, "save_response", None):
        path = Path(args.save_response)
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(resp, f, indent=2, default=str)
        print(f"Saved raw API response to {path}")

    if args.db:
        if args.all_leagues:
            records = all_projection_records
            player_records = list(all_player_records.values())
        else:
            records = parse_to_projection_stage_records(resp)
            player_records = parse_to_stage_records(resp, league_id)
        if not records:
            print("No projection records to upsert.")
            return 1
        try:
            trusted = getattr(args, "trusted_connection", False) or os.environ.get("PROPS_DB_USE_TRUSTED_CONNECTION", "").strip().lower() in ("1", "true", "yes")
            n = insert_projection_stage(
                records,
                server=args.db_server,
                database="Props",
                user=args.db_user,
                password=args.db_password,
                trusted_connection=trusted,
            )
            print(f"Loaded {n} rows into prizepicks_projection_stage")
            merge_count, history_count, moved_count, deleted_count = upsert_projection_from_stage(
                server=args.db_server,
                database="Props",
                user=args.db_user,
                password=args.db_password,
                trusted_connection=trusted,
            )
            print(f"Upserted {merge_count} rows to prizepicks_projection ({history_count} archived on line change, {moved_count} moved to history for passed start_time, {deleted_count} deleted)")
            pn = 0
            p_merge = 0
            if player_records:
                pn = insert_player_stage(
                    player_records,
                    server=args.db_server,
                    database="Props",
                    user=args.db_user,
                    password=args.db_password,
                    trusted_connection=trusted,
                )
                print(f"Loaded {pn} rows into prizepicks_player_stage")
                p_merge = upsert_player_from_stage(
                    server=args.db_server,
                    database="Props",
                    user=args.db_user,
                    password=args.db_password,
                    trusted_connection=trusted,
                )
                print(f"Upserted {p_merge} rows to prizepicks_player")
            game_records = parse_to_game_stage_records(resp)
            if not game_records:
                included = resp.get("included") or []
                types_seen = sorted({(i.get("type") or "(none)") for i in included if isinstance(i, dict)})
                print(f"No game entities parsed from API. Included types in response: {types_seen}")
            if game_records:
                gn = insert_game_stage(
                    game_records,
                    server=args.db_server,
                    database="Props",
                    user=args.db_user,
                    password=args.db_password,
                    trusted_connection=trusted,
                )
                print(f"Loaded {gn} rows into prizepicks_game_stage")
                g_merge = upsert_game_from_stage(
                    server=args.db_server,
                    database="Props",
                    user=args.db_user,
                    password=args.db_password,
                    trusted_connection=trusted,
                )
                print(f"Upserted {g_merge} rows to prizepicks_game")
            else:
                gn = 0
                g_merge = 0

            ref = parse_included_reference_records(resp)
            ref_counts = insert_included_reference_stages(
                ref,
                server=args.db_server,
                database="Props",
                user=args.db_user,
                password=args.db_password,
                trusted_connection=trusted,
            )
            print(f"Loaded included reference stages: {ref_counts}")
            merge_included_reference_from_stage(
                server=args.db_server,
                database="Props",
                user=args.db_user,
                password=args.db_password,
                trusted_connection=trusted,
            )
            print("MergePrizepicksIncludedReferenceFromStage completed")
        except Exception as e:
            print(f"DB failed: {e}")
            import traceback
            traceback.print_exc()
            return 1

    if args.output:
        out = Path(args.output)
        out.parent.mkdir(parents=True, exist_ok=True)
        if out.suffix.lower() == ".json":
            with open(out, "w") as f:
                json.dump(projections, f, indent=2)
        else:
            import pandas as pd
            df = pd.DataFrame(projections)
            df.to_csv(out, index=False)
        print(f"Saved to {out}")
    elif not args.all_leagues:
        for p in projections[:20]:
            print(p)
        if len(projections) > 20 and not args.all_leagues:
            print(f"... and {len(projections) - 20} more")

    return 0


if __name__ == "__main__":
    exit(main())
