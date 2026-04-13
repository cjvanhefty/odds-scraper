"""
FastAPI app for projections UI.
Run from repo root: uvicorn app.main:app --reload
"""

import logging
import os
import subprocess
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from fastapi import FastAPI, Query, HTTPException, Body
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from starlette.middleware.gzip import GZipMiddleware
from pydantic import BaseModel

logger = logging.getLogger(__name__)

# When True, resolve_nba_player_ids will call nba_api if [player] table is empty/missing.
# Set PROPS_USE_NBA_API_FALLBACK=1 to enable when DB mapping is not yet populated (slower, external dependency).
USE_NBA_API_FALLBACK = os.environ.get("PROPS_USE_NBA_API_FALLBACK", "").strip().lower() in ("1", "true", "yes")

PLAYER_MAPPING_WARNING = (
    "Player mapping unavailable; streak data may be empty. "
    "Ensure prizepicks_player.nba_player_id or [player] table is populated. "
    "Or set PROPS_USE_NBA_API_FALLBACK=1 to use nba_api as fallback."
)

# Repo root on path for imports
REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from projection_over_streak import (
    get_projections,
    resolve_nba_player_ids,
    enrich_projections_with_streak,
)

from app.db import get_conn

from cross_book_stat_normalize import normalize_for_join, parlay_match_league_id_for_prizepicks


def _lookup_parlay_line(
    scoped: dict,
    fallback: dict,
    pp_league_id,
    display_name: str,
    norm_stat: str,
    game_date: str,
):
    """Prefer Parlay rows in the Parlay league that maps from PrizePicks league_id; else name+stat+date."""
    pl = parlay_match_league_id_for_prizepicks(pp_league_id)
    if pl is not None:
        v = scoped.get((pl, display_name, norm_stat, game_date))
        if v is not None:
            return v
    return fallback.get((display_name, norm_stat, game_date))


def _json_safe(val):
    """Coerce value to JSON-serializable type (e.g. Decimal -> float, for API responses)."""
    if val is None:
        return None
    if isinstance(val, (bool, str)):
        return val
    if isinstance(val, int) and not isinstance(val, bool):
        return int(val)
    if hasattr(val, "__float__") and not isinstance(val, bool):
        try:
            return float(val)
        except (TypeError, ValueError):
            return None
    if isinstance(val, (list, tuple)):
        return [_json_safe(x) for x in val]
    if isinstance(val, dict):
        return {k: _json_safe(v) for k, v in val.items()}
    return val


def _parlay_slate_date_bounds(projections: list) -> tuple[str, str] | None:
    """Min/max calendar dates from PrizePicks start_time, expanded ±1 day for TZ/slate edges."""
    ds: list[date] = []
    for p in projections:
        st = p.get("start_time")
        if not st:
            continue
        s = str(st).strip()
        day = s[:10]
        if len(day) != 10 or day[4] != "-" or day[7] != "-":
            continue
        try:
            y, m, d = int(day[:4]), int(day[5:7]), int(day[8:10])
            ds.append(date(y, m, d))
        except ValueError:
            continue
    if not ds:
        return None
    lo, hi = min(ds), max(ds)
    lo = lo - timedelta(days=1)
    hi = hi + timedelta(days=1)
    return (lo.isoformat(), hi.isoformat())


SPORTSBOOK_KEYS = ("prizepicks", "underdog", "parlay_play")


def _serialize_datetime(val):
    if val is None:
        return None
    if isinstance(val, datetime):
        return val.replace(tzinfo=None).isoformat()
    if hasattr(val, "isoformat"):
        try:
            return val.isoformat()
        except Exception:
            return str(val)
    return str(val)


def _projection_choice_score(sportsbook: str, odds_type: str | None, start_time_val) -> tuple[int, str]:
    sb = (sportsbook or "").strip().lower()
    odds = (odds_type or "").strip().lower()
    if sb == "prizepicks":
        odds_rank = 0 if odds == "standard" else 1
    elif sb == "parlay_play":
        odds_rank = 0 if odds in ("main", "") else 1
    else:
        odds_rank = 0
    return (odds_rank, _serialize_datetime(start_time_val) or "")


def _fetch_unified_projection_rows(
    conn,
    league_id_list: list[int] | None,
    include_all_odds: bool,
    player_name: str | None,
    active_only: bool,
) -> list[dict]:
    now_central = "CAST(GETUTCDATE() AT TIME ZONE 'UTC' AT TIME ZONE 'Central Standard Time' AS datetime2(0))"
    where_parts = [
        "sp.player_name IS NOT NULL",
        "LTRIM(RTRIM(sp.player_name)) <> N''",
        f"sp.start_time >= DATEADD(day, -30, CAST({now_central} AS DATE))",
    ]
    params: list = []
    if active_only:
        where_parts.append(f"sp.start_time >= {now_central}")
    if not include_all_odds:
        where_parts.append(
            "("
            "(sp.sportsbook <> N'prizepicks' OR LOWER(LTRIM(RTRIM(COALESCE(sp.odds_type, N'')))) = N'standard') "
            "AND "
            "(sp.sportsbook <> N'parlay_play' OR LOWER(LTRIM(RTRIM(COALESCE(sp.odds_type, N'main')))) = N'main')"
            ")"
        )
    if league_id_list:
        placeholders = ",".join("?" * len(league_id_list))
        where_parts.append(f"sp.league_id IN ({placeholders})")
        params.extend(league_id_list)
    if player_name and player_name.strip():
        where_parts.append("LOWER(LTRIM(RTRIM(sp.player_name))) = LOWER(?)")
        params.append(player_name.strip())
    where_sql = " AND ".join(where_parts)
    sql = f"""
        SELECT
            sp.sportsbook,
            sp.external_projection_id,
            sp.source_player_id,
            sp.source_game_id,
            sp.player_name,
            sp.stat_type_name,
            sp.line_score,
            sp.odds_type,
            sp.start_time,
            sp.league_id,
            sp.team,
            sp.team_name,
            sp.home_abbreviation,
            sp.away_abbreviation,
            sp.opponent_abbreviation,
            sp.home_away,
            sp.event_name
        FROM [dbo].[sportsbook_projection] sp
        WHERE {where_sql}
        ORDER BY sp.start_time, sp.player_name, sp.stat_type_name, sp.sportsbook, sp.external_projection_id
    """
    cursor = conn.cursor()
    cursor.execute(sql, params)
    columns = [c[0] for c in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


def _group_player_projections(rows: list[dict], stat_type_filter: str | None) -> list[dict]:
    wanted_stat = normalize_for_join((stat_type_filter or "").strip()) if stat_type_filter and stat_type_filter.strip() else None
    grouped: dict[str, dict] = {}
    player_order: list[str] = []
    for row in rows:
        player_name = (row.get("player_name") or "").strip()
        if not player_name:
            continue
        stat_type_name = (row.get("stat_type_name") or "").strip()
        stat_key = normalize_for_join(stat_type_name)
        if not stat_key:
            continue
        if wanted_stat and stat_key != wanted_stat:
            continue
        pkey = player_name.lower()
        player_item = grouped.get(pkey)
        if player_item is None:
            player_item = {
                "display_name": player_name,
                "player": player_name,
                "team": row.get("team"),
                "team_name": row.get("team_name"),
                "league_id": row.get("league_id"),
                "game": {
                    "start_time": _serialize_datetime(row.get("start_time")),
                    "event_name": row.get("event_name"),
                    "home_abbreviation": row.get("home_abbreviation"),
                    "away_abbreviation": row.get("away_abbreviation"),
                    "opponent_abbreviation": row.get("opponent_abbreviation"),
                    "home_away": row.get("home_away"),
                },
                "__stats": {},
                "__source_rank": 999,
            }
            grouped[pkey] = player_item
            player_order.append(pkey)
        sportsbook = (row.get("sportsbook") or "").strip().lower()
        rank = 0 if sportsbook == "prizepicks" else (1 if sportsbook == "underdog" else (2 if sportsbook == "parlay_play" else 3))
        if rank < player_item["__source_rank"]:
            player_item["team"] = row.get("team")
            player_item["team_name"] = row.get("team_name")
            player_item["league_id"] = row.get("league_id")
            player_item["game"] = {
                "start_time": _serialize_datetime(row.get("start_time")),
                "event_name": row.get("event_name"),
                "home_abbreviation": row.get("home_abbreviation"),
                "away_abbreviation": row.get("away_abbreviation"),
                "opponent_abbreviation": row.get("opponent_abbreviation"),
                "home_away": row.get("home_away"),
            }
            player_item["__source_rank"] = rank
        stat_item = player_item["__stats"].get(stat_key)
        if stat_item is None:
            stat_item = {
                "stat_type_name": stat_type_name,
                "stat_type_key": stat_key,
                "sportsbook_projections": {k: None for k in SPORTSBOOK_KEYS},
                "__book_scores": {},
            }
            player_item["__stats"][stat_key] = stat_item
        elif sportsbook == "prizepicks":
            stat_item["stat_type_name"] = stat_type_name
        if sportsbook not in SPORTSBOOK_KEYS:
            continue
        payload = {
            "projection_id": _json_safe(row.get("external_projection_id")),
            "line_score": _json_safe(row.get("line_score")),
            "odds_type": row.get("odds_type"),
            "source_player_id": row.get("source_player_id"),
            "source_game_id": row.get("source_game_id"),
            "start_time": _serialize_datetime(row.get("start_time")),
            "league_id": row.get("league_id"),
            "event_name": row.get("event_name"),
        }
        score = _projection_choice_score(sportsbook, row.get("odds_type"), row.get("start_time"))
        existing = stat_item["__book_scores"].get(sportsbook)
        if existing is None or score < existing:
            stat_item["sportsbook_projections"][sportsbook] = payload
            stat_item["__book_scores"][sportsbook] = score
    out: list[dict] = []
    for pkey in player_order:
        player_item = grouped[pkey]
        stats: list[dict] = []
        for stat in player_item["__stats"].values():
            stat.pop("__book_scores", None)
            stats.append(stat)
        stats.sort(key=lambda s: ((s.get("stat_type_name") or ""), (s.get("stat_type_key") or "")))
        player_item.pop("__stats", None)
        player_item.pop("__source_rank", None)
        player_item["projections"] = stats
        out.append(player_item)
    out.sort(key=lambda r: ((r.get("game", {}).get("start_time") or ""), (r.get("display_name") or "")))
    return out


def get_parlay_play_lines_by_match(
    conn,
    date_from: str | None = None,
    date_to: str | None = None,
) -> tuple[dict, dict]:
    """Return (scoped, fallback) dicts for Parlay Play lines keyed for PrizePicks joins.

    scoped: (parlay_match_league_id, display_name, norm_stat, game_date) -> line_score
    fallback: (display_name, norm_stat, game_date) -> line_score (last wins; same main-line preference as scoped)

    When date_from/date_to are set (YYYY-MM-DD), only Parlay Play rows in that Central-date range are read.
    When both are None, uses a rolling ~45-day window to avoid scanning the full history table.
    """
    cursor = conn.cursor()
    try:
        # Some deployments use `{table}_id` instead of `id` as the PK column.
        cursor.execute(
            """
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = 'dbo'
              AND TABLE_NAME = 'parlay_play_match'
              AND COLUMN_NAME IN ('id', 'parlay_play_match_id')
            """
        )
        match_pk_cols = {r[0] for r in cursor.fetchall()}
        if "id" in match_pk_cols:
            match_pk = "id"
        elif "parlay_play_match_id" in match_pk_cols:
            match_pk = "parlay_play_match_id"
        else:
            raise RuntimeError(
                "Cannot resolve PK column for dbo.parlay_play_match. "
                "Expected 'id' or 'parlay_play_match_id'."
            )

        if date_from is None or date_to is None:
            end = datetime.now(timezone.utc).date()
            start = end - timedelta(days=45)
            date_from = start.isoformat()
            date_to = (end + timedelta(days=2)).isoformat()
        # Include alt lines: 3-PT attempts are often is_main_line=0 while sharing a parent "made threes" market.
        # Rows are merged into dicts with last-write-wins. ORDER BY main-last (non-main rows first) so the
        # final line_score per key is is_main_line=1, matching PrizePicks/Underdog standard odds.
        cursor.execute(
            f"""
            SELECT m.league_id, p.display_name, p.stat_type_name,
                   CONVERT(varchar(10), CAST(p.start_time AT TIME ZONE 'Central Standard Time' AS datetime2(0)), 120) AS game_date,
                   p.line_score
            FROM [dbo].[parlay_play_projection] p
            INNER JOIN [dbo].[parlay_play_match] m ON m.[{match_pk}] = p.match_id
            WHERE p.display_name IS NOT NULL AND p.stat_type_name IS NOT NULL
              AND CONVERT(date, CAST(p.start_time AT TIME ZONE 'Central Standard Time' AS datetime2(0))) >= ?
              AND CONVERT(date, CAST(p.start_time AT TIME ZONE 'Central Standard Time' AS datetime2(0))) <= ?
            ORDER BY m.league_id, p.display_name, p.stat_type_name,
                     CONVERT(varchar(10), CAST(p.start_time AT TIME ZONE 'Central Standard Time' AS datetime2(0)), 120),
                     CASE WHEN p.is_main_line = 1 THEN 1 ELSE 0 END ASC
            """,
            (date_from, date_to),
        )
        scoped: dict = {}
        fallback: dict = {}
        for row in cursor.fetchall():
            plid = int(row[0])
            name = (row[1] or "").strip()
            stat = normalize_for_join((row[2] or "").strip())
            date_part = (row[3] or "")[:10]
            line = float(row[4]) if row[4] is not None else None
            if not name or not stat or not date_part:
                continue
            sk = (plid, name, stat, date_part)
            scoped[sk] = line
            k2 = (name, stat, date_part)
            fallback[k2] = line
        return scoped, fallback
    except Exception:
        return {}, {}
    finally:
        cursor.close()


app = FastAPI(title="Projections API", version="0.1.0")

# CORS for web frontend
from fastapi.middleware.cors import CORSMiddleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.add_middleware(GZipMiddleware, minimum_size=500)

# Static files (frontend) - mount after routes so /api takes precedence
STATIC_DIR = REPO_ROOT / "app" / "static"
if STATIC_DIR.exists():
    app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


def _parse_league_ids(league_id: int, league_ids: str | None) -> list[int]:
    """Parse league_id / league_ids into a list for get_projections."""
    if league_ids and league_ids.strip():
        try:
            return [int(x.strip()) for x in league_ids.split(",") if x.strip()]
        except ValueError:
            return [7]
    return [league_id]


def _get_projections_count(league_id: int, league_ids: str | None):
    """Shared logic for projection count. Respects league_id and league_ids."""
    league_id_list = _parse_league_ids(league_id, league_ids)
    conn = None
    try:
        conn = get_conn()
        projections = get_projections(
            conn, league_id=league_id_list, odds_type="standard", active_only=True
        )
        return {"count": len(projections), "league_ids": league_id_list}
    finally:
        if conn:
            conn.close()


@app.get("/api/projections/count")
@app.get("/api/projections-count")
def get_projections_count(
    league_id: int = Query(7, description="League id (7=NBA). Ignored if league_ids set."),
    league_ids: str | None = Query(None, description="Comma-separated league_ids"),
):
    """Return how many projection rows are in the DB (same date/league filter as list). Use to confirm data is loaded."""
    try:
        return _get_projections_count(league_id, league_ids)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


STREAK_GAMES = 5


@app.get("/api/projections/streak")
def get_projections_streak(
    player_name: str = Query(..., description="Player display name (e.g. for modal lazy-load)"),
):
    """Return streak enrichment (last 5 games) for one player. Frontend merges by projection_id for lazy-load modal."""
    conn = None
    try:
        conn = get_conn()
        projections = get_projections(
            conn,
            league_id=None,
            odds_type=None,
            active_only=False,
        )
        pn = (player_name or "").strip()
        if not pn:
            return []
        projections = [p for p in projections if (p.get("display_name") or p.get("pp_name") or "").strip() == pn]
        if not projections:
            return []
        name_to_nba_id = resolve_nba_player_ids(conn, use_api_fallback=USE_NBA_API_FALLBACK)
        projections = enrich_projections_with_streak(conn, projections, name_to_nba_id, STREAK_GAMES)
        out = []
        for r in projections:
            out.append({
                "projection_id": _json_safe(r.get("projection_id")),
                "display_name": (r.get("display_name") or r.get("pp_name") or "").strip(),
                "stat_type_name": (r.get("stat_type_name") or "").strip(),
                "favored": r.get("favored"),
                "risk": r.get("risk"),
                "cushion": _json_safe(r.get("cushion")),
                "last_n_values": _json_safe(r.get("last_n_values") or []),
                "last_n_dates": r.get("last_n_dates") or [],
                "last_n_opponents": r.get("last_n_opponents") or [],
                "last_n_projection_lines": _json_safe(r.get("last_n_projection_lines") or []),
                "streak_games": STREAK_GAMES,
            })
        return out
    except Exception as e:
        logger.exception("get_projections_streak failed")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if conn:
            conn.close()


@app.get("/api/projections")
def list_projections(
    stat_type: str | None = Query(None, description="Filter by stat type (e.g. Points, Rebounds)"),
    league_id: int = Query(7, description="PrizePicks league_id (7 = NBA). Ignored if league_ids is set."),
    league_ids: str | None = Query(None, description="Comma-separated PrizePicks league_ids (e.g. 7,9,82 for NBA, NFL, Soccer)"),
    include_all_odds: bool = Query(False, description="Include all odds types (standard, demon, goblin) for modals"),
    player_name: str | None = Query(None, description="Filter to one player (e.g. for modal)"),
    skip_streak: bool = Query(False, description="If True, omit streak enrichment (faster; use with player_name for lazy-load modal)"),
    page: int = Query(1, ge=1, description="Page number (used when page_size > 0)"),
    page_size: int = Query(0, ge=0, le=500, description="Rows per page; 0 = return all (no paging)"),
    full_list: bool = Query(False, description="If True and not filtering by player, return all projections (no dedupe) for client-side filtering"),
    use_unified_projection_store: bool = Query(False, description="Read from sportsbook_projection unified table."),
):
    """Return PrizePicks projections with favored/risk from last N games. When page_size > 0, returns { items, total, page, page_size }.
    When league_ids is omitted or empty, returns all leagues (for grid client-side sport filter). When set, filters by those leagues."""
    conn = None
    try:
        league_id_list = None
        if league_ids and league_ids.strip():
            league_id_list = _parse_league_ids(league_id, league_ids)
        conn = get_conn()
        active_only = not (player_name and player_name.strip())
        if use_unified_projection_store:
            unified_rows = _fetch_unified_projection_rows(
                conn=conn,
                league_id_list=league_id_list,
                include_all_odds=include_all_odds,
                player_name=player_name,
                active_only=active_only,
            )
            grouped = _group_player_projections(unified_rows, stat_type)
            if page_size > 0:
                total = len(grouped)
                start = (page - 1) * page_size
                end = start + page_size
                items = grouped[start:end]
                body = {"items": items, "total": total, "page": page, "page_size": page_size}
                return JSONResponse(content=body)
            return JSONResponse(content=grouped)
        projections = get_projections(
            conn,
            league_id=league_id_list,
            odds_type=None if include_all_odds else "standard",
            active_only=active_only,
        )
        if stat_type and stat_type.strip():
            projections = [p for p in projections if (p.get("stat_type_name") or "").strip() == stat_type.strip()]
        if player_name and player_name.strip():
            pn = player_name.strip()
            projections = [p for p in projections if (p.get("display_name") or p.get("pp_name") or "").strip() == pn]
        # Run streak enrichment only when opening modal (single player) and not skip_streak; grid loads fast with empty streak fields
        streak_mapping_unavailable = False
        if player_name and player_name.strip() and not skip_streak:
            name_to_nba_id = resolve_nba_player_ids(conn, use_api_fallback=USE_NBA_API_FALLBACK)
            if not name_to_nba_id and projections:
                logger.warning(
                    "Modal requested but NBA player mapping is empty; streak data may be missing. %s",
                    PLAYER_MAPPING_WARNING,
                )
                streak_mapping_unavailable = True
            projections = enrich_projections_with_streak(conn, projections, name_to_nba_id, STREAK_GAMES)
        elif player_name and player_name.strip() and skip_streak:
            projections = [
                {
                    **dict(p),
                    "favored": False,
                    "risk": None,
                    "cushion": None,
                    "last_n_values": [],
                    "last_n_dates": [],
                    "last_n_opponents": [],
                    "last_n_projection_lines": [],
                    "streak_games": STREAK_GAMES,
                }
                for p in projections
            ]
        else:
            projections = [
                {
                    **dict(p),
                    "favored": False,
                    "risk": None,
                    "cushion": None,
                    "last_n_values": [],
                    "last_n_dates": [],
                    "last_n_opponents": [],
                    "last_n_projection_lines": [],
                    "streak_games": STREAK_GAMES,
                }
                for p in projections
            ]
        if not projections:
            parlay_play_scoped, parlay_play_fallback = {}, {}
        else:
            slate_bounds = _parlay_slate_date_bounds(projections)
            if slate_bounds:
                d0, d1 = slate_bounds
                parlay_play_scoped, parlay_play_fallback = get_parlay_play_lines_by_match(
                    conn, date_from=d0, date_to=d1
                )
            else:
                parlay_play_scoped, parlay_play_fallback = get_parlay_play_lines_by_match(conn)
        out = []
        for r in projections:
            r = dict(r)
            row = {}
            for k, v in r.items():
                if hasattr(v, "__float__") and not isinstance(v, (bool, int)):
                    row[k] = float(v) if v is not None else None
                else:
                    row[k] = v
            # line_underdog comes from get_projections (join via [player].underdog_player_id)
            display_name = (r.get("display_name") or r.get("pp_name") or "").strip()
            stat_type_name = normalize_for_join((r.get("stat_type_name") or "").strip())
            start_time = r.get("start_time")
            game_date = (str(start_time)[:10] if start_time else "") or ""
            row["line_parlay_play"] = _lookup_parlay_line(
                parlay_play_scoped,
                parlay_play_fallback,
                r.get("league_id"),
                display_name,
                stat_type_name,
                game_date,
            )
            # Opponent and H/A from prizepicks_game when available
            home_abbrev = (r.get("home_abbreviation") or "").strip()
            away_abbrev = (r.get("away_abbreviation") or "").strip()
            player_team = (r.get("team") or r.get("team_name") or "").strip()
            if home_abbrev and away_abbrev and player_team:
                ph, pa = player_team.upper(), away_abbrev.upper()
                hh, ha = home_abbrev.upper(), away_abbrev.upper()
                if ph == hh or (hh in ph) or (ph in hh):
                    row["opponent_abbreviation"] = away_abbrev
                    row["home_away"] = "H"
                elif ph == ha or (ha in ph) or (ph in ha):
                    row["opponent_abbreviation"] = home_abbrev
                    row["home_away"] = "A"
                else:
                    row["opponent_abbreviation"] = None
                    row["home_away"] = None
            else:
                row["opponent_abbreviation"] = None
                row["home_away"] = None
            out.append(row)
        # One row per player (earliest game): when not loading modal and not full_list, dedupe by display_name.
        # full_list: return all rows for client-side stat filter; otherwise one row per player.
        if not (player_name and player_name.strip()):
            out.sort(
                key=lambda r: (
                    r.get("start_time") or "",
                    (r.get("display_name") or r.get("pp_name") or "").strip(),
                    (r.get("stat_type_name") or "").strip(),
                    str(r.get("projection_id") or ""),
                )
            )
            if not full_list:
                seen_players: set[str] = set()
                deduped: list[dict] = []
                for row in out:
                    name = (row.get("display_name") or row.get("pp_name") or "").strip()
                    if name in seen_players:
                        continue
                    seen_players.add(name)
                    deduped.append(row)
                out = deduped
        if page_size > 0:
            total = len(out)
            start = (page - 1) * page_size
            end = start + page_size
            items = out[start:end]
            body = {"items": items, "total": total, "page": page, "page_size": page_size}
            headers = {"X-Projections-Warning": PLAYER_MAPPING_WARNING} if streak_mapping_unavailable else {}
            return JSONResponse(content=body, headers=headers)
        headers = {"X-Projections-Warning": PLAYER_MAPPING_WARNING} if streak_mapping_unavailable else {}
        return JSONResponse(content=out, headers=headers)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if conn:
            conn.close()


@app.get("/api/projections/last-five")
def get_last_five_batch(
    league_id: int = Query(7, description="League id (7=NBA). Ignored if league_ids set."),
    league_ids: str | None = Query(None, description="Comma-separated league_ids (e.g. 7 for NBA)."),
):
    """Return last 5 game stats for every (player, stat) in the given league(s). Client merges by (display_name, stat_type_name)."""
    conn = None
    try:
        conn = get_conn()
        league_id_list = _parse_league_ids(league_id, league_ids)
        if not league_id_list:
            return []
        projections = get_projections(
            conn,
            league_id=league_id_list,
            odds_type=None,
            active_only=True,
        )
        if not projections:
            return []
        name_to_nba_id = resolve_nba_player_ids(conn, use_api_fallback=USE_NBA_API_FALLBACK)
        if not name_to_nba_id:
            logger.warning(
                "Last-five batch: NBA player mapping is empty; all streak data may be missing. %s",
                PLAYER_MAPPING_WARNING,
            )
        enriched = enrich_projections_with_streak(
            conn, projections, name_to_nba_id, STREAK_GAMES
        )
        out = []
        seen_key: set[tuple[str, str]] = set()
        for r in enriched:
            key = (
                (r.get("display_name") or r.get("pp_name") or "").strip(),
                (r.get("stat_type_name") or "").strip(),
            )
            if key in seen_key:
                continue
            seen_key.add(key)
            out.append({
                "display_name": (r.get("display_name") or r.get("pp_name") or "").strip(),
                "stat_type_name": (r.get("stat_type_name") or "").strip(),
                "favored": r.get("favored"),
                "risk": r.get("risk"),
                "cushion": _json_safe(r.get("cushion")),
                "last_n_values": _json_safe(r.get("last_n_values") or []),
                "last_n_dates": r.get("last_n_dates") or [],
                "last_n_opponents": r.get("last_n_opponents") or [],
                "last_n_projection_lines": _json_safe(r.get("last_n_projection_lines") or []),
                "streak_games": STREAK_GAMES,
            })
        headers = {"X-Projections-Warning": PLAYER_MAPPING_WARNING} if not name_to_nba_id else {}
        return JSONResponse(content=out, headers=headers)
    except Exception as e:
        logger.exception("get_last_five_batch failed")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if conn:
            conn.close()


@app.get("/api/players")
def list_players(
    q: str | None = Query(None, description="Search prefix (case-insensitive)"),
    limit: int = Query(20, ge=1, le=50),
):
    """Return distinct player names from active projections (all leagues) for typeahead. Only includes players with upcoming games."""
    conn = None
    try:
        conn = get_conn()
        cursor = conn.cursor()
        now_central = "CAST(GETUTCDATE() AT TIME ZONE 'UTC' AT TIME ZONE 'Central Standard Time' AS datetime2(0))"
        where_parts = [
            "p.player_id IS NOT NULL",
            "(p.stat_type_name NOT LIKE N'%(Combo)%' AND p.stat_type_name NOT LIKE N'%Combo%')",
            f"p.start_time >= DATEADD(day, -30, CAST({now_central} AS DATE))",
            f"p.start_time >= {now_central}",
        ]
        where_sql = " AND ".join(where_parts)
        params: list = [limit]  # TOP (?) is first in SQL
        if q is not None and q.strip():
            where_parts.append(
                "LOWER(LTRIM(RTRIM(COALESCE(pp.display_name, pp.name)))) LIKE LOWER(?) + N'%'"
            )
            where_sql = " AND ".join(where_parts)
            params.append(q.strip())
        sql = f"""
            SELECT DISTINCT TOP (?) LTRIM(RTRIM(COALESCE(pp.display_name, pp.name))) AS display_name
            FROM [dbo].[prizepicks_projection] p
            INNER JOIN [dbo].[prizepicks_player] pp ON pp.player_id = CAST(p.player_id AS NVARCHAR(20))
            WHERE {where_sql}
            ORDER BY display_name
        """
        cursor.execute(sql, params)
        rows = cursor.fetchall()
        return [{"display_name": (row[0] or "").strip()} for row in rows if row and (row[0] or "").strip()]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if conn:
            conn.close()


@app.get("/api/projections/last-updated")
def get_projections_last_updated():
    """Return the most recent last_modified_at timestamps (PrizePicks, Underdog, Parlay Play, NBA player_stat)."""
    conn = None
    try:
        conn = get_conn()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT
                (SELECT MAX(last_modified_at) FROM [dbo].[prizepicks_projection]) AS last_updated,
                (SELECT MAX(last_modified_at) FROM [dbo].[underdog_projection]) AS underdog_last_updated,
                (SELECT MAX(last_modified_at) FROM [dbo].[parlay_play_projection]) AS parlay_play_last_updated,
                (SELECT MAX(last_modified_at) FROM [dbo].[player_stat]) AS nba_stats_last_updated
        """)
        row = cursor.fetchone()
        cursor.close()

        def _serialize_dt(val):
            if val is None:
                return None
            # These timestamps are persisted in America/Chicago (datetime2) for display.
            # Keep them tz-naive so the frontend displays the stored value as-is.
            if isinstance(val, datetime):
                return val.replace(tzinfo=None).isoformat()
            if hasattr(val, "isoformat"):
                return val.isoformat()
            return str(val)

        if not row:
            return {
                "last_updated": None,
                "underdog_last_updated": None,
                "parlay_play_last_updated": None,
                "nba_stats_last_updated": None,
            }

        return {
            "last_updated": _serialize_dt(row[0]),
            "underdog_last_updated": _serialize_dt(row[1]),
            "parlay_play_last_updated": _serialize_dt(row[2]),
            "nba_stats_last_updated": _serialize_dt(row[3]),
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if conn:
            conn.close()


@app.post("/api/update/projections")
def update_projections():
    """Run prizepicks_scraper in-process (avoids debugger subprocess issues)."""
    server = os.environ.get("PROPS_DB_SERVER", "localhost\\SQLEXPRESS")
    user = os.environ.get("PROPS_DB_USER", "dbadmin")
    password = os.environ.get("PROPS_DB_PASSWORD", "")
    trusted = os.environ.get("PROPS_DB_USE_TRUSTED_CONNECTION", "").strip().lower() in ("1", "true", "yes")
    use_browser = os.environ.get("PROPS_PRIZEPICKS_USE_BROWSER", "").strip().lower() in ("1", "true", "yes")
    database = os.environ.get("PROPS_DATABASE", "Props")

    argv = [
        "prizepicks_scraper.py",
        "--all-leagues",
        "--db",
        "--db-server", server,
        "--database", database,
        "--db-user", user,
        "--db-password", password,
    ]
    if trusted:
        argv.append("--trusted-connection")
    if use_browser:
        argv.append("--browser")

    from io import StringIO
    old_argv = sys.argv
    old_stdout = sys.stdout
    sys.argv = argv
    sys.stdout = StringIO()
    try:
        import prizepicks_scraper
        exit_code = prizepicks_scraper.main()
        out = sys.stdout.getvalue()
    except Exception as e:
        out = sys.stdout.getvalue()
        tail = (out or "").strip()
        if len(tail) > 2500:
            tail = tail[-2500:]
        detail = (str(e) or "Scraper failed").strip()
        if tail:
            detail += "\n\n--- scraper log (tail) ---\n" + tail
        raise HTTPException(status_code=500, detail=detail[:3500])
    finally:
        sys.argv = old_argv
        sys.stdout = old_stdout

    if exit_code != 0:
        tail = (out or "").strip()
        if len(tail) > 3500:
            tail = tail[-3500:]
        if "Login failed for user" in out or "18456" in out:
            detail = (
                "Database login failed. Use the same credentials as NBA stats: set PROPS_DB_USER and PROPS_DB_PASSWORD, "
                "or PROPS_DB_USE_TRUSTED_CONNECTION=1 for Windows auth. Original error: " + out[:400]
            )
        elif "403" in out or "Forbidden" in out:
            detail = (
                "PrizePicks may be blocking the request. Set PROPS_PRIZEPICKS_USE_BROWSER=1 and restart the app. "
                "Original error: " + out[:400]
            )
        else:
            detail = f"Scraper exited {exit_code}.\n\n--- scraper log (tail) ---\n{tail}"
        raise HTTPException(status_code=502, detail=detail)
    return {"ok": True, "message": "Projections updated", "log": out}


class NbaStatsUpdateBody(BaseModel):
    date_from: str
    date_to: str


@app.post("/api/update/parlayplay-projections")
def update_parlayplay_projections():
    """Run parlayplay_scraper in-process (avoids debugger subprocess issues). Same behavior as CLI --db.
    Set PROPS_PARLAYPLAY_USE_BROWSER=1 to force browser; PROPS_PARLAYPLAY_USER_DATA_DIR for saved login."""
    server = os.environ.get("PROPS_DB_SERVER", "localhost\\SQLEXPRESS")
    user = os.environ.get("PROPS_DB_USER", "dbadmin")
    password = os.environ.get("PROPS_DB_PASSWORD", "")
    trusted = os.environ.get("PROPS_DB_USE_TRUSTED_CONNECTION", "").strip().lower() in ("1", "true", "yes")
    use_browser = os.environ.get("PROPS_PARLAYPLAY_USE_BROWSER", "").strip().lower() in ("1", "true", "yes")
    user_data_dir = os.environ.get("PROPS_PARLAYPLAY_USER_DATA_DIR", "").strip()
    connect_url = os.environ.get("PROPS_BROWSER_CDP", "").strip()
    argv = [
        "parlayplay_scraper.py",
        "--db",
        "--db-server", server,
        "--db-user", user,
        "--db-password", password,
    ]
    if trusted:
        argv.append("--trusted-connection")
    if use_browser:
        argv.append("--browser")
    if user_data_dir:
        argv.extend(["--user-data-dir", user_data_dir])
    if connect_url:
        argv.extend(["--connect", connect_url])
    from io import StringIO
    old_argv = sys.argv
    old_stdout = sys.stdout
    sys.argv = argv
    sys.stdout = StringIO()
    try:
        import parlayplay_scraper
        exit_code = parlayplay_scraper.main()
        out = sys.stdout.getvalue()
    except Exception as e:
        out = sys.stdout.getvalue()
        detail = str(e)[:400]
        if out:
            detail += " Log: " + (out.strip()[-400:] if len(out) > 400 else out.strip())
        raise HTTPException(status_code=500, detail=detail[:500])
    finally:
        sys.argv = old_argv
        sys.stdout = old_stdout
    if exit_code != 0:
        if "Login failed for user" in out or "18456" in out:
            detail = (
                "Database login failed. Use same credentials as other scrapers: set PROPS_DB_USER and PROPS_DB_PASSWORD, "
                "or PROPS_DB_USE_TRUSTED_CONNECTION=1 for Windows auth. Original error: " + out[:400]
            )
        else:
            detail = f"Scraper exited {exit_code}: {(out or '')[:500]}"
        raise HTTPException(status_code=502, detail=detail)
    if "Fetched 0 projection records" in out and "via API" in out:
        return {"ok": True, "message": "Parlay Play projections updated (0 lines).", "log": out}
    if "No records after browser fallback" in out:
        return {"ok": True, "message": "Parlay Play: 0 records after browser fallback (session may be expired). Run from CLI with --browser --headed to log in.", "log": out}
    return {"ok": True, "message": "Parlay Play projections updated", "log": out}


@app.post("/api/update/underdog-projections")
def update_underdog_projections():
    """Run underdog_scraper.py --db (tries httpx first, falls back to browser on 401/403). Set PROPS_UNDERDOG_USER_DATA_DIR for saved login fallback."""
    # Run in-process to avoid debugpy/pydevd subprocess interception (which returns 502).
    # We still enforce an HTTP timeout to keep the API responsive.
    server = os.environ.get("PROPS_DB_SERVER", "localhost\\SQLEXPRESS")
    user = os.environ.get("PROPS_DB_USER", "dbadmin")
    password = os.environ.get("PROPS_DB_PASSWORD", "")
    trusted = os.environ.get("PROPS_DB_USE_TRUSTED_CONNECTION", "").strip().lower() in ("1", "true", "yes")
    user_data_dir = os.environ.get("PROPS_UNDERDOG_USER_DATA_DIR", "").strip()
    connect_url = os.environ.get("PROPS_BROWSER_CDP", "").strip()

    argv = [
        "underdog_scraper.py",
        "--db",
        "--db-server", server,
        "--db-user", user,
        "--db-password", password,
    ]
    if trusted:
        argv.append("--trusted-connection")
    if user_data_dir:
        argv.extend(["--user-data-dir", user_data_dir])
    if connect_url:
        argv.extend(["--connect", connect_url])

    timeout_s = 180 if user_data_dir else 120

    from io import StringIO
    from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeoutError

    old_argv = sys.argv
    old_stdout = sys.stdout
    sys.argv = argv
    sys.stdout = StringIO()
    try:
        import underdog_scraper

        def _run():
            return underdog_scraper.main()

        with ThreadPoolExecutor(max_workers=1) as ex:
            fut = ex.submit(_run)
            try:
                exit_code = fut.result(timeout=timeout_s)
            except FutureTimeoutError:
                raise HTTPException(status_code=504, detail="Update timed out")

        out = sys.stdout.getvalue()
    except HTTPException:
        raise
    except Exception as e:
        out = sys.stdout.getvalue()
        tail = (out or "").strip()
        if len(tail) > 2500:
            tail = tail[-2500:]
        detail = (str(e) or "Underdog scraper failed").strip()
        if tail:
            detail += "\n\n--- scraper log (tail) ---\n" + tail
        raise HTTPException(status_code=500, detail=detail[:3500])
    finally:
        sys.argv = old_argv
        sys.stdout = old_stdout

    if exit_code != 0:
        tail = (out or "").strip()
        if len(tail) > 3500:
            tail = tail[-3500:]
        if "Login failed for user" in out or "18456" in out:
            detail = (
                "Database login failed. Use same credentials as NBA/PrizePicks: set PROPS_DB_USER and PROPS_DB_PASSWORD, "
                "or PROPS_DB_USE_TRUSTED_CONNECTION=1 for Windows auth. Restart the app after setting env. "
                "Original error: " + out[:600]
            )
        else:
            detail = f"Scraper exited {exit_code}.\n\n--- scraper log (tail) ---\n{tail}"
        raise HTTPException(status_code=502, detail=detail)

    if "Fetched 0 projection records via API." in out:
        return {"ok": True, "message": "Underdog projections updated (0 lines).", "log": out}
    if "No records after browser fallback" in out:
        return {"ok": True, "message": "Underdog: 0 records after browser fallback (session may be expired). Run from CLI with --browser --headed to log in.", "log": out}
    if "0 projection records" in out or "No records." in out:
        msg = "Underdog: 0 records."
        if "401" in out or "unauthorized" in out.lower():
            msg = "Underdog: 0 records (session expired or not logged in). Run from CLI: python underdog_scraper.py --browser --user-data-dir .playwright-underdog --headed — log in, press Enter. Then try Update Underdog again."
        elif "403" in out or "forbidden" in out.lower():
            msg = "Underdog: 0 records (403 forbidden). Log in again with --headed, then try Update Underdog again."
        else:
            msg = "Underdog: 0 records. Set PROPS_UNDERDOG_USER_DATA_DIR=.playwright-underdog and restart. First time: run from CLI with --headed to log in."
        return {"ok": True, "message": msg, "log": out}
    return {"ok": True, "message": "Underdog projections updated", "log": out}


@app.post("/api/update/nba-stats")
def update_nba_stats(body: NbaStatsUpdateBody = Body(...)):
    """Run nba_scraper.py --db --date-from X --date-to Y. Send date_from/date_to in JSON body."""
    date_from, date_to = body.date_from, body.date_to
    if not date_from or not date_to:
        raise HTTPException(status_code=400, detail="date_from and date_to required (YYYY-MM-DD)")
    if date_from > date_to:
        raise HTTPException(status_code=400, detail="date_from must be <= date_to")
    env = os.environ.copy()
    server = env.get("PROPS_DB_SERVER", "localhost\\SQLEXPRESS")
    user = env.get("PROPS_DB_USER", "dbadmin")
    password = env.get("PROPS_DB_PASSWORD", "")
    trusted = env.get("PROPS_DB_USE_TRUSTED_CONNECTION", "").strip().lower() in ("1", "true", "yes")
    cmd = [
        sys.executable,
        str(REPO_ROOT / "nba" / "nba_scraper.py"),
        "--db",
        "--date-from", date_from,
        "--date-to", date_to,
        "--db-server", server,
        "--db-user", user,
        "--db-password", password,
    ]
    if trusted:
        cmd.append("--trusted-connection")
    try:
        result = subprocess.run(
            cmd,
            cwd=str(REPO_ROOT),
            capture_output=True,
            text=True,
            timeout=300,
        )
        if result.returncode != 0:
            err = result.stderr or result.stdout or ""
            if "Login failed for user" in err or "18456" in err:
                detail = (
                    "Database login failed. Set PROPS_DB_USER and PROPS_DB_PASSWORD to your SQL Server credentials, "
                    "or set PROPS_DB_USE_TRUSTED_CONNECTION=1 to use Windows Authentication. "
                    "Original error: " + err[:500]
                )
            else:
                detail = f"Scraper exited {result.returncode}: {err[:500]}"
            raise HTTPException(status_code=502, detail=detail)
        return {"ok": True, "message": "NBA stats updated", "log": result.stdout}
    except subprocess.TimeoutExpired:
        raise HTTPException(status_code=504, detail="Update timed out")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/")
def index():
    """Serve the frontend."""
    index_path = STATIC_DIR / "index.html"
    if index_path.exists():
        return FileResponse(index_path)
    return {"message": "Projections API. Use /api/projections. Add app/static/index.html for UI."}
