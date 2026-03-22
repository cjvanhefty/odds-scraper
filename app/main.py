"""
FastAPI app for projections UI.
Run from repo root: uvicorn app.main:app --reload
"""

import logging
import os
import subprocess
import sys
from pathlib import Path

from fastapi import FastAPI, Query, HTTPException, Body
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
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


def _normalize_join_stat(stat_type_name: str | None) -> str:
    """Normalize stat names for cross-provider joins (UI keys), without changing what we display."""
    s = (stat_type_name or "").strip()
    if s in ("Blocks", "Blocked Shots"):
        return "Blocks__Blocked_Shots"
    if s in ("Double Doubles", "Double-Doubles"):
        return "Double_Doubles"
    if s.startswith("Blks+Stls"):
        return "Blks_Stls"
    if s in ("3 Pointers", "3 Pointers Made"):
        return "FG3M"
    return s


def get_parlay_play_lines_by_match(conn) -> dict:
    """Return (display_name, stat_type_name, game_date) -> line_score for matching to PrizePicks."""
    cursor = conn.cursor()
    try:
        cursor.execute("""
            SELECT display_name, stat_type_name,
                   CONVERT(varchar(10), CAST(start_time AT TIME ZONE 'Central Standard Time' AS datetime2(0)), 120) AS game_date,
                   line_score
            FROM [dbo].[parlay_play_projection]
            WHERE display_name IS NOT NULL AND stat_type_name IS NOT NULL
              AND is_main_line = 1
        """)
        key_to_line = {}
        for row in cursor.fetchall():
            name = (row[0] or "").strip()
            stat = _normalize_join_stat((row[1] or "").strip())
            date_part = (row[2] or "")[:10]
            line = float(row[3]) if row[3] is not None else None
            if name and stat and date_part:
                key_to_line[(name, stat, date_part)] = line
        return key_to_line
    except Exception:
        return {}
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
        parlay_play_map = get_parlay_play_lines_by_match(conn)
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
            stat_type_name = _normalize_join_stat((r.get("stat_type_name") or "").strip())
            start_time = r.get("start_time")
            game_date = (str(start_time)[:10] if start_time else "") or ""
            key = (display_name, stat_type_name, game_date)
            row["line_parlay_play"] = parlay_play_map.get(key)
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
    """Return the most recent last_modified_at for PrizePicks projections (when they were last updated)."""
    conn = None
    try:
        conn = get_conn()
        cursor = conn.cursor()
        cursor.execute(
            "SELECT MAX(last_modified_at) AS last_updated FROM [dbo].[prizepicks_projection]"
        )
        row = cursor.fetchone()
        cursor.close()
        if row and row[0] is not None:
            # Return ISO format for the frontend
            val = row[0]
            if hasattr(val, "isoformat"):
                return {"last_updated": val.isoformat()}
            return {"last_updated": str(val)}
        return {"last_updated": None}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if conn:
            conn.close()


@app.post("/api/update/projections")
def update_projections():
    """Run prizepicks_scraper.py --all-leagues --db to fetch all sports. Uses same DB auth as NBA stats."""
    env = os.environ.copy()
    server = env.get("PROPS_DB_SERVER", "localhost\\SQLEXPRESS")
    user = env.get("PROPS_DB_USER", "dbadmin")
    password = env.get("PROPS_DB_PASSWORD", "")
    trusted = env.get("PROPS_DB_USE_TRUSTED_CONNECTION", "").strip().lower() in ("1", "true", "yes")
    use_browser = env.get("PROPS_PRIZEPICKS_USE_BROWSER", "").strip().lower() in ("1", "true", "yes")
    cmd = [
        sys.executable,
        str(REPO_ROOT / "prizepicks_scraper.py"),
        "--all-leagues",
        "--db",
        "--db-server", server,
        "--db-user", user,
        "--db-password", password,
    ]
    if trusted:
        cmd.append("--trusted-connection")
    if use_browser:
        cmd.append("--browser")
    try:
        result = subprocess.run(
            cmd,
            cwd=str(REPO_ROOT),
            capture_output=True,
            text=True,
            timeout=300 if use_browser else 180,
            env=env,
        )
        if result.returncode != 0:
            err = result.stderr or result.stdout or ""
            if "Login failed for user" in err or "18456" in err:
                detail = (
                    "Database login failed. Use the same credentials as NBA stats: set PROPS_DB_USER and PROPS_DB_PASSWORD, "
                    "or PROPS_DB_USE_TRUSTED_CONNECTION=1 for Windows auth. Restart the app after setting env. "
                    "Original error: " + err[:400]
                )
            elif "403" in err or "Forbidden" in err:
                detail = (
                    "PrizePicks may be blocking the request. Set PROPS_PRIZEPICKS_USE_BROWSER=1 and restart the app. "
                    "Original error: " + err[:400]
                )
            else:
                detail = f"Scraper exited {result.returncode}: {err[:500]}"
            raise HTTPException(status_code=502, detail=detail)
        return {"ok": True, "message": "Projections updated", "log": result.stdout}
    except subprocess.TimeoutExpired:
        raise HTTPException(status_code=504, detail="Update timed out")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


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
    env = os.environ.copy()
    server = env.get("PROPS_DB_SERVER", "localhost\\SQLEXPRESS")
    user = env.get("PROPS_DB_USER", "dbadmin")
    password = env.get("PROPS_DB_PASSWORD", "")
    trusted = env.get("PROPS_DB_USE_TRUSTED_CONNECTION", "").strip().lower() in ("1", "true", "yes")
    user_data_dir = env.get("PROPS_UNDERDOG_USER_DATA_DIR", "").strip()
    cmd = [
        sys.executable,
        str(REPO_ROOT / "underdog_scraper.py"),
        "--db",
        "--db-server", server,
        "--db-user", user,
        "--db-password", password,
    ]
    if trusted:
        cmd.append("--trusted-connection")
    if user_data_dir:
        cmd.extend(["--user-data-dir", user_data_dir])
    try:
        result = subprocess.run(
            cmd,
            cwd=str(REPO_ROOT),
            capture_output=True,
            text=True,
            timeout=180 if user_data_dir else 120,
            env=env,
        )
        if result.returncode != 0:
            err = result.stderr or result.stdout or ""
            if "Login failed for user" in err or "18456" in err:
                detail = (
                    "Database login failed. Use same credentials as NBA/PrizePicks: set PROPS_DB_USER and PROPS_DB_PASSWORD, "
                    "or PROPS_DB_USE_TRUSTED_CONNECTION=1 for Windows auth. Restart the app after setting env. "
                    "Original error: " + err[:400]
                )
            else:
                detail = f"Scraper exited {result.returncode}: {err[:500]}"
            raise HTTPException(status_code=502, detail=detail)
        out = result.stdout or ""
        if "Fetched 0 projection records via API." in out:
            return {"ok": True, "message": "Underdog projections updated (0 lines).", "log": result.stdout}
        if "No records after browser fallback" in out:
            return {"ok": True, "message": "Underdog: 0 records after browser fallback (session may be expired). Run from CLI with --browser --headed to log in.", "log": result.stdout}
        if "0 projection records" in out or "No records." in out:
            msg = "Underdog: 0 records."
            if "401" in out or "unauthorized" in out.lower():
                msg = "Underdog: 0 records (session expired or not logged in). Run from CLI: python underdog_scraper.py --browser --user-data-dir .playwright-underdog --headed — log in, press Enter. Then try Update Underdog again."
            elif "403" in out or "forbidden" in out.lower():
                msg = "Underdog: 0 records (403 forbidden). Log in again with --headed, then try Update Underdog again."
            else:
                msg = "Underdog: 0 records. Set PROPS_UNDERDOG_USER_DATA_DIR=.playwright-underdog and restart. First time: run from CLI with --headed to log in."
            return {"ok": True, "message": msg, "log": result.stdout}
        return {"ok": True, "message": "Underdog projections updated", "log": result.stdout}
    except subprocess.TimeoutExpired:
        raise HTTPException(status_code=504, detail="Update timed out")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


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
