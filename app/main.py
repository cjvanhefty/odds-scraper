"""
FastAPI app for projections UI.
Run from repo root: uvicorn app.main:app --reload
"""

import os
import subprocess
import sys
from pathlib import Path

from fastapi import FastAPI, Query, HTTPException, Body
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

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


def get_underdog_lines_by_match(conn) -> dict:
    """Return (display_name, stat_type_name, game_date) -> line_score for matching to PrizePicks."""
    cursor = conn.cursor()
    try:
        cursor.execute("""
            SELECT display_name, stat_type_name,
                   CONVERT(varchar(10), start_time, 120) AS game_date,
                   line_score
            FROM [dbo].[underdog_projection]
            WHERE display_name IS NOT NULL AND stat_type_name IS NOT NULL
        """)
        key_to_line = {}
        for row in cursor.fetchall():
            name = (row[0] or "").strip()
            stat = (row[1] or "").strip()
            date_part = (row[2] or "")[:10]
            line = float(row[3]) if row[3] is not None else None
            if name and stat and date_part:
                key_to_line[(name, stat, date_part)] = line
        return key_to_line
    except Exception:
        return {}
    finally:
        cursor.close()


def get_parlay_play_lines_by_match(conn) -> dict:
    """Return (display_name, stat_type_name, game_date) -> line_score for matching to PrizePicks."""
    cursor = conn.cursor()
    try:
        cursor.execute("""
            SELECT display_name, stat_type_name,
                   CONVERT(varchar(10), start_time, 120) AS game_date,
                   line_score
            FROM [dbo].[parlay_play_projection]
            WHERE display_name IS NOT NULL AND stat_type_name IS NOT NULL
        """)
        key_to_line = {}
        for row in cursor.fetchall():
            name = (row[0] or "").strip()
            stat = (row[1] or "").strip()
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


@app.get("/api/projections")
def list_projections(
    stat_type: str | None = Query(None, description="Filter by stat type (e.g. Points, Rebounds)"),
    streak_games: int = Query(5, ge=1, le=20, description="Number of games for over-streak / risk"),
    league_id: int = Query(7, description="PrizePicks league_id (7 = NBA). Ignored if league_ids is set."),
    league_ids: str | None = Query(None, description="Comma-separated PrizePicks league_ids (e.g. 7,9,82 for NBA, NFL, Soccer)"),
    include_all_odds: bool = Query(False, description="Include all odds types (standard, demon, goblin) for modals"),
    player_name: str | None = Query(None, description="Filter to one player (e.g. for modal)"),
    page: int = Query(1, ge=1, description="Page number (used when page_size > 0)"),
    page_size: int = Query(0, ge=0, le=500, description="Rows per page; 0 = return all (no paging)"),
):
    """Return PrizePicks projections with favored/risk from last N games. When page_size > 0, returns { items, total, page, page_size }."""
    conn = None
    try:
        if league_ids and league_ids.strip():
            try:
                league_id_list = [int(x.strip()) for x in league_ids.split(",") if x.strip()]
            except ValueError:
                league_id_list = [7]
        else:
            league_id_list = [league_id]
        conn = get_conn()
        projections = get_projections(
            conn,
            league_id=league_id_list,
            odds_type=None if include_all_odds else "standard",
        )
        if stat_type and stat_type.strip():
            projections = [p for p in projections if (p.get("stat_type_name") or "").strip() == stat_type.strip()]
        if player_name and player_name.strip():
            pn = player_name.strip()
            projections = [p for p in projections if (p.get("display_name") or p.get("pp_name") or "").strip() == pn]
        name_to_nba_id = resolve_nba_player_ids(conn, use_api_fallback=True)
        enriched = enrich_projections_with_streak(conn, projections, name_to_nba_id, streak_games)
        underdog_map = get_underdog_lines_by_match(conn)
        parlay_play_map = get_parlay_play_lines_by_match(conn)
        out = []
        for r in enriched:
            row = {}
            for k, v in r.items():
                if hasattr(v, "__float__") and not isinstance(v, (bool, int)):
                    row[k] = float(v) if v is not None else None
                else:
                    row[k] = v
            display_name = (r.get("display_name") or r.get("pp_name") or "").strip()
            stat_type_name = (r.get("stat_type_name") or "").strip()
            start_time = r.get("start_time")
            game_date = (str(start_time)[:10] if start_time else "") or ""
            key = (display_name, stat_type_name, game_date)
            row["line_underdog"] = underdog_map.get(key)
            row["line_parlay_play"] = parlay_play_map.get(key)
            out.append(row)
        # One row per player (earliest game): when not loading modal, dedupe by display_name so each player appears once
        if not (player_name and player_name.strip()):
            out.sort(key=lambda r: (r.get("start_time") or "", (r.get("display_name") or r.get("pp_name") or "")))
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
            return {"items": items, "total": total, "page": page, "page_size": page_size}
        return out
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
    """Run parlayplay_scraper.py --browser --db or with --input."""
    env = os.environ.copy()
    server = env.get("PROPS_DB_SERVER", "localhost\\SQLEXPRESS")
    user = env.get("PROPS_DB_USER", "dbadmin")
    password = env.get("PROPS_DB_PASSWORD", "")
    cmd = [
        sys.executable,
        str(REPO_ROOT / "parlayplay_scraper.py"),
        "--browser",
        "--db",
        "--db-server", server,
        "--db-user", user,
        "--db-password", password,
    ]
    try:
        result = subprocess.run(
            cmd,
            cwd=str(REPO_ROOT),
            capture_output=True,
            text=True,
            timeout=120,
        )
        if result.returncode != 0:
            raise HTTPException(
                status_code=502,
                detail=f"Scraper exited {result.returncode}: {result.stderr or result.stdout}",
            )
        return {"ok": True, "message": "Parlay Play projections updated", "log": result.stdout}
    except subprocess.TimeoutExpired:
        raise HTTPException(status_code=504, detail="Update timed out")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/update/underdog-projections")
def update_underdog_projections():
    """Run underdog_scraper.py --browser --db. Uses same DB auth as NBA/PrizePicks. Set PROPS_UNDERDOG_USER_DATA_DIR to use saved login."""
    env = os.environ.copy()
    server = env.get("PROPS_DB_SERVER", "localhost\\SQLEXPRESS")
    user = env.get("PROPS_DB_USER", "dbadmin")
    password = env.get("PROPS_DB_PASSWORD", "")
    trusted = env.get("PROPS_DB_USE_TRUSTED_CONNECTION", "").strip().lower() in ("1", "true", "yes")
    user_data_dir = env.get("PROPS_UNDERDOG_USER_DATA_DIR", "").strip()
    cmd = [
        sys.executable,
        str(REPO_ROOT / "underdog_scraper.py"),
        "--browser",
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
        if "Captured 0 projection records" in out:
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
