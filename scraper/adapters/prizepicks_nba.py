"""
PrizePicks NBA props scraper.
Uses Playwright; optionally captures API responses for stable parsing.
Run: python -m scraper.adapters.prizepicks_nba
"""
import json
import re
import time
from datetime import datetime
from pathlib import Path

# Project root for imports
import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from playwright.sync_api import sync_playwright

from scraper.storage import PRIZEPICKS_BOOK_ID, insert_prop_lines, upsert_event

NBA_URL = "https://www.prizepicks.com/category/nba"


def _normalize_stat_type(label: str) -> str:
    """Map display label to stat_type (e.g. 'Points' -> 'points')."""
    return label.strip().lower().replace(" ", "_") if label else ""


def _parse_api_payload(data: dict) -> list[dict]:
    """
    Extract prop line dicts from a JSON API response.
    Returns list of {player_name, stat_type, line_value, multiplier?}.
    """
    out = []

    def find_projection_like(obj):
        if not isinstance(obj, dict):
            return
        for k, v in obj.items():
            if k in ("projections", "props", "lines", "picks") and isinstance(v, list):
                for item in v:
                    if isinstance(item, dict):
                        ath = item.get("athlete") or {}
                        name = (item.get("name") or item.get("player_name") or ath.get("name") or "").strip()
                        stat = (item.get("stat_type") or item.get("stat") or item.get("statType") or item.get("stat_label") or "").strip()
                        line = item.get("line") or item.get("line_value") or item.get("lineValue")
                        if name and line is not None:
                            try:
                                line_val = float(line)
                            except (TypeError, ValueError):
                                continue
                            out.append({
                                "player_name": name,
                                "stat_type": _normalize_stat_type(stat) or "points",
                                "line_value": line_val,
                                "multiplier": item.get("multiplier"),
                            })
            elif isinstance(v, dict):
                find_projection_like(v)
            elif isinstance(v, list) and v and isinstance(v[0], dict):
                for i in v:
                    find_projection_like(i)

    find_projection_like(data)
    return out


def _captured_api_data: list[dict] = []


def _on_response(response):
    """Capture JSON responses that might contain props data."""
    try:
        if "application/json" in (response.headers.get("content-type") or ""):
            body = response.text()
            if not body or len(body) > 2_000_000:
                return
            data = json.loads(body)
            if isinstance(data, dict) and any(
                k in str(data).lower() for k in ("projection", "prop", "line", "player", "athlete")
            ):
                _captured_api_data.append(data)
    except Exception:
        pass


def scrape_prizepicks_nba(headless: bool = True) -> dict:
    """
    Scrape NBA props from PrizePicks and write to DB.
    Returns {"events": N, "lines": M, "error": optional str}.
    """
    global _captured_api_data
    _captured_api_data = []
    total_events = 0
    total_lines = 0
    error = None

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless)
        try:
            context = browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            )
            page = context.new_page()
            page.on("response", _on_response)

            page.goto(NBA_URL, wait_until="networkidle", timeout=30_000)
            time.sleep(3)  # Allow JS to load more data

            # Parse all captured API responses into flat list of line dicts
            all_lines = []
            for data in _captured_api_data:
                all_lines.extend(_parse_api_payload(data))

            # If no API data, try DOM (inspect page and adjust selectors if needed)
            if not all_lines:
                cards = page.query_selector_all("[data-testid='projection-card'], .projection-card, [class*='ProjectionCard']")
                if not cards:
                    cards = page.query_selector_all("article, [class*='card'], [class*='prop']")
                for card in cards[:200]:
                    try:
                        name_el = card.query_selector("[class*='name'], [class*='player'], .player-name, h3, h4")
                        line_el = card.query_selector("[class*='line'], [class*='stat'], [class*='value']")
                        stat_el = card.query_selector("[class*='stat'], [class*='type'], [class*='label']")
                        name = name_el.inner_text().strip() if name_el else ""
                        line_text = line_el.inner_text().strip() if line_el else ""
                        stat_text = stat_el.inner_text().strip() if stat_el else "points"
                        if not name or not line_text:
                            continue
                        num = re.search(r"[\d.]+", line_text)
                        line_value = float(num.group()) if num else None
                        if line_value is not None:
                            all_lines.append({
                                "player_name": name,
                                "stat_type": _normalize_stat_type(stat_text) or "points",
                                "line_value": line_value,
                            })
                    except Exception:
                        continue

            # Persist: one event for this scrape, all lines under it
            if all_lines:
                event_name = "NBA"
                game_date = datetime.utcnow().strftime("%Y-%m-%d")
                event_id = upsert_event(None, event_name, game_date, None)
                total_events = 1
                total_lines = insert_prop_lines(PRIZEPICKS_BOOK_ID, event_id, all_lines)

        except Exception as e:
            error = str(e)
        finally:
            browser.close()

    return {"events": total_events, "lines": total_lines, "error": error}


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--headed", action="store_true", help="Run browser visible")
    args = ap.parse_args()
    result = scrape_prizepicks_nba(headless=not args.headed)
    print("Result:", result)
    if result.get("error"):
        print("Error:", result["error"])
