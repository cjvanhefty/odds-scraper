# AGENTS.md

## Cursor Cloud specific instructions

### Overview

This is a Python 3.10+ sports props scraping app. It scrapes NBA player prop betting lines (currently PrizePicks only) and stores them in a local SQLite database. See `README.md` for full setup and run instructions.

### Architecture

- **`backend/`** — SQLite DB layer (`db.py` for connections, `schema.sql` for schema + seed data). Database file lives at `backend/data/props.db`.
- **`scraper/`** — Scraping adapters and persistence. `storage.py` handles DB writes; `adapters/prizepicks_nba.py` is the PrizePicks scraper using Playwright.

### Development environment

- Python virtualenv at `.venv`. Activate with `source .venv/bin/activate`.
- Single dependency: `playwright` (see `requirements.txt`). Playwright requires a Chromium browser binary installed via `playwright install chromium`.
- Database must be initialized once: `cd backend && python db.py`.

### Known issues

- `scraper/adapters/prizepicks_nba.py` line 67 has a pre-existing syntax error: `def _captured_api_data: list[dict] = []` should be `_captured_api_data: list[dict] = []`. This causes a `SyntaxError` when running the scraper module directly.

### Running

- Scraper: `python -m scraper.adapters.prizepicks_nba` (requires the syntax error fix above).
- DB init: `cd backend && python db.py`.

### Testing

- No test framework or lint tooling is configured in this repository. No `pyproject.toml`, `setup.cfg`, `tox.ini`, or test directories exist.
- To validate the environment, import and exercise the storage layer: `from scraper.storage import upsert_event, insert_prop_lines`.
