# Plan: Database Structure and App Functionality Changes

Status: **Draft for discussion** — no code or schema has been changed in this branch.
Scope: a concrete proposal for what to change in the database (`Props` on MSSQL) and
in the `app/` FastAPI backend + static frontend, with explicit trade-offs and an ordered
delivery plan.

This plan deliberately focuses on **what should change and why**, and leaves
calendar estimates out — the work is scoped by subsystem and risk instead.

---

## 1. Current state (what we have today)

### 1.1 Repository layout

- **Scrapers** (HTTP + Playwright fallback) writing per-book stage tables:
  - `prizepicks_scraper.py` → `prizepicks_*_stage` tables
  - `underdog_scraper.py` → `underdog_*_stage` tables
  - `parlayplay_scraper.py` → `parlay_play_*_stage` tables
- **Stat feeds:**
  - `nba/nba_scraper.py` (uses `nba_api`)
  - `soccer/soccer_scraper.py` (uses `soccerdata`)
- **Reference / ETL:**
  - `sportsbook_dimension_sync.py` — merges per-book reference tables
    (`sportsbook_sport`, `sportsbook_league`, `sportsbook_team`, `sportsbook_player`,
    `sportsbook_stat_type`, `sportsbook_game`).
  - `sportsbook_projection_sync.py` — merges per-book projections into a unified
    `dbo.sportsbook_projection` table plus `sportsbook_projection_history`.
  - `projection_over_streak.py` — joins projections to NBA last-N-game stats for the UI.
  - `cross_book_stat_normalize.py` — canonical stat/league lookup tables used by both
    ETL and the API.
- **App** (`app/`):
  - `app/main.py` — FastAPI service (~1,350 LOC) exposing `/api/projections`,
    `/api/projections/streak`, `/api/projections/last-five`, `/api/players`,
    `/api/projections/last-updated`, plus `POST /api/update/*` endpoints that run
    each scraper in-process.
  - `app/db.py` — opens a pyodbc connection to `Props` using `PROPS_DB_*` env vars.
  - `app/static/index.html` — single-file vanilla JS UI (~1,100 LOC) that renders a
    grid of players, a stat-filter bar, and a modal with a last-5 chart.
- **SQL** (`schema/*.sql`) — DDL fragments, per-book `run_order.md` docs, and
  reference merge procedures.
- **Top-level scratch file** `db_update` — a *sample* normalized schema (books /
  sports / leagues / teams / players / games / stat_types / projections) that does
  **not** match the live Props DB today. It is useful as a north-star target.

### 1.2 Current DB shape (simplified)

```
prizepicks_*           parlay_play_*            underdog_*
  _stage (from scraper)   _stage (from scraper)   _stage (from scraper)
  + reference merge       + reference merge       + reference merge
  player, league,         sport, league, team,    stat_type, game, player,
  duration, game,         match, player,          solo_game, appearance,
  projection, history     stat_type, projection,  projection
                          history

            ↓ sportsbook_projection_sync.py (per-book → unified)

sportsbook_sport
sportsbook_league
sportsbook_team
sportsbook_stat_type
sportsbook_player           ← xrefs {prizepicks, underdog, parlay_play}_player_id
sportsbook_game
sportsbook_projection       ← PK (sportsbook, external_projection_id)
sportsbook_projection_history
```

Stat feeds live in separate tables (`player_stat`, `player_stat_last_n`,
`player_stat_stage`, `soccer_player_stat_stage`), joined to sportsbook dimensions
mostly by name + league + date.

### 1.3 Strengths to preserve

- Scrapers + stage tables are well isolated; we can change downstream shape without
  touching scrapers.
- `sportsbook_projection` already gives one row per (book, external_id), with a
  paired history table and line-change archiving. This is the right spine.
- `sportsbook_dimension_relations.sql` + the `UQ_*` indexes already set up a clean
  star pattern.

### 1.4 Known friction / gaps

- **Dimension FKs are soft.** `sportsbook_projection` stores `player_name`,
  `team`, `league_id`, `event_name` as denormalized text. Joins in the API and
  `vw_*` views fall back to fuzzy name matches (see `_group_player_projections`).
- **No canonical `player_id`, `game_id`, `stat_type_id`, `team_id`** on
  `sportsbook_projection`. `sportsbook_player` etc. exist, but nothing enforces
  that the unified projection row is linked to them. That blocks: reliable
  history analytics, alt-line grouping, cross-book consensus lines, player-page
  deep links, and anything that needs a stable PK outside of an external book id.
- **Alt lines are not modeled** in the unified store. `db_update` hints at
  `alt_lines` as a child table; we need the same idea for PP demon/goblin and
  Underdog higher/lower variants.
- **Settlement / results** are only represented for PrizePicks via
  `vw_prizepicks_result.sql`. There's no cross-book settled-result table, so we
  can't compute book-agnostic hit rates or track line-movement → outcome.
- **`dbo` schema only.** Staging, reference, dimension, and fact tables all share
  `dbo`. Hard to apply least-privilege or clear ownership.
- **UI monolith.** `app/static/index.html` is one file. Growing features (filters,
  saved views, odds comparisons, parlays, auth) will be painful without a build
  step.
- **No auth.** `POST /api/update/*` endpoints are unauthenticated and run
  scrapers in-process (one user = full server worker pool).
- **No migrations tool.** DDL is a collection of idempotent `IF NOT EXISTS` SQL
  scripts with an implicit per-file ordering described in `*_run_order.md`. There
  is no record of which environment is at which version.

---

## 2. Goals

Two narrow, verifiable goals for this initiative:

1. **Canonicalize the data model** so every projection (active and historical)
   links to stable `sportsbook_player_id`, `sportsbook_game_id`,
   `sportsbook_stat_type_id`, `sportsbook_league_id`, and is aware of its
   alt-line siblings. Downstream features (consensus, results, analytics) should
   read from this canonical shape, not from name-based joins.
2. **Make the app useful as a product, not just a grid** — add consensus pricing,
   alt-line drill-down, per-player pages, saved filters, and a background job
   runner that replaces the current “run scraper in the request handler” path.

---

## 3. Proposed database changes

### 3.1 Schema-level

- Introduce two SQL schemas alongside `dbo`:
  - `stage` — all `*_stage` tables (write-heavy, truncated per scrape).
  - `ref`   — per-book reference tables (`prizepicks_*`, `underdog_*`,
    `parlay_play_*`) and cross-book xrefs.
  - Keep canonical / fact tables in `dbo` (`sportsbook_*`, `player_stat*`).
- Introduce a `dbo.schema_migrations` table (version, applied_at, checksum) and a
  Python migrations runner (section 6.2). All new DDL lands as numbered
  migrations; the existing ad-hoc `schema/*.sql` scripts are wrapped as the
  initial baseline.

### 3.2 Canonical dimension tables

Additions/changes (non-breaking — new columns are NULLable, indexes added
non-blocking):

- `dbo.sportsbook_player`
  - Already has book xref columns. Add `nba_player_id` and `wnba_player_id`
    (nullable) so stats joins don't need `resolve_nba_player_ids` text matching.
  - Add index on `(display_name, canonical_league_id)` (already partially there).
- `dbo.sportsbook_stat_type`
  - Add `canonical_stat_key` (nvarchar(80), e.g. `points`, `rebounds`,
    `threes_made`, `3pt_attempts`) — populated from
    `cross_book_stat_normalize.normalize_for_join`. Unique per
    (`sportsbook_sport_id`, `canonical_stat_key`).
- `dbo.sportsbook_game`
  - Add `home_sportsbook_team_id`, `away_sportsbook_team_id` (already present),
    plus `game_date_central` AS `CAST(... AT TIME ZONE 'Central Standard Time' AS date) PERSISTED`
    to replace the repeated `CONVERT` expressions in the API and
    `get_parlay_play_lines_by_match`.

### 3.3 Canonical fact tables

- **New columns on `dbo.sportsbook_projection` (and `_history`)**:
  - `sportsbook_player_id bigint NULL` (FK `sportsbook_player`)
  - `sportsbook_game_id bigint NULL` (FK `sportsbook_game`)
  - `sportsbook_stat_type_id bigint NULL` (FK `sportsbook_stat_type`)
  - `sportsbook_league_id bigint NULL` (FK `sportsbook_league`)
  - `is_alt_line bit NOT NULL DEFAULT 0`
  - `parent_projection_key nvarchar(120) NULL` — group key
    `(sportsbook, source_player_id, source_game_id, canonical_stat_key)`.
  - `over_american_price int NULL`, `under_american_price int NULL`
  - `over_decimal_price decimal(6,2) NULL`, `under_decimal_price decimal(6,2) NULL`
  - `payout_multiplier decimal(6,2) NULL` (PP demon/goblin / Underdog higher-lower)
- **Backfill** in `sportsbook_projection_sync.py`:
  - When we merge from each book's stage/reference table, resolve the four
    `*_id` columns via the existing xref tables (keep name-based fallback, just
    store the resolved id alongside).
  - Populate `canonical_stat_key` by calling `normalize_for_join` server-side
    via a UDF (`dbo.fn_normalize_stat_basic`) created from the Python lookup.
- **Alt-line table** `dbo.sportsbook_projection_alt`
  - One row per child alt (demon, goblin, “+3 pts”, etc.) with FK to
    `sportsbook_projection` via the canonical parent row. Mirrors the
    `alt_lines` shape in `db_update`.

### 3.4 New analytics tables

- `dbo.sportsbook_projection_result`
  - `sportsbook_projection_id bigint PK` (matches the row in the main table)
  - `actual_value decimal(10,2) NULL`
  - `result nvarchar(10) NULL` ('over' / 'under' / 'push' / 'void')
  - `settled_at datetime2(7) NULL`
  - `source nvarchar(30) NOT NULL` ('nba_api', 'soccerdata', 'manual', 'book')
  - Populated by a new job `sportsbook_projection_settle.py` that joins the
    historical projection row to `player_stat` by `(nba_player_id, game_date)`.
- `dbo.sportsbook_line_movement`
  - Already half-exists as `sportsbook_projection_history` with
    `archive_reason='line_changed'`. Add a view `vw_sportsbook_line_movement`
    that sequences open → close per projection for charting.
- `dbo.sportsbook_consensus_line` (view or materialized table)
  - Key: `(canonical_league_id, sportsbook_player_id,
    sportsbook_stat_type_id, game_date_central)`.
  - Columns: min line, max line, median line, book count, last update.
  - Drives the proposed consensus column in the UI (section 4.2).

### 3.5 Retention and indexing

- Add a partitioning / retention policy for `sportsbook_projection_history` —
  rows older than 180 days get moved to `sportsbook_projection_history_archive`
  and out of hot indexes. For now, this can be a scheduled stored proc.
- Revisit `IX_sportsbook_projection_player_time` after the new `*_id` columns
  land — replace the player_name / stat_type_name fields with id-based covering
  indexes (keep the old ones during the transition).

### 3.6 Migration approach (no breakage)

1. Add new columns as NULLable.
2. Backfill via the existing sync scripts (they already re-read stage on each run).
3. Build the new views / analytics tables reading from canonical ids, falling
   back to names when `*_id IS NULL`.
4. Once backfill is ≥99% complete, add the FKs WITH NOCHECK, then later CHECK
   (so existing unresolvable rows don't block deployment).
5. Drop dependency on name-based joins in `app/main.py` (see section 4.5).

---

## 4. Proposed app functionality changes

### 4.1 API re-shape (additive, versioned)

Add a `/api/v2` namespace. v1 endpoints keep working for the current UI.

- `GET /api/v2/projections` — same filters as today, but returns canonical ids
  alongside the existing display strings:
  ```json
  {
    "player": {"id": 123, "display_name": "...", "team_abbrev": "..."},
    "game":   {"id": 456, "start_time": "...", "home": "...", "away": "..."},
    "stat":   {"id": 11, "canonical_key": "points", "display_name": "Points"},
    "sportsbooks": {
      "prizepicks": {"line": 24.5, "odds_type": "standard", "projection_id": "..."},
      "underdog":   {"line": 24.5, "multiplier": 3.0, "projection_id": "..."},
      "parlay_play":{"line": 24.5, "odds_type": "main", "projection_id": "..."}
    },
    "alt_lines": [ ... ],
    "consensus": {"median": 24.5, "min": 24.0, "max": 25.0, "books": 3}
  }
  ```
- `GET /api/v2/players/{sportsbook_player_id}` — player page: bio,
  season line, all active props, last-N rolling chart (replaces the current
  modal-only view).
- `GET /api/v2/games/{sportsbook_game_id}` — all props in a game, grouped by
  stat_type, sorted by consensus line.
- `GET /api/v2/projections/{sportsbook_projection_id}/line-movement` — open
  line, current line, all change points; feeds the line-movement chart.
- `GET /api/v2/results` — settled projection results for the last N days (filter
  by book/league/player/stat).

### 4.2 UI changes

- Replace the single `app/static/index.html` with a small build (Vite + vanilla
  TypeScript or Svelte, kept dependency-light). Keep the current dark theme and
  grid density as the default view.
- New grid columns:
  - **Consensus line** (median across active books)
  - **Best over / best under** (book + price)
  - **Line movement** indicator (▲/▼ vs last archived line)
- New pages (same app, separate routes via hash or a tiny router):
  - `/player/:id` — player summary, all props, last-N chart, line movement.
  - `/game/:id` — game slate view.
  - `/movers` — top line movers in the last X hours.
- Keep `index.html` as a shim that loads the built bundle, so existing deep
  links keep working.

### 4.3 Parlays / saved views (new feature set)

Two small but high-value additions:

- `dbo.saved_parlay` + `dbo.saved_parlay_leg` — a user can pin a combination of
  projections and revisit it later. No accounts yet; a client-generated
  `parlay_code` + local storage on the client side, with optional server
  persistence keyed by a user-supplied label.
- `dbo.saved_filter` — a named set of `(league, stat_type, book)` filters for
  the grid.

### 4.4 Background jobs

Move the `POST /api/update/*` handlers off the request thread.

- Introduce a tiny job queue in SQL (`dbo.job_queue`: id, job_type, args_json,
  status, started_at, finished_at, log_tail). The FastAPI handler enqueues and
  returns immediately. A separate `app/worker.py` process polls and runs the
  scraper `main()`s, captures stdout, and writes back status.
- The UI polls `GET /api/v2/jobs/{id}` for progress instead of waiting on the
  POST.
- This also fixes the current 502-on-debugger problem (see
  `update_underdog_projections` inline comments) and lets us add per-book
  rate limits.

### 4.5 Consolidation inside `app/main.py`

- Split `app/main.py` into:
  - `app/api/projections.py`
  - `app/api/players.py`
  - `app/api/games.py`
  - `app/api/jobs.py`
  - `app/services/projection_query.py` — the unified-query SQL currently inlined
    in `_fetch_unified_projection_rows` and `_group_player_projections` moves
    here.
- Replace name-based Parlay Play join logic (`_lookup_parlay_line`,
  `get_parlay_play_lines_by_match`) with id-based joins once
  `sportsbook_projection.sportsbook_player_id` / `sportsbook_stat_type_id` are
  backfilled; keep the name logic as a fallback for ≥6 months.

### 4.6 AuthN / AuthZ (minimum viable)

Only needed for the update endpoints and saved data.

- Add a single shared-secret header `X-Update-Token` read from env
  `PROPS_UPDATE_TOKEN`. Required for all `POST /api/update/*` and all
  `POST/PUT/DELETE /api/v2/*`. Fail closed when unset.
- Rate-limit per-IP for read endpoints via a lightweight middleware
  (`slowapi` or in-process token bucket).

---

## 5. Risks and trade-offs

- **Adding columns to `sportsbook_projection` requires re-running the sync
  scripts.** Mitigated by: additive columns, NULL-safe lookups, and backfill-on-
  every-run in `sportsbook_projection_sync.py`.
- **Name-based joins will not be 100% resolvable.** Some PrizePicks /
  Underdog rows never match a canonical player (missing xref). We must keep
  showing those rows in the UI — canonical ids are enrichment, not a gate.
- **Build pipeline for UI.** Introducing Vite/TS adds a `npm install` to the
  dev loop. Can be isolated under `app/ui/` so Python-only dev still works.
- **MSSQL vs cross-platform.** Several proposals (`AT TIME ZONE`,
  `SYSUTCDATETIME`, `JSON_QUERY`, filtered unique indexes) are MSSQL-specific.
  That's consistent with the current codebase and the `Props` deployment.
- **Migrations tool choice.** Alembic is SQLAlchemy-centric and heavy for our
  raw-SQL workflow. A custom 80-line runner that executes numbered
  `schema/migrations/NNNN_*.sql` files and records versions in
  `dbo.schema_migrations` is simpler and matches the existing style.

---

## 6. Delivery plan (ordered, each step independently shippable)

Listed by dependency order. No time estimates — each step is a separate PR and
each is independently revertible.

### 6.1 Phase 1 — foundations (no user-visible changes)

1. Add `dbo.schema_migrations` + a Python runner (`scripts/migrate.py`).
   Wrap all existing `schema/*.sql` idempotent scripts as migration
   `0000_baseline.sql`.
2. Move staging-only tables into a new `stage` schema via a migration that
   renames or creates synonyms for backward compatibility.
3. Create `dbo.fn_normalize_stat_basic` (MSSQL UDF) and migrate
   `sportsbook_stat_type.canonical_stat_key`.

### 6.2 Phase 2 — canonical ids on facts

4. Add nullable id columns (`sportsbook_player_id`, `sportsbook_game_id`,
   `sportsbook_stat_type_id`, `sportsbook_league_id`) and pricing columns to
   `sportsbook_projection` + `sportsbook_projection_history`.
5. Extend `sportsbook_projection_sync.py` to resolve + write those ids.
6. Add indexes: `(sportsbook_player_id, start_time)`,
   `(sportsbook_game_id, sportsbook_stat_type_id)`.
7. Add `sportsbook_projection_alt` and have each book's sync populate it where
   alt lines already exist in the scraper output.

### 6.3 Phase 3 — analytics surface

8. Ship `vw_sportsbook_consensus_line` + a nightly materialized refresh.
9. Ship `sportsbook_projection_result` + `scripts/settle_projections.py` that
   joins to `player_stat` by `nba_player_id`.
10. Ship `vw_sportsbook_line_movement`.

### 6.4 Phase 4 — API v2

11. Add `/api/v2/projections`, `/api/v2/players/{id}`, `/api/v2/games/{id}`,
    `/api/v2/projections/{id}/line-movement`, `/api/v2/results`.
12. Add job queue tables, `app/worker.py`, and `/api/v2/jobs`. Switch
    `POST /api/update/*` to enqueue.
13. Add `X-Update-Token` auth on all write endpoints.

### 6.5 Phase 5 — UI

14. Scaffold `app/ui/` (Vite + TS or Svelte). Port the existing grid and modal
    to the new build, reading from `/api/v2` where helpful and falling back to
    `/api/v1` otherwise.
15. Add `/player/:id`, `/game/:id`, `/movers` routes.
16. Add saved filters + saved parlays (localStorage first, then server tables).

### 6.6 Phase 6 — cleanup

17. Add NOCHECK FKs for the new id columns; then CHECK once resolution rate is
    high enough.
18. Drop the name-based fallbacks in `app/main.py` (or gate behind a
    `PROPS_API_NAME_FALLBACK=1` env flag).
19. Retire the legacy `POST /api/update/*` sync path.

---

## 7. Open questions for the maintainer

1. Are there books beyond PrizePicks / Underdog / ParlayPlay we should plan
   schema capacity for (DraftKings Pick6, Dabble, Thrive, BetMGM, etc.)?
2. Is the `db_update` file the intended target shape, or is it an experiment?
   The plan above is compatible with it but not a direct port.
3. Do we need user accounts for saved parlays, or is per-device localStorage
   enough for now?
4. Is there an appetite for Postgres as an alt backend, or is MSSQL-only the
   long-term plan? (This affects whether we can rely on `AT TIME ZONE`, filtered
   indexes, `STRING_AGG`-style JSON, etc.)
5. Which environments need zero-downtime migrations? If production is 24/7, we
   should prefer additive-only steps in Phase 2 and rebuild indexes online.

Leave answers as PR review comments on this file and we'll incorporate them
into the next revision before any code changes land.
