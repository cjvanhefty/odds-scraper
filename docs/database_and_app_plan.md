# Plan: Database Structure and App Functionality Changes

Status: **Accepted — hybrid rollout (Option A via the staged path in section 3.0.3a).**
No code or schema has been changed in this branch yet. Scope: a concrete
proposal for what to change in the database (`Props` on MSSQL) and in the
`app/` FastAPI backend + static frontend, with explicit trade-offs and an
ordered delivery plan.

This plan deliberately focuses on **what should change and why**, and leaves
calendar estimates out — the work is scoped by subsystem and risk instead.

**Decision log**

- *Section 3.0 (duplicate `sportsbook_*` rows):* **Hybrid rollout accepted.**
  Xref tables become authoritative for book-specific ids across every
  dimension. Per-book id columns on the canonical tables stay for one release
  as an auto-populated mirror to avoid breaking existing queries, then are
  dropped in the following release. See section 3.0.3a for the pros/cons this
  was picked against, and section 6.1 for the PR-sized Phase 1 breakdown that
  implements it.

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

- **Duplicate canonical rows across every `sportsbook_*` dimension** — the
  biggest data-quality issue. `sportsbook_player`, `sportsbook_team`,
  `sportsbook_stat_type`, `sportsbook_game`, `sportsbook_league`, and
  `sportsbook_sport` all use one column per book (`prizepicks_*_id`,
  `underdog_*_id`, `parlay_play_*_id`) with filtered unique indexes. Because
  each book emits multiple external ids for the same real-world thing (e.g.
  PrizePicks has a separate `player_id` per full-game / 1st-half / 1st-quarter
  variant of the same player; Underdog has `solo_game` + `appearance`
  variants), the sync code is forced to create one canonical row per external
  id. Result: five "LeBron James" rows, each with two of three id columns
  NULL. The existing `_consolidate_sportsbook_player_dupes` dedup step keys on
  exact-match `(lower(display_name), team_abbrev, jersey_number)` and silently
  fails on any normalization drift. See section 3.0 for the fix.
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

### 3.0 Eliminate duplicate rows in `sportsbook_*` dimension tables (top priority)

**Problem (observed):** the `sportsbook_player` table contains multiple rows for
the same real-world player — e.g., five "LeBron James" rows: one per PrizePicks
variant (full game / 1H / 1Q / …), one for Underdog, one for ParlayPlay. Each
row has at most one of `prizepicks_player_id`, `underdog_player_id`,
`parlay_play_player_id` populated and the other two are NULL. The same bug
exists for `sportsbook_team`, `sportsbook_stat_type`, `sportsbook_game`,
`sportsbook_league`, and `sportsbook_sport`.

**Root cause** (confirmed from `sportsbook_dimension_sync.py` +
`schema/sportsbook_player.sql` + `schema/sportsbook_player_xref.sql`):

1. **Schema models each book as a single column.** `sportsbook_player` has
   three direct book-id columns, each with a unique filtered index
   (`UQ_sportsbook_player_pp`, `UQ_sportsbook_player_ud`,
   `UQ_sportsbook_player_parlay`). This enforces *at most one row per external
   id per book*, which is fine, but combined with the fact that books emit
   multiple ids per person (see below), it forces duplicate canonical rows.
2. **PrizePicks emits many `player_id`s per real person.** Quarter / half /
   full-game variants live in `prizepicks_player` as separate rows with
   distinct `player_id` but a shared stable `ppid`. The sync code tries to
   detect this via `ppid`, but only after a new canonical row has usually
   already been created.
3. **A parallel N:1 table already exists but isn't authoritative.**
   `sportsbook_player_xref(sportsbook, external_player_id)
   → sportsbook_player_id` is designed to map many external ids to one
   canonical row, and the code does write to it for PrizePicks. The
   `sportsbook_player.*_player_id` columns are still filled in parallel,
   though, so the two stores can disagree and both get written to by different
   code paths.
4. **Deduplication is keyed on fragile fields.**
   `_consolidate_sportsbook_player_dupes` merges using
   `(LOWER(display_name), team_abbrev, jersey_number)` — any drift in name
   punctuation ("LeBron James" vs "Lebron James Jr."), team abbreviation
   (trade-day mismatch), or missing jersey number splits the group.
5. **Same pattern on every other dimension.** `sportsbook_team`,
   `sportsbook_stat_type`, `sportsbook_game`, `sportsbook_league`, and
   `sportsbook_sport` all have the same "one column per book" shape and the
   same duplication risk when any book emits multiple ids per thing (Underdog
   solo games, PrizePicks stat variants like "Points" vs "Points (Combo)",
   etc.).

**Target model:** the xref table is the right pattern; we extend it everywhere
and retire the per-book columns on the canonical tables.

- `dbo.sportsbook_player`                     ← one row per real person per league
- `dbo.sportsbook_player_xref (sportsbook, external_player_id) → sportsbook_player_id`
  ← already exists; becomes the only place book-specific ids live.
- `dbo.sportsbook_team_xref`                  ← new, same shape
- `dbo.sportsbook_stat_type_xref`             ← new, same shape
- `dbo.sportsbook_game_xref`                  ← new, same shape
- `dbo.sportsbook_league_xref`                ← new, same shape
- `dbo.sportsbook_sport_xref`                 ← new, same shape

Each xref row also carries an `external_id_kind` column (e.g. `'player_id'`,
`'ppid'`, `'pickem_id'`) so the same book can legitimately register multiple
id namespaces per canonical row without loss (we already prefix these strings
today — this just formalizes it).

**Write path rule (enforced by code + a CHECK constraint later):**

> Scrapers / sync never reference `sportsbook_player.prizepicks_player_id`
> etc. directly. They *always* resolve via `sportsbook_player_xref`, and if no
> match exists they run the resolver and only then create a canonical row.

### 3.0.1 Name / key normalization (the piece that makes dedup reliable)

Add MSSQL UDFs callable from both T-SQL merges and the Python sync:

- `dbo.fn_normalize_person_name(@n nvarchar(255))` → nvarchar(255)
  - lowercase, NFKD-strip accents, remove `.`, `'`, `"`, `,`, `-`,
    collapse whitespace, strip trailing suffixes
    `{jr, sr, ii, iii, iv, v}`. (Python mirror:
    `cross_book_stat_normalize.normalize_person_name` — new.)
- `dbo.fn_normalize_team_abbrev(@a nvarchar(20))` — uppercase + alias map
  (e.g. `'LA' → 'LAL'` for NBA when league is known, `'NOP' = 'NO'`).
- `dbo.fn_normalize_stat_basic(@s nvarchar(120))` — wrap the existing Python
  `normalize_for_join` as a UDF so it can be used in T-SQL dedup keys.
- `dbo.fn_game_natural_key(@league_id, @home_tid, @away_tid, @start_date)`
  — single canonical key for a game across books.

Alias lookup tables (versionable, testable) live under `ref`:

- `ref.team_alias(canonical_league_id, source, alias, canonical_team_id)`
- `ref.stat_alias(canonical_sport_id, source, alias, canonical_stat_type_id)`

### 3.0.2 Canonical unique keys (after normalization)

Replace the current dedup keys with stable natural keys, per table:

| Table                     | Natural key (after normalization)                                       |
|---------------------------|-------------------------------------------------------------------------|
| `sportsbook_sport`        | `display_name_normalized`                                                |
| `sportsbook_league`       | `(canonical_league_id)` OR `(sportsbook_sport_id, display_name_normalized)` |
| `sportsbook_team`         | `(canonical_league_id, abbrev_normalized)` with name fallback            |
| `sportsbook_player`       | `(canonical_league_id, person_name_normalized, team_abbrev_normalized)`  |
| `sportsbook_stat_type`    | `(sportsbook_sport_id, canonical_stat_key)`                               |
| `sportsbook_game`         | `(canonical_league_id, game_date_central, home_team_id, away_team_id)`   |

Each gets a computed-column unique index (`PERSISTED`, filtered on
`IS NOT NULL`) so the DB itself prevents future duplicates.

### 3.0.3 One-time consolidation migration

Written as a numbered migration (`0001_consolidate_sportsbook_dimensions.sql`)
that is **idempotent and re-runnable**, and runs in this order:

1. Create the new xref tables for team / stat_type / game / league / sport.
2. Backfill each xref from the existing per-book columns
   (`sportsbook_team.{parlay_play_team_id,underdog_team_id,...}`, etc.).
3. Compute normalized columns on every row.
4. For each dimension table, pick a survivor per natural key (lowest id),
   `COALESCE` all other rows' attributes onto it, repoint xref rows and
   every downstream FK (`sportsbook_projection`, `sportsbook_projection_history`,
   `sportsbook_game.home_sportsbook_team_id/away_sportsbook_team_id`, etc.),
   then delete the losers.
5. Add the computed-column unique indexes from 3.0.2.
6. Deprecate the per-book columns — keep them for one release so external
   callers don't break, but have them populated by a trigger that just reads
   from the xref ("preferred book id" = min by sort order).

### 3.0.3a Pros / cons vs. keeping the current shape

Two realistic options, plus the hybrid we actually recommend.

**Option A — xref-first (this plan's default).**

*Pros:*
- Correct by construction for many-to-one (e.g. PrizePicks emitting five
  `player_id`s per LeBron is five xref rows, not five canonical rows).
- Adding a new book is data-only; no `ALTER TABLE` on `sportsbook_player`
  every time.
- Canonical rows stop being 2/3-NULL by design.
- One uniform "look up xref → else resolve + create" helper for every scraper.
- Dedup stops depending on name normalization for rows that already carry any
  book's external id; normalization is only needed for brand-new rows.
- Downstream FKs (`sportsbook_projection.sportsbook_player_id`, analytics)
  become stable and unique per real person.
- Clean audit trail per `(book, external_id)` with its own timestamps.

*Cons:*
- Reads that want a book-specific id become a JOIN (or a view). Mildly slower
  and more verbose per call.
- "Which PrizePicks id is the canonical one?" becomes an explicit decision
  (preferred id selection) instead of being hidden by the dedup.
- The consolidation migration is invasive: it repoints every downstream FK
  and deletes losing rows. Failure mid-flight needs careful rollback.
- Ad-hoc SQL and BI dashboards that used
  `SELECT prizepicks_player_id FROM sportsbook_player` need a compatibility
  view (`vw_sportsbook_player_book_ids`) to keep working.
- Still requires the normalization layer to avoid duplicate canonical rows
  when we *first* see a player from different books.

**Option B — keep per-book id columns, harden dedup.**

*Pros:*
- No breaking schema change; every existing query keeps working.
- Fastest "one book" reads (no join).
- Lowest migration risk — DDL is nearly unchanged.
- Most legible shape for SQL-first readers.
- Smaller blast radius if we get normalization wrong (you get NULL-heavy rows,
  not false merges).

*Cons:*
- Does not actually solve the root problem. PrizePicks will keep emitting
  multiple `player_id`s per real person; with `UQ_sportsbook_player_pp` in
  place you still get either duplicate canonical rows or lost-id rows or a
  dropped unique index (at which point you've reinvented xref badly).
- Dedup complexity keeps growing — every new scraper behavior is a new
  special case in `_consolidate_sportsbook_player_dupes`.
- You end up needing an alias table per book anyway (which is an xref in
  disguise).
- Adding a new book is a schema migration every time and the table becomes
  mostly NULL past ~5 books.
- Projection → player joins stay name-based when a projection's external id
  is not the "preferred" one — a latent data-quality issue in analytics.
- Future features (consensus lines, line movement, settled results, player
  pages) all want "one stable id per real person" and will reintroduce xref
  lookups anyway.

**Recommended — Option A via a hybrid rollout:**

1. Build xref tables for every dimension and make them authoritative for
   reads + writes inside the sync code and scrapers (this is the work in
   section 3.0.4).
2. Keep the per-book columns on the canonical tables for one release, but
   have them auto-populated from xref as the "preferred" id per book (via
   trigger or at end of sync). Existing queries keep working unchanged.
3. Run the one-time consolidation migration so today's duplicate rows
   collapse.
4. Add a CI check that every `sportsbook_*` table has exactly one row per
   natural key.
5. Drop the per-book columns in the release after once BI / ad-hoc consumers
   have migrated to `vw_sportsbook_player_book_ids` (a pivot of xref in the
   old column shape).

This gets Option A's correctness with Option B's migration safety. The rest
of section 3.0 is written assuming this sequence.

### 3.0.4 Reconfigured sync logic

`sportsbook_dimension_sync.py` changes:

- All `_sync_*` functions switch to "xref-first": look up via xref, create a
  canonical row only when no xref hit *and* no natural-key match. Current
  code is already close for `_sync_player` PrizePicks path; extend to
  Underdog, ParlayPlay, and to the other dimensions.
- `_consolidate_sportsbook_player_dupes` is replaced by a generic
  `_consolidate_sportsbook_dimension_dupes(table)` that uses the normalized
  natural key from 3.0.2 — it continues to run each sync cycle as a safety net,
  but in the steady state finds zero dupes.
- Emit a metric `sportsbook_dim_dupes_merged_total{table}` so we can alert when
  dedup picks up work (signals a missed alias, a broken normalization, or new
  scraper behavior).

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

### 6.1 Phase 1 — foundations + deduplicate dimension tables (no user-visible changes)

Implements the hybrid rollout for section 3.0. Each numbered item is a
separate PR, independently revertible, and ordered so nothing downstream ever
sees a broken intermediate state. No user-visible behavior changes until
step 1.10.

**1.1 — Migrations runner**

- Add `dbo.schema_migrations (version nvarchar(20) PK, applied_at, checksum,
  script_name)`.
- Add `scripts/migrate.py`: reads `schema/migrations/NNNN_*.sql` in order,
  splits on `GO`, executes in a single transaction per file, records the row
  in `schema_migrations`. Re-runs are idempotent (skips already-applied
  versions).
- Wrap every current idempotent `schema/*.sql` script as migration
  `0000_baseline.sql`. The existing per-feature `run_order.md` docs stay as
  historical breadcrumbs.
- CI job runs `python scripts/migrate.py --check` to fail when a migration
  file is edited after being applied (checksum mismatch).

**1.2 — Normalization UDFs + Python mirrors**

- Migration `0001_normalization_udfs.sql`:
  - `dbo.fn_normalize_person_name(@n nvarchar(255))` — lowercase, strip
    accents, remove `.`, `'`, `"`, `,`, `-`, collapse whitespace, drop
    trailing suffixes (`jr`, `sr`, `ii`, `iii`, `iv`, `v`).
  - `dbo.fn_normalize_team_abbrev(@a nvarchar(20), @canonical_league_id int)`
    — uppercase + alias-table lookup.
  - `dbo.fn_normalize_stat_basic(@s nvarchar(120))` — mirrors the Python
    `normalize_for_join`.
  - `dbo.fn_game_natural_key(@league_id int, @home_tid bigint,
    @away_tid bigint, @start_date date)` — returns a single nvarchar key.
- Add Python mirrors in `cross_book_stat_normalize.py`
  (`normalize_person_name`, `normalize_team_abbrev`, `game_natural_key`) so
  scrapers and T-SQL agree bit-for-bit.
- Unit tests on the Python side, plus a SQL test script that asserts
  UDF output matches Python output for ~50 fixtures.

**1.3 — Alias reference tables**

- Migration `0002_ref_alias_tables.sql` creates `ref` schema and:
  - `ref.team_alias (canonical_league_id int, source nvarchar(30),
    alias nvarchar(40), canonical_team_abbrev nvarchar(20), PRIMARY KEY
    (canonical_league_id, source, alias))`.
  - `ref.stat_alias (canonical_sport_id bigint, source nvarchar(30),
    alias_normalized nvarchar(120), canonical_stat_key nvarchar(80),
    PRIMARY KEY (canonical_sport_id, source, alias_normalized))`.
  - `ref.person_alias (canonical_league_id int, source nvarchar(30),
    alias_normalized nvarchar(255), canonical_display_name nvarchar(255),
    PRIMARY KEY (canonical_league_id, source, alias_normalized))` — optional,
    populated lazily when the dedup audit flags a mismatch.
- Seeds for known trouble cases (harvested from current duplicate rows before
  they are collapsed): a `scripts/seed_aliases.py` that reads the current
  `sportsbook_*` tables, proposes a seed file (`schema/seeds/*.sql`), and
  writes it for review. Reviewing + merging the seed file is part of this PR.

**1.4 — New xref tables for every dimension**

- Migration `0003_sportsbook_xref_tables.sql`:
  - `sportsbook_team_xref`, `sportsbook_stat_type_xref`,
    `sportsbook_game_xref`, `sportsbook_league_xref`,
    `sportsbook_sport_xref` — all shaped like the existing
    `sportsbook_player_xref` plus an extra column
    `external_id_kind nvarchar(20) NOT NULL` (`'id'`, `'ppid'`,
    `'pickem_id'`, …) baked into the PK so one book can register multiple
    id namespaces per canonical row without collision.
- Extend the existing `sportsbook_player_xref` with
  `external_id_kind` (backfilled from the current prefixed
  `external_player_id` values: `'player_id:…' → 'player_id'`,
  `'ppid:…' → 'ppid'`) in the same migration.
- FKs on every xref: `sportsbook_player_xref_player` etc. already exist for
  players; add the equivalents for the new tables.

**1.5 — Normalized-name + natural-key computed columns**

- Migration `0004_natural_key_columns.sql` adds PERSISTED computed columns
  on each dimension (e.g. `sportsbook_player.display_name_normalized AS
  dbo.fn_normalize_person_name(display_name) PERSISTED`) and matching
  filtered unique indexes from section 3.0.2.
- The unique indexes are added **as `NOT FOR REPLICATION`** and created
  online where the edition supports it. If a duplicate survives the
  consolidation migration in 1.6, the index creation fails loudly — this is
  the guardrail that prevents a silent regression.

**1.6 — One-time consolidation migration**

- Migration `0005_consolidate_sportsbook_dimensions.sql`. Runs in a single
  transaction per dimension. For each of sport / league / team / stat_type /
  player / game:
  1. Backfill the xref table from the existing per-book id columns. Every
     non-NULL `sportsbook_player.prizepicks_player_id`, every
     `sportsbook_team.underdog_team_id`, etc. produces one row.
  2. Pick a survivor per natural key (lowest id).
  3. `COALESCE` every non-survivor row's attributes onto the survivor.
  4. Repoint xref rows to the survivor.
  5. Repoint every downstream FK: `sportsbook_projection.sportsbook_player_id`,
     `sportsbook_projection_history.sportsbook_player_id`,
     `sportsbook_game.home_sportsbook_team_id/away_sportsbook_team_id`,
     plus anything surfaced by a `sys.foreign_keys` audit run at the start
     of the migration (recorded in a `#fk_audit` temp table and logged).
  6. Delete the losers.
  7. Assert the unique indexes from 1.5 are valid (they fail loudly if step
     2–5 missed a group).
- Before committing, write a summary row per table to a new audit table
  `dbo.sportsbook_dedup_audit(migration_version, table_name, rows_before,
  rows_after, groups_merged, run_at)` so operators can verify the numbers
  look right before Phase 1 proceeds.
- Ships with a dry-run mode (`--dry-run` on the migrations runner, or
  `@dry_run bit = 1` variable at the top of the script) that runs steps 1–5
  to temp tables and prints what it would do.

**1.7 — Xref-first resolution in sync code**

- Refactor `sportsbook_dimension_sync.py`:
  - Introduce `_resolve_or_create_canonical(conn, dimension, natural_key,
    attributes, external_id_bindings)` — the single helper every `_sync_*`
    function uses. Looks up via xref first, falls back to natural key, only
    creates a canonical row when both miss.
  - `_consolidate_sportsbook_player_dupes` is replaced by
    `_consolidate_sportsbook_dimension_dupes(table)` using the natural-key
    columns from 1.5.
  - Remove direct writes to `sportsbook_player.prizepicks_player_id` etc.
    in the per-book `_sync_*` branches — those columns are now a mirror, not
    the source of truth (see 1.8).
- Extend scrapers (`prizepicks_scraper.py`, `underdog_scraper.py`,
  `parlayplay_scraper.py`) only where they currently write canonical ids
  directly — most write to stage tables, which is unaffected. Any direct
  canonical writes go through the new helper.

**1.8 — "Preferred id per book" mirror (keeps existing queries working)**

- Migration `0006_preferred_book_id_triggers.sql` adds an AFTER INSERT/UPDATE
  trigger on each xref table that recomputes the matching
  canonical-table column (`sportsbook_player.prizepicks_player_id`, etc.)
  as the "preferred" id per book: lowest `external_id_kind` priority order
  (`player_id > ppid`), then earliest `created_at`.
- Alternatively, and preferred for simplicity, a single stored proc
  `dbo.refresh_preferred_book_ids` is called at the end of
  `sync_sportsbook_dimensions(...)`. Triggers are only used if we ever see
  direct xref inserts outside the sync path.
- After this step the per-book columns on the canonical tables still exist
  and still contain sensible values, so every existing query, view, and
  scraper read keeps working unchanged. This is the entire point of the
  hybrid rollout.

**1.9 — Compatibility views + CI guardrails**

- Migration `0007_compat_views.sql`:
  - `dbo.vw_sportsbook_player_book_ids` — pivots xref into the old
    `(sportsbook_player_id, prizepicks_player_id, underdog_player_id,
    parlay_play_player_id, …)` shape so ad-hoc SQL and BI keep a one-line
    migration path for when the columns drop in step 1.11.
  - Equivalent views for `team`, `stat_type`, `game`, `league`, `sport`.
- `scripts/check_dimension_dupes.py` — runs in CI and fails when any
  `sportsbook_*` table has >1 row per natural key, or when the xref tables
  contain rows pointing to deleted canonical rows, or when the preferred-id
  mirror disagrees with xref.

**1.10 — App reads switch to xref-first**

- Update `app/main.py` / `projection_over_streak.py` / `app/db.py` helpers to
  resolve `sportsbook_player_id` for a given scraper payload via
  `sportsbook_player_xref` instead of
  `WHERE prizepicks_player_id = ?`. Keep the old path behind a
  `PROPS_NAME_FALLBACK_ENABLED=1` env flag for one release.
- Once dashboards and queries have run a release cycle on the new path,
  flip the env flag to `0` by default.

**1.11 — Drop per-book columns (separate, later PR — not in Phase 1)**

- Migration `0008_drop_per_book_id_columns.sql` — removes
  `prizepicks_player_id`, `underdog_player_id`, `parlay_play_player_id` from
  `sportsbook_player` (and the equivalents on the other dimensions) plus the
  triggers/procs and the mirror step. Ships only after at least one release
  has been live on the xref-first path and the compat views have stood in
  for ad-hoc consumers. Listed here so it isn't forgotten; do **not** ship
  it as part of Phase 1.

**1.12 — `stage` schema split**

- Move staging-only tables into a new `stage` schema via a migration that
  renames (not copies) and creates synonyms in `dbo` for one release. This
  is independent of the dedup work and is grouped under Phase 1 only
  because it's also a "DDL cleanup" step; it can ship in parallel with or
  after 1.10.

**Phase 1 acceptance criteria**

- Every `sportsbook_*` dimension table has exactly one row per natural key
  (enforced by the filtered unique indexes from 1.5, and verified by the
  CI check in 1.9).
- Every existing query against `sportsbook_player.prizepicks_player_id` (and
  the equivalent columns on the other tables) keeps returning a sensible
  value via the preferred-id mirror.
- `sportsbook_dedup_audit` has a row per dimension showing rows merged, and
  the numbers look plausible when eyeballed.
- `scripts/check_dimension_dupes.py` passes in CI.

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
