# AGENTS.md

Operator and agent guide for working on this repo.

## What this project actually is

Cross-sportsbook player-prop aggregator. Scrapes **PrizePicks**, **Underdog**,
and **Parlay Play**, normalizes their leagues / players / stats into a
unified shape, joins projections to historical stats (currently NBA and
soccer), and serves the result through a FastAPI + static-HTML web app.

**Database:** Microsoft SQL Server (tested on 2016+), database name
`Props`. There is **no** SQLite anywhere in this repo — any reference
you see to SQLite, `backend/data/props.db`, or a `backend/` directory is
stale and should be ignored.

## Top-level layout

```
.
├── app/                              FastAPI service + single-file static UI
│   ├── main.py                       routes + unified projection query logic
│   ├── db.py                         pyodbc connection helper
│   └── static/index.html             vanilla JS frontend
├── nba/nba_scraper.py                NBA box-score feed via nba_api
├── soccer/soccer_scraper.py          soccerdata feed
├── prizepicks_scraper.py             PrizePicks scraper (+ Playwright fallback)
├── underdog_scraper.py               Underdog scraper (+ Playwright fallback)
├── parlayplay_scraper.py             Parlay Play scraper (+ Playwright fallback)
├── sportsbook_dimension_sync.py      per-book reference -> unified sportsbook_* dims
├── sportsbook_projection_sync.py     per-book projections -> sportsbook_projection
├── projection_over_streak.py         joins projections to NBA last-N box scores
├── cross_book_stat_normalize.py      canonical stat/league/person-name helpers
├── db_config.py                      loads .env (PROPS_DB_*) at import time
├── requirements.txt                  Python deps (pyodbc, pandas, playwright, fastapi, …)
├── .env.example                      copy to .env and fill in DB creds
├── schema/                           SQL DDL + versioned migrations
│   ├── migrations/                   numbered migrations applied by scripts/migrate.py
│   │   └── _proposals/               operator-only alias seed drafts (gitignored)
│   ├── *.sql                         legacy idempotent DDL (pre-migrations era)
│   └── *_run_order.md                legacy bootstrap order docs
├── scripts/
│   ├── migrate.py                    schema migration runner
│   ├── check_sql_udfs.py             live-DB parity check for normalization UDFs
│   ├── seed_aliases.py               harvest proposed ref.*_alias rows from live DB
│   └── _gen_*.py                     generators that emit migrations from Python source
├── tests/                            pytest (offline parity + fixture tests)
│   ├── fixtures/normalization.json   shared Python+SQL normalization fixtures
│   ├── test_normalization.py
│   └── test_sql_parity.py            parses migrations + asserts SQL==Python==fixture
└── docs/
    └── database_and_app_plan.md      accepted multi-phase plan (hybrid rollout)
```

There is no `backend/` directory. There is no `scraper/` directory. There
is no `prizepicks_nba.py`. Don't create them — existing code lives in
the top-level files above.

## Environment setup

```bash
# From repo root, inside a fresh venv:
pip install -r requirements.txt

# Playwright is only needed if a scraper falls back to browser mode
# (PrizePicks / Underdog / Parlay Play sometimes 403 direct HTTP):
playwright install chromium

# DB credentials:
cp .env.example .env
# Edit .env; either set PROPS_DB_USE_TRUSTED_CONNECTION=1 (Windows auth)
# or set PROPS_DB_USER / PROPS_DB_PASSWORD (SQL auth).
```

### Windows note

On Windows the Python executable is `python`, not `python3`. Commands in
this doc use `python` consistently. Linux/WSL users should substitute
`python3` everywhere.

## Running things

| Task | Command |
|------|---------|
| Scrape PrizePicks into DB | `python prizepicks_scraper.py --all-leagues --db` |
| Scrape Underdog into DB | `python underdog_scraper.py --db` |
| Scrape Parlay Play into DB | `python parlayplay_scraper.py --db` |
| Sync per-book ref -> sportsbook_* dims | `python sportsbook_dimension_sync.py` |
| Sync per-book proj -> sportsbook_projection | `python sportsbook_projection_sync.py` |
| Run the FastAPI app | `uvicorn app.main:app --reload` (serves `/` + `/api/*`) |
| List migration status | `python scripts/migrate.py --list` |
| Apply pending migrations | `python scripts/migrate.py` |
| Dry-run pending migrations | `python scripts/migrate.py --dry-run` |
| Verify UDF parity against fixtures | `python scripts/check_sql_udfs.py` |

## Database migrations

**Every schema change lands as a new numbered file under
`schema/migrations/NNNN_*.sql`.** The runner records each in
`dbo.schema_migrations` with a SHA-256 checksum and refuses to apply
further migrations if an already-applied file has drifted.

Rules (also in `schema/migrations/README.md`):

1. Never edit a migration file after it has been applied in any
   environment. Add a new migration on top to fix mistakes.
2. Each file is one transaction. `GO` separates batches; the runner
   commits the whole file or none of it.
3. Files that cannot run in a user transaction (some `ALTER DATABASE`,
   some online index ops) opt out with `-- pragma: no-transaction` as
   the first non-blank line.
4. When `ALTER`ing a schemabound function that other schemabound
   functions reference, `DROP` the dependents first and recreate them
   with identical bodies in the same file. SQL Server raises error
   3729 otherwise.
5. When a UDF uses `<>` or `=` or `LIKE` to drive a `RETURN`, force a
   binary collation (`COLLATE Latin1_General_BIN2`) on that
   comparison. The default collation is case-insensitive on this
   deployment and will silently misroute control flow.

### Generators

`scripts/_gen_*_migration.py` files produce migrations from Python
source (`cross_book_stat_normalize.py` mappings, etc.) so Python and
SQL can't drift. Regenerate only *before* the migration has been
applied anywhere:

```bash
python scripts/_gen_0001_migration.py > schema/migrations/0001_normalization_udfs.sql
```

After it ships, corrections land as new numbered migrations.

## Testing

```bash
python -m pytest tests/
```

What the tests cover:

- `tests/test_normalization.py` — every `cross_book_stat_normalize.py`
  helper against `tests/fixtures/normalization.json`.
- `tests/test_sql_parity.py` — parses the latest migration that owns
  each UDF, reimplements the SQL logic in Python, and asserts the
  simulator, the Python helper, and the fixture all agree. Plus
  structural guards (canonical map byte-identical between source and
  migration; diacritic pairs NFKD-correct; known regression tests).

There is no lint config. There is no CI yet. `python -m pytest tests/`
and a successful `scripts/migrate.py --check` against a live DB are the
two checks that matter.

## Working style for agents

- **Plan doc is canonical:** `docs/database_and_app_plan.md` is the
  accepted roadmap for the current Phase-1 work (hybrid rollout of the
  xref-first deduplication model). Every implementation PR should
  cross-reference the step number (e.g. "Plan step 1.4").
- **One PR per plan step.** Branch off `main`, implement the step, open
  a draft PR, wait for the operator to apply and verify before starting
  the next step. The operator runs the migrations and the parity
  checker against the real DB; this is the source of truth, not a CI
  pipeline.
- **Additive-only for dimension work.** Phase 1 migrations must not
  break `sportsbook_dimension_sync.py` or any existing query. The
  per-book id columns on `sportsbook_player`, `sportsbook_team`, etc.
  stay populated via triggers/procs until step 1.11 retires them.
- **No SQLAlchemy, no Alembic, no ORMs.** Every SQL change is hand-
  authored (or generator-authored) raw T-SQL. `pyodbc` is the only DB
  client.

## What to ignore

- `db_update` (top level) — an *experimental* target schema from an
  earlier design exercise. It is not live DDL. Do not apply it. The
  accepted long-term direction is in `docs/database_and_app_plan.md`.
- Any reference in older docs to SQLite or a `backend/` directory.
  This project has never used SQLite.
