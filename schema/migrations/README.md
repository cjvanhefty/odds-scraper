# Schema migrations

Versioned, sequential SQL migrations for the `Props` SQL Server database. A
thin Python runner (`scripts/migrate.py`) applies them in order and records
each one in `dbo.schema_migrations`.

## Why this directory exists

Historically the repo has dozens of idempotent DDL scripts in `schema/*.sql`
with a human-readable run order in per-feature `*_run_order.md` files. That
shape is fine for bootstrapping a new database but provides no way to know
which environment is at which version, and no way to prevent an already-applied
script from being edited in place. Migrations (this directory) replace that
with a simple, auditable append-only sequence.

## File naming

```
NNNN_snake_case.sql
```

- `NNNN` is a 4-digit, zero-padded, strictly increasing version. No skipped
  numbers.
- The rest is a short snake_case description. No spaces, no uppercase.

Example: `0005_consolidate_sportsbook_dimensions.sql`.

## Rules

1. **Never edit a file after it has been applied in any environment.** The
   runner records a SHA-256 of the file contents and will fail
   `scripts/migrate.py --check` (and refuse `apply` without `--force-drift`)
   if the checksum stops matching. If you need to change what an already-
   applied migration did, write a new migration that makes the correction.
2. **Each file is its own transaction.** The runner wraps every batch in the
   file plus the `schema_migrations` insert in a single user transaction, so
   a failure anywhere rolls the whole migration back. If you need a
   migration that cannot run inside a user transaction (e.g. `ALTER DATABASE`,
   some online index ops), make the very first non-blank line of the file
   exactly:

   ```sql
   -- pragma: no-transaction
   ```

   The runner will then execute each batch with autocommit. These should be
   rare and the file should still be safe to retry on failure.
3. **Batches are split on `^\s*GO\s*$`** (SQL Server client-side batch
   separator; the runner mimics `sqlcmd` here).
4. **Keep migrations idempotent when cheap to do so.** `IF NOT EXISTS` guards
   around `CREATE TABLE` / `CREATE INDEX` make half-applied migrations easier
   to recover. Not every migration can be idempotent; that's fine.
5. **No destructive changes without a paired rollback plan.** Drops and
   renames should only land after the mirroring / compatibility work on the
   previous migration has been deployed for at least one release.

## Common operations

```bash
# Apply everything pending
python scripts/migrate.py

# Show status for every discovered and every recorded migration
python scripts/migrate.py --list

# CI-safe: fail if anything is pending or has drifted
python scripts/migrate.py --check

# Execute pending migrations inside a transaction that's always rolled back
python scripts/migrate.py --dry-run

# Stop after a specific version (inclusive)
python scripts/migrate.py --target 0003

# Record a version as applied without running its SQL; used once per existing
# database during the initial baseline cutover.
python scripts/migrate.py --mark-applied 0000
```

## The 0000 sentinel

`0000_baseline.sql` in this directory is intentionally a no-op. Existing
databases already contain every object produced by today's `schema/*.sql`
files, so the first real migration (`0001+`) will land on top of that
already-present schema. On a brand-new database, the current bootstrap path
remains "run the scripts described in `schema/*_run_order.md`, then
`python scripts/migrate.py --mark-applied 0000`". A follow-up migration
planned as step 1.1b in `docs/database_and_app_plan.md` will replace this
sentinel with a real from-zero baseline.

## Where migrations come from

Each migration corresponds to a step in the delivery plan in
[`docs/database_and_app_plan.md`](../../docs/database_and_app_plan.md).
Reference the step number in the migration file's header comment (e.g.
`-- Plan step 1.2 — normalization UDFs`) so reviewers can cross-check scope.

## Environment variables

Same as the rest of the codebase; loaded automatically from repo-root `.env`
via `db_config.py`:

| Variable                           | Default                 |
|------------------------------------|-------------------------|
| `PROPS_DB_SERVER`                  | `localhost\SQLEXPRESS`  |
| `PROPS_DATABASE`                   | `Props`                 |
| `PROPS_DB_USER`                    | `dbadmin`               |
| `PROPS_DB_PASSWORD`                |                         |
| `PROPS_DB_USE_TRUSTED_CONNECTION`  | unset (=SQL auth)       |
