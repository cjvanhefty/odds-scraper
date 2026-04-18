"""Schema migration runner for the Props SQL Server database.

Usage (from repo root):

    python scripts/migrate.py               # apply all pending migrations
    python scripts/migrate.py --list        # show status of all migrations
    python scripts/migrate.py --check       # CI-friendly: fail if drift/pending
    python scripts/migrate.py --dry-run     # apply inside a rolled-back tx
    python scripts/migrate.py --target 0003 # stop after version 0003
    python scripts/migrate.py --mark-applied 0000
                                            # record a version as applied
                                            # without executing it (for existing
                                            # databases that are already at that
                                            # state)
    python scripts/migrate.py --sync-checksum 0001
                                            # refresh dbo.schema_migrations.checksum
                                            # for an applied version after editing
                                            # that file on disk (then apply/--check)

Migration files live in ``schema/migrations/`` and are named
``NNNN_snake_case.sql`` where ``NNNN`` is a 4-digit zero-padded version.

Each file is executed as a single transaction spanning every ``GO``-separated
batch, plus the insert into ``dbo.schema_migrations``, so a failure inside any
batch rolls the whole migration back. Files whose first non-blank line is
``-- pragma: no-transaction`` are executed without a wrapping user transaction
(for statements that cannot run inside one, such as some online index ops).

Environment variables (same as the rest of the codebase; loaded from repo-root
``.env`` via ``db_config``):

    PROPS_DB_SERVER              default: localhost\\SQLEXPRESS
    PROPS_DATABASE               default: Props
    PROPS_DB_USER                default: dbadmin
    PROPS_DB_PASSWORD
    PROPS_DB_USE_TRUSTED_CONNECTION   "1"/"true"/"yes" for Windows auth
"""

from __future__ import annotations

import argparse
import hashlib
import os
import re
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import db_config  # noqa: F401 — loads .env into PROPS_DB_* before we read them

MIGRATIONS_DIR = REPO_ROOT / "schema" / "migrations"
VERSION_RE = re.compile(r"^(?P<version>\d{4})_(?P<name>[A-Za-z0-9_\-]+)\.sql$")
_GO_SPLIT_RE = re.compile(r"^\s*GO\s*$", flags=re.IGNORECASE | re.MULTILINE)
_NO_TX_RE = re.compile(r"^\s*--\s*pragma:\s*no-transaction\s*$", flags=re.IGNORECASE)


@dataclass(frozen=True)
class Migration:
    version: str
    name: str
    path: Path

    @property
    def script_name(self) -> str:
        return self.path.name

    def read_text(self) -> str:
        return self.path.read_text(encoding="utf-8-sig")

    def checksum(self) -> str:
        return _checksum_sql(self.read_text())


def _checksum_sql(text: str) -> str:
    """SHA-256 hex of the migration after normalizing line endings + trailing ws.

    Normalization keeps Windows CRLF and trailing whitespace from silently
    failing ``--check`` across platforms.
    """
    lines = [ln.rstrip() for ln in text.replace("\r\n", "\n").replace("\r", "\n").split("\n")]
    while lines and lines[-1] == "":
        lines.pop()
    return hashlib.sha256(("\n".join(lines) + "\n").encode("utf-8")).hexdigest()


def _discover_migrations(migrations_dir: Path = MIGRATIONS_DIR) -> list[Migration]:
    if not migrations_dir.exists():
        return []
    migrations: list[Migration] = []
    seen_versions: dict[str, Path] = {}
    for entry in sorted(migrations_dir.iterdir()):
        if not entry.is_file() or entry.suffix.lower() != ".sql":
            continue
        m = VERSION_RE.match(entry.name)
        if not m:
            raise RuntimeError(
                f"Migration file {entry.name!r} does not match NNNN_snake_case.sql"
            )
        version = m.group("version")
        if version in seen_versions:
            raise RuntimeError(
                f"Duplicate migration version {version!r}: "
                f"{seen_versions[version].name} and {entry.name}"
            )
        seen_versions[version] = entry
        migrations.append(Migration(version=version, name=m.group("name"), path=entry))
    return migrations


def _sql_batches(sql: str) -> list[str]:
    return [b.strip() for b in _GO_SPLIT_RE.split(sql) if b and b.strip()]


def _first_non_blank_line(sql: str) -> str:
    for raw in sql.replace("\r\n", "\n").split("\n"):
        if raw.strip():
            return raw
    return ""


def _wants_no_transaction(sql: str) -> bool:
    return bool(_NO_TX_RE.match(_first_non_blank_line(sql)))


def _connect():
    import pyodbc

    server = os.environ.get("PROPS_DB_SERVER", "localhost\\SQLEXPRESS")
    database = os.environ.get("PROPS_DATABASE", "Props")
    user = os.environ.get("PROPS_DB_USER", "dbadmin")
    password = os.environ.get("PROPS_DB_PASSWORD", "")
    trusted = (
        os.environ.get("PROPS_DB_USE_TRUSTED_CONNECTION", "").strip().lower()
        or os.environ.get("PROPS_DB_TRUSTED", "").strip().lower()
    ) in ("1", "true", "yes")

    base_driver = "ODBC Driver 17 for SQL Server"
    for driver in (base_driver, "SQL Server"):
        if trusted:
            conn_str = (
                f"DRIVER={{{driver}}};SERVER={server};DATABASE={database};"
                f"Trusted_Connection=yes;"
            )
        else:
            conn_str = (
                f"DRIVER={{{driver}}};SERVER={server};DATABASE={database};"
                f"UID={user};PWD={password or ''}"
            )
        try:
            return pyodbc.connect(conn_str, autocommit=False)
        except pyodbc.Error:
            last_err_driver = driver
            continue
    raise RuntimeError(
        f"Could not connect to {database} on {server} with any known ODBC driver "
        f"(last tried: {last_err_driver})"
    )


def _ensure_schema_migrations_table(conn) -> None:
    """Create ``dbo.schema_migrations`` on first run. Idempotent."""
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            IF NOT EXISTS (
                SELECT 1 FROM sys.tables
                WHERE name = 'schema_migrations' AND schema_id = SCHEMA_ID('dbo')
            )
            BEGIN
                CREATE TABLE [dbo].[schema_migrations](
                    [version] NVARCHAR(20) NOT NULL,
                    [script_name] NVARCHAR(255) NOT NULL,
                    [checksum] CHAR(64) NOT NULL,
                    [applied_at] DATETIME2(7) NOT NULL
                        CONSTRAINT [DF_schema_migrations_applied_at]
                        DEFAULT (CONVERT(datetime2(7), SYSUTCDATETIME()
                            AT TIME ZONE 'UTC' AT TIME ZONE 'Central Standard Time')),
                    [applied_by] NVARCHAR(128) NULL
                        CONSTRAINT [DF_schema_migrations_applied_by]
                        DEFAULT (SUSER_SNAME()),
                    [execution_ms] INT NULL,
                    CONSTRAINT [PK_schema_migrations]
                        PRIMARY KEY CLUSTERED ([version] ASC)
                );
            END
            """
        )
        conn.commit()
    finally:
        cursor.close()


def _fetch_applied(conn) -> dict[str, dict]:
    cursor = conn.cursor()
    try:
        cursor.execute(
            "SELECT version, script_name, checksum, applied_at, applied_by, execution_ms "
            "FROM dbo.schema_migrations"
        )
        out: dict[str, dict] = {}
        for row in cursor.fetchall():
            out[str(row[0])] = {
                "version": str(row[0]),
                "script_name": row[1],
                "checksum": row[2],
                "applied_at": row[3],
                "applied_by": row[4],
                "execution_ms": row[5],
            }
        return out
    finally:
        cursor.close()


def _record_migration(conn, migration: Migration, execution_ms: int) -> None:
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            INSERT INTO dbo.schema_migrations
                (version, script_name, checksum, execution_ms)
            VALUES (?, ?, ?, ?)
            """,
            (migration.version, migration.script_name, migration.checksum(), execution_ms),
        )
    finally:
        cursor.close()


def _execute_migration_in_transaction(conn, migration: Migration) -> int:
    """Run every batch + record row inside one user transaction. Returns ms elapsed."""
    sql = migration.read_text()
    batches = _sql_batches(sql)
    cursor = conn.cursor()
    start = time.perf_counter()
    try:
        for batch in batches:
            cursor.execute(batch)
        _record_migration(conn, migration, int((time.perf_counter() - start) * 1000))
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()
    return int((time.perf_counter() - start) * 1000)


def _execute_migration_no_transaction(conn, migration: Migration) -> int:
    """Run each batch with autocommit semantics (for --pragma: no-transaction files)."""
    sql = migration.read_text()
    batches = _sql_batches(sql)
    cursor = conn.cursor()
    start = time.perf_counter()
    try:
        conn.autocommit = True
        for batch in batches:
            cursor.execute(batch)
        conn.autocommit = False
        _record_migration(conn, migration, int((time.perf_counter() - start) * 1000))
        conn.commit()
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
        conn.autocommit = False
        raise
    finally:
        cursor.close()
    return int((time.perf_counter() - start) * 1000)


def _apply_migration(conn, migration: Migration) -> int:
    sql = migration.read_text()
    if _wants_no_transaction(sql):
        return _execute_migration_no_transaction(conn, migration)
    return _execute_migration_in_transaction(conn, migration)


def _filter_until_target(
    migrations: Iterable[Migration], target: str | None
) -> list[Migration]:
    if target is None:
        return list(migrations)
    out: list[Migration] = []
    for m in migrations:
        out.append(m)
        if m.version == target:
            return out
    raise RuntimeError(f"--target version {target!r} not found among discovered migrations")


def _cmd_list(args) -> int:
    migrations = _discover_migrations()
    conn = _connect()
    try:
        _ensure_schema_migrations_table(conn)
        applied = _fetch_applied(conn)
    finally:
        conn.close()
    header = f"{'version':<8} {'status':<10} {'checksum_ok':<12} {'applied_at':<27} script"
    print(header)
    print("-" * len(header))
    applied_versions = set(applied)
    disk_versions = {m.version for m in migrations}
    for m in migrations:
        rec = applied.get(m.version)
        if rec is None:
            status, checksum_ok, applied_at = "pending", "-", ""
        else:
            status = "applied"
            checksum_ok = "ok" if rec["checksum"] == m.checksum() else "DRIFT"
            applied_at = str(rec["applied_at"] or "")
        print(f"{m.version:<8} {status:<10} {checksum_ok:<12} {applied_at:<27} {m.script_name}")
    # Report orphan versions: applied in DB but file missing on disk.
    for v in sorted(applied_versions - disk_versions):
        rec = applied[v]
        print(f"{v:<8} {'applied':<10} {'MISSING':<12} {str(rec['applied_at']):<27} {rec['script_name']} (file removed)")
    return 0


def _cmd_check(args) -> int:
    migrations = _discover_migrations()
    conn = _connect()
    try:
        _ensure_schema_migrations_table(conn)
        applied = _fetch_applied(conn)
    finally:
        conn.close()
    problems: list[str] = []
    disk_versions = {m.version for m in migrations}
    for m in migrations:
        rec = applied.get(m.version)
        if rec is None:
            problems.append(f"pending: {m.script_name}")
        elif rec["checksum"] != m.checksum():
            problems.append(
                f"drift: {m.script_name} was edited after being applied "
                f"(db={rec['checksum'][:12]}…, file={m.checksum()[:12]}…)"
            )
    for v in sorted(set(applied) - disk_versions):
        problems.append(f"missing on disk: {applied[v]['script_name']} (version {v})")
    if problems:
        print("schema_migrations check FAILED:", file=sys.stderr)
        for p in problems:
            print(f"  - {p}", file=sys.stderr)
        return 1
    print(f"schema_migrations check OK ({len(migrations)} migration(s), all applied + clean)")
    return 0


def _cmd_sync_checksum(args) -> int:
    """Point ``schema_migrations.checksum`` at the current file for an applied version."""
    migrations = {m.version: m for m in _discover_migrations()}
    version = str(args.sync_checksum).strip()
    if version not in migrations:
        print(f"version {version!r} not found in {MIGRATIONS_DIR}", file=sys.stderr)
        return 2
    m = migrations[version]
    conn = _connect()
    try:
        _ensure_schema_migrations_table(conn)
        applied = _fetch_applied(conn)
        if version not in applied:
            print(
                f"version {version} is not recorded in dbo.schema_migrations; "
                f"nothing to sync (use a normal apply or --mark-applied first).",
                file=sys.stderr,
            )
            return 2
        cursor = conn.cursor()
        try:
            new_cs = m.checksum()
            cursor.execute(
                "UPDATE dbo.schema_migrations SET checksum = ? WHERE version = ?",
                (new_cs, version),
            )
            if cursor.rowcount != 1:
                print(
                    f"UPDATE schema_migrations expected 1 row, got {cursor.rowcount}",
                    file=sys.stderr,
                )
                conn.rollback()
                return 1
            conn.commit()
        finally:
            cursor.close()
        print(
            f"synced checksum for {m.script_name} (version {version}) "
            f"to file hash {new_cs[:12]}…"
        )
        return 0
    finally:
        conn.close()


def _cmd_mark_applied(args) -> int:
    migrations = {m.version: m for m in _discover_migrations()}
    version = args.mark_applied
    if version not in migrations:
        print(f"version {version!r} not found in {MIGRATIONS_DIR}", file=sys.stderr)
        return 2
    m = migrations[version]
    conn = _connect()
    try:
        _ensure_schema_migrations_table(conn)
        applied = _fetch_applied(conn)
        if version in applied:
            print(f"version {version} already recorded; no change")
            return 0
        cursor = conn.cursor()
        try:
            cursor.execute(
                "INSERT INTO dbo.schema_migrations (version, script_name, checksum, execution_ms) "
                "VALUES (?, ?, ?, 0)",
                (m.version, m.script_name, m.checksum()),
            )
            conn.commit()
        finally:
            cursor.close()
        print(f"recorded {m.script_name} as applied (no SQL executed)")
        return 0
    finally:
        conn.close()


def _cmd_apply(args) -> int:
    migrations = _discover_migrations()
    if not migrations:
        print(f"no migrations found under {MIGRATIONS_DIR}")
        return 0
    migrations = _filter_until_target(migrations, args.target)

    conn = _connect()
    try:
        _ensure_schema_migrations_table(conn)
        applied = _fetch_applied(conn)

        drift = [
            m for m in migrations
            if m.version in applied
            and applied[m.version]["checksum"] != m.checksum()
        ]
        if drift:
            print("refusing to apply: checksum drift on already-applied migration(s):", file=sys.stderr)
            for m in drift:
                rec = applied[m.version]
                print(
                    f"  - {m.script_name}: db={rec['checksum'][:12]}…, "
                    f"file={m.checksum()[:12]}…",
                    file=sys.stderr,
                )
            print(
                "Edit a new migration on top of the existing one instead of "
                "editing the applied file. Use --force-drift to override.",
                file=sys.stderr,
            )
            if not args.force_drift:
                return 2

        pending = [m for m in migrations if m.version not in applied]
        if not pending:
            print("no pending migrations")
            return 0

        for m in pending:
            label = f"{m.version} {m.script_name}"
            if args.dry_run:
                conn.autocommit = False
                cursor = conn.cursor()
                start = time.perf_counter()
                try:
                    for batch in _sql_batches(m.read_text()):
                        cursor.execute(batch)
                    ms = int((time.perf_counter() - start) * 1000)
                    conn.rollback()
                    print(f"[dry-run] would apply {label} ({ms} ms; rolled back)")
                except Exception as exc:
                    conn.rollback()
                    print(f"[dry-run] FAILED on {label}: {exc}", file=sys.stderr)
                    return 1
                finally:
                    cursor.close()
                continue
            try:
                ms = _apply_migration(conn, m)
            except Exception as exc:
                print(f"FAILED on {label}: {exc}", file=sys.stderr)
                return 1
            print(f"applied {label} ({ms} ms)")
        return 0
    finally:
        conn.close()


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Schema migration runner for the Props SQL Server database."
    )
    sub = parser.add_mutually_exclusive_group()
    sub.add_argument("--list", action="store_true", help="Print migration status and exit.")
    sub.add_argument(
        "--check",
        action="store_true",
        help="CI-friendly: exit non-zero if any migration is pending, drifted, or missing on disk.",
    )
    sub.add_argument(
        "--mark-applied",
        metavar="VERSION",
        help="Record VERSION as applied without executing its SQL (for pre-existing databases).",
    )
    sub.add_argument(
        "--sync-checksum",
        metavar="VERSION",
        help=(
            "Update the recorded checksum for an already-applied VERSION to match the file "
            "on disk (use after intentionally editing that migration; then run apply/--check)."
        ),
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Execute pending migrations inside a transaction that is always rolled back.",
    )
    parser.add_argument(
        "--target",
        metavar="VERSION",
        help="Stop after this version (inclusive).",
    )
    parser.add_argument(
        "--force-drift",
        action="store_true",
        help="Apply pending migrations even if already-applied migrations show checksum drift.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    if args.list:
        return _cmd_list(args)
    if args.check:
        return _cmd_check(args)
    if args.mark_applied:
        return _cmd_mark_applied(args)
    if args.sync_checksum:
        return _cmd_sync_checksum(args)
    return _cmd_apply(args)


if __name__ == "__main__":
    sys.exit(main())
