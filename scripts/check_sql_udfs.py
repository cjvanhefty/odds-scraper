"""Live-DB parity check between cross_book_stat_normalize.py and the SQL UDFs.

Runs every row in tests/fixtures/normalization.json through the matching
dbo.fn_* function and asserts the SQL output equals the fixture's expected
output. Intended to be run by the operator after applying migration
0001_normalization_udfs.sql, as an independent validation that the Python
and SQL sides agree.

Usage (from repo root):

    python3 scripts/check_sql_udfs.py

Exits 0 on full parity, 1 on any mismatch (printing the failing fixtures),
and 2 on connection / environment errors.

Reads DB credentials from the same PROPS_DB_* env vars as the rest of the
codebase (via db_config).
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import db_config  # noqa: F401

FIXTURES_PATH = REPO_ROOT / "tests" / "fixtures" / "normalization.json"


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

    for driver in ("ODBC Driver 17 for SQL Server", "SQL Server"):
        if trusted:
            cs = (
                f"DRIVER={{{driver}}};SERVER={server};DATABASE={database};"
                f"Trusted_Connection=yes;"
            )
        else:
            cs = (
                f"DRIVER={{{driver}}};SERVER={server};DATABASE={database};"
                f"UID={user};PWD={password or ''}"
            )
        try:
            return pyodbc.connect(cs)
        except pyodbc.Error:
            continue
    raise RuntimeError("Could not connect to Props with any known ODBC driver")


def _scalar(cursor, sql: str, params: tuple) -> object:
    cursor.execute(sql, params)
    row = cursor.fetchone()
    return None if row is None else row[0]


def _check_single_arg(cursor, fn: str, cases: list[dict]) -> list[str]:
    failures: list[str] = []
    for c in cases:
        expected = c["out"]
        got = _scalar(cursor, f"SELECT {fn}(?) AS v", (c["in"],))
        # NULL → empty string: SQL funcs above also return N'' on NULL, so comparison is direct.
        if got != expected:
            failures.append(f"{fn}({c['in']!r}) → {got!r}, expected {expected!r}")
    return failures


def _check_team_abbrev(cursor, cases: list[dict]) -> list[str]:
    failures: list[str] = []
    for c in cases:
        got = _scalar(
            cursor, "SELECT dbo.fn_normalize_team_abbrev(?, CAST(NULL AS int)) AS v", (c["in"],)
        )
        if got != c["out"]:
            failures.append(f"fn_normalize_team_abbrev({c['in']!r}) → {got!r}, expected {c['out']!r}")
    return failures


def _check_game_key(cursor, cases: list[dict]) -> list[str]:
    failures: list[str] = []
    for c in cases:
        args = c["in"]
        raw_date = args.get("start_date")
        # Mirror Python: only 'YYYY-MM-DD' prefix is honored. We pass a real DATE
        # when parseable; otherwise NULL, which is what Python does for garbage.
        date_arg = None
        if isinstance(raw_date, str) and len(raw_date) >= 10 and raw_date[4] == "-" and raw_date[7] == "-":
            date_arg = raw_date[:10]
        got = _scalar(
            cursor,
            "SELECT dbo.fn_game_natural_key(?, ?, ?, CAST(? AS date)) AS v",
            (args.get("league_id"), args.get("home_team_id"), args.get("away_team_id"), date_arg),
        )
        if got != c["out"]:
            failures.append(
                f"fn_game_natural_key({args}) → {got!r}, expected {c['out']!r}"
            )
    return failures


def main() -> int:
    try:
        fixtures = json.loads(FIXTURES_PATH.read_text(encoding="utf-8"))
    except FileNotFoundError:
        print(f"fixtures not found: {FIXTURES_PATH}", file=sys.stderr)
        return 2

    try:
        conn = _connect()
    except Exception as exc:
        print(f"DB connect failed: {exc}", file=sys.stderr)
        return 2

    try:
        cursor = conn.cursor()
        all_failures: list[str] = []
        all_failures += _check_single_arg(cursor, "dbo.fn_normalize_person_name", fixtures["person_name"])
        all_failures += _check_team_abbrev(cursor, fixtures["team_abbrev"])
        all_failures += _check_single_arg(cursor, "dbo.fn_normalize_stat_basic", fixtures["stat_basic"])
        all_failures += _check_single_arg(cursor, "dbo.fn_normalize_for_join", fixtures["stat_for_join"])
        all_failures += _check_game_key(cursor, fixtures["game_natural_key"])
        total = (
            len(fixtures["person_name"])
            + len(fixtures["team_abbrev"])
            + len(fixtures["stat_basic"])
            + len(fixtures["stat_for_join"])
            + len(fixtures["game_natural_key"])
        )
        if all_failures:
            print(f"FAIL: {len(all_failures)}/{total} fixtures disagree:", file=sys.stderr)
            for f in all_failures:
                print(f"  - {f}", file=sys.stderr)
            return 1
        print(f"OK: {total}/{total} fixtures agree between Python and SQL")
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
