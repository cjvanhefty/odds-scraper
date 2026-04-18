"""Offline parity test: simulate the SQL UDFs in Python and assert they match the fixtures.

We do **not** want to wait for a live MSSQL server to find out that the SQL
migration is wrong. This test reimplements each SQL UDF's logic step by step
against the exact src/dst TRANSLATE strings and the exact VALUES lookup
present in schema/migrations/0001_normalization_udfs.sql, then runs every
fixture through both the Python mirror and the SQL-like simulator, asserting
they agree.

This test fails loudly if someone edits one side without the other.

Run from repo root:

    python3 -m pytest tests/test_sql_parity.py -v
"""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from cross_book_stat_normalize import (
    CANONICAL_STAT_BY_ALNUM,
    apply_join_aliases,
    game_natural_key,
    normalize_for_join,
    normalize_person_name,
    normalize_stat_basic,
    normalize_team_abbrev,
)

FIXTURES_PATH = REPO_ROOT / "tests" / "fixtures" / "normalization.json"
MIGRATION_PATH = REPO_ROOT / "schema" / "migrations" / "0001_normalization_udfs.sql"
REF_ALIASES_MIGRATION_PATH = REPO_ROOT / "schema" / "migrations" / "0003_ref_alias_tables.sql"


# ---------------------------------------------------------------------------
# Extract the exact TRANSLATE(src, dst) strings and the VALUES-lookup pairs
# from the migration file. If anyone changes the SQL, this extraction reflects
# that change and the parity tests below run against the new payload.
# ---------------------------------------------------------------------------


def _extract_diacritic_pairs(sql: str) -> list[tuple[str, str]]:
    """Pull every stacked `SET @s = REPLACE(@s, NCHAR(cp), N'c');` pair.

    Before SQL Server 2017 `TRANSLATE()` was unavailable, so the migration
    builds its diacritic strip as a chain of REPLACE calls inside
    fn_normalize_person_name. Each of those lines maps one lowercase
    accented code point to one lowercase ASCII character; together they
    mirror Python's NFKD strip.

    We scope to the fn_normalize_person_name body so non-diacritic REPLACE
    calls (punctuation, whitespace) elsewhere in the migration aren't
    picked up.
    """
    m = re.search(
        r"CREATE FUNCTION\s+dbo\.fn_normalize_person_name.*?^END\s*$",
        sql,
        re.S | re.M,
    )
    if not m:
        raise RuntimeError("could not find fn_normalize_person_name body")
    body = m.group(0)
    pairs: list[tuple[str, str]] = []
    for mo in re.finditer(
        r"SET\s+@s\s*=\s*REPLACE\(\s*@s\s*,\s*NCHAR\((\d+)\)\s*,\s*N'([^']*)'\s*\)\s*;",
        body,
    ):
        cp = int(mo.group(1))
        dst = mo.group(2)
        # Only count diacritic rows: single-char ASCII replacement for a
        # code point in the Latin Extended range. Punctuation/whitespace
        # REPLACEs use string sources, not NCHAR(cp), so they're filtered
        # out naturally by the regex.
        if cp < 0x00C0 or cp > 0x017F:
            continue
        if len(dst) != 1 or not dst.isascii():
            continue
        pairs.append((chr(cp), dst))
    if not pairs:
        raise RuntimeError("no diacritic REPLACE pairs found in fn_normalize_person_name")
    return pairs


def _extract_canonical_map_from_values(sql: str) -> dict[str, str]:
    """Pull VALUES rows from inside fn_canonical_stat_by_alnum.

    Used against migration 0001. After migration 0003 ships, the function
    body no longer contains a VALUES block (it reads from ref.stat_alias),
    and the canonical map comes from `_extract_canonical_map_from_seed`
    instead. The test below prefers the seed extractor when 0003 is
    present and falls back to this one otherwise, so the parity harness
    works whether or not 0003 has been generated.
    """
    m = re.search(r"FROM\s*\(\s*VALUES\s*(.+?)\s*\)\s*AS\s+m\(k,\s*v\)", sql, re.S | re.I)
    if not m:
        raise RuntimeError("could not find fn_canonical_stat_by_alnum VALUES block")
    body = m.group(1)
    rows = re.findall(r"\(\s*N'((?:[^']|'')*)'\s*,\s*N'((?:[^']|'')*)'\s*\)", body)
    return {k.replace("''", "'"): v.replace("''", "'") for k, v in rows}


def _extract_canonical_map_from_seed(sql: str) -> dict[str, str]:
    """Pull the (source, alias_alnum_key, canonical_label) seed rows from
    migration 0003. Only rows with source=='_any' are returned -- they are
    the mirror of CANONICAL_STAT_BY_ALNUM. Per-book rows (when they
    exist) don't round-trip to that dict.
    """
    m = re.search(
        r"WITH\s+seed\s*\(\s*source\s*,\s*alias_alnum_key\s*,\s*canonical_label\s*\)\s*AS\s*\(\s*SELECT\s*\*\s*FROM\s*\(\s*VALUES\s*(.+?)\s*\)\s*AS\s+v",
        sql,
        re.S | re.I,
    )
    if not m:
        raise RuntimeError("could not find ref.stat_alias seed VALUES block")
    body = m.group(1)
    rows = re.findall(
        r"\(\s*N'((?:[^']|'')*)'\s*,\s*N'((?:[^']|'')*)'\s*,\s*N'((?:[^']|'')*)'\s*\)",
        body,
    )
    out: dict[str, str] = {}
    for source, alias_key, canonical in rows:
        if source != "_any":
            continue
        out[alias_key.replace("''", "'")] = canonical.replace("''", "'")
    return out


MIGRATION_SQL = MIGRATION_PATH.read_text(encoding="utf-8")
DIACRITIC_PAIRS = _extract_diacritic_pairs(MIGRATION_SQL)

# Prefer the seed in 0003 as the authoritative source of the canonical map
# once that migration exists. This mirrors production: after 0003 is
# applied, dbo.fn_canonical_stat_by_alnum reads from ref.stat_alias.
if REF_ALIASES_MIGRATION_PATH.exists():
    REF_ALIASES_SQL = REF_ALIASES_MIGRATION_PATH.read_text(encoding="utf-8")
    CANONICAL_MAP_FROM_SQL = _extract_canonical_map_from_seed(REF_ALIASES_SQL)
else:
    REF_ALIASES_SQL = ""
    CANONICAL_MAP_FROM_SQL = _extract_canonical_map_from_values(MIGRATION_SQL)


# ---------------------------------------------------------------------------
# Python re-implementations of the SQL bodies, using payloads extracted above.
# ---------------------------------------------------------------------------


def _sql_alnum_key(s: str | None) -> str:
    if s is None:
        return ""
    r = s.lower()
    return "".join(c for c in r if ("a" <= c <= "z") or ("0" <= c <= "9"))


def _sql_apply_join_aliases(s: str | None) -> str:
    if s is None:
        return ""
    t = s.strip()
    if not t:
        return ""
    if t in ("Blocks", "Blocked Shots"):
        return "Blocks__Blocked_Shots"
    if t in ("Double Doubles", "Double-Doubles", "Double-Double", "Double Double"):
        return "Double_Doubles"
    if t in ("Triple Doubles", "Triple-Doubles", "Triple-Double"):
        return "Triple_Doubles"
    if t in ("Blocks + Steals", "Blocks+Steals"):
        return "Blks_Stls"
    if t.startswith("Blks+Stls"):
        return "Blks_Stls"
    if t in ("3-PT Attempted", "3 Pointers Attempted", "3s Attempted"):
        return "FG3A"
    if t in ("3 Pointers", "3 Pointers Made", "3-PT Made", "3-Pointers Made"):
        return "FG3M"
    if t in ("Hits+Runs+RBIs", "Hits + Runs + RBIs"):
        return "Hits+Runs+RBIs"
    if t in ("Shots On Target", "Shots on Target"):
        return "Shots On Target"
    if t in ("Goal + Assist", "Goals + Assists"):
        return "Goal + Assist"
    if t in ("Passes Attempted", "Passes"):
        return "Passes Attempted"
    return t


def _sql_canonical_stat_by_alnum(k: str | None) -> str | None:
    if not k:
        return None
    return CANONICAL_MAP_FROM_SQL.get(k)


def _sql_normalize_stat_basic(s: str | None) -> str:
    if s is None:
        return ""
    t = s.strip()
    if not t:
        return ""
    mapped = _sql_canonical_stat_by_alnum(_sql_alnum_key(t))
    return t if mapped is None else mapped


def _sql_ne_ci(a: str, b: str) -> bool:
    """Simulate SQL Server's default case-insensitive accent-insensitive <>.

    Most MSSQL deployments (including the Props DB) use a CI collation as
    the default, so `N'Shots On Target' <> N'Shots on Target'` returns
    FALSE in T-SQL even though Python's `!=` returns True. Any UDF that
    uses `<>` to distinguish "alias applied" from "alias didn't apply"
    must either use a binary collation (COLLATE Latin1_General_BIN2) or
    accept CI semantics.

    fn_normalize_for_join in migration 0002 uses the binary collation
    explicitly. The simulator here models the DEFAULT collation so we can
    write a regression test asserting the SIM without the binary COLLATE
    would have reproduced the operator-reported bug, and that with it,
    it doesn't.
    """
    return a.casefold() != b.casefold()


def _sql_normalize_for_join(s: str | None, *, use_binary_collation: bool = True) -> str:
    """Simulator for fn_normalize_for_join.

    use_binary_collation=True mirrors the fix in migration 0002 (the
    production behavior). use_binary_collation=False mirrors the buggy
    pre-fix behavior under a CI collation, used by
    test_normalize_for_join_regresses_without_binary_collation below.
    """
    if s is None:
        return ""
    t = s.strip()
    if not t:
        return ""
    pre = _sql_apply_join_aliases(t)
    # Binary (case-sensitive) comparison matches SQL's
    # '@pre <> @trimmed COLLATE Latin1_General_BIN2'.
    differs = (pre != t) if use_binary_collation else _sql_ne_ci(pre, t)
    if differs:
        return pre
    mapped = _sql_canonical_stat_by_alnum(_sql_alnum_key(t))
    if mapped is None:
        return t
    return _sql_apply_join_aliases(mapped)


def _sql_normalize_person_name(n: str | None) -> str:
    """Mirror of the SQL body. The migration lowercases first, then does a
    chain of REPLACEs on lowercase code points, so we do the same here."""
    if n is None:
        return ""
    s = n
    # 1. lowercase first (matches the migration's SET @s = LOWER(@s) step)
    s = s.lower()
    # 2. diacritic strip using the exact pairs parsed from the migration.
    for src_ch, dst_ch in DIACRITIC_PAIRS:
        if src_ch in s:
            s = s.replace(src_ch, dst_ch)
    # 3. remove . , ' " - and whitespace chars
    for ch in ".,'\"-":
        s = s.replace(ch, "")
    s = s.replace("\t", " ").replace("\r", " ").replace("\n", " ")
    # 4. collapse spaces (stacked REPLACE 4x)
    for _ in range(4):
        s = s.replace("  ", " ")
    # 5. trim
    s = s.strip()
    if not s:
        return ""
    # 6. drop trailing generational suffix
    if " " in s:
        last = s.rsplit(" ", 1)[1]
        if last in ("jr", "sr", "ii", "iii", "iv", "v"):
            s = s.rsplit(" ", 1)[0].strip()
    return s


def _sql_normalize_team_abbrev(abbrev: str | None, _league_id: int | None = None) -> str:
    if abbrev is None:
        return ""
    t = abbrev.strip()
    if not t:
        return ""
    return t.upper()


def _sql_game_natural_key(league_id, home_team_id, away_team_id, start_date) -> str:
    # SQL builds the string using ISNULL(CAST(... AS nvarchar), N'').
    # CONVERT(nvarchar(10), @start_date, 23) is YYYY-MM-DD or '' when the param
    # is NULL. The live-DB checker passes a parseable date or NULL; mirror that.
    def part(v):
        return "" if v is None else str(v)
    date_part = ""
    if start_date is not None:
        if hasattr(start_date, "isoformat"):
            try:
                date_part = start_date.isoformat()[:10]
            except Exception:
                date_part = ""
        else:
            raw = str(start_date).strip()
            if len(raw) >= 10 and raw[4] == "-" and raw[7] == "-":
                date_part = raw[:10]
    return "|".join([part(league_id), date_part, part(home_team_id), part(away_team_id)])


# ---------------------------------------------------------------------------
# Fixture parity tests. Each test runs the fixture through the Python mirror
# AND through the SQL simulator and asserts both equal the fixture's expected
# output.
# ---------------------------------------------------------------------------


FIXTURES = json.loads(FIXTURES_PATH.read_text(encoding="utf-8"))


def _ids(cases: list[dict]) -> list[str]:
    return [repr(c.get("in")) for c in cases]


@pytest.mark.parametrize("case", FIXTURES["person_name"], ids=_ids(FIXTURES["person_name"]))
def test_person_name_sql_matches_python(case):
    sql_out = _sql_normalize_person_name(case["in"])
    py_out = normalize_person_name(case["in"])
    assert sql_out == case["out"], f"SQL simulator: {sql_out!r} != {case['out']!r}"
    assert py_out == case["out"], f"Python helper: {py_out!r} != {case['out']!r}"
    assert sql_out == py_out, f"SQL {sql_out!r} disagrees with Python {py_out!r}"


@pytest.mark.parametrize("case", FIXTURES["team_abbrev"], ids=_ids(FIXTURES["team_abbrev"]))
def test_team_abbrev_sql_matches_python(case):
    sql_out = _sql_normalize_team_abbrev(case["in"], None)
    py_out = normalize_team_abbrev(case["in"], None)
    assert sql_out == case["out"]
    assert py_out == case["out"]
    assert sql_out == py_out


@pytest.mark.parametrize("case", FIXTURES["stat_basic"], ids=_ids(FIXTURES["stat_basic"]))
def test_stat_basic_sql_matches_python(case):
    sql_out = _sql_normalize_stat_basic(case["in"])
    py_out = normalize_stat_basic(case["in"])
    assert sql_out == case["out"]
    assert py_out == case["out"]
    assert sql_out == py_out


@pytest.mark.parametrize("case", FIXTURES["stat_for_join"], ids=_ids(FIXTURES["stat_for_join"]))
def test_stat_for_join_sql_matches_python(case):
    sql_out = _sql_normalize_for_join(case["in"])
    py_out = normalize_for_join(case["in"])
    assert sql_out == case["out"]
    assert py_out == case["out"]
    assert sql_out == py_out


@pytest.mark.parametrize(
    "case",
    FIXTURES["game_natural_key"],
    ids=[repr(c.get("in")) for c in FIXTURES["game_natural_key"]],
)
def test_game_natural_key_sql_matches_python(case):
    args = case["in"]
    sql_out = _sql_game_natural_key(
        args.get("league_id"),
        args.get("home_team_id"),
        args.get("away_team_id"),
        args.get("start_date"),
    )
    py_out = game_natural_key(
        args.get("league_id"),
        args.get("home_team_id"),
        args.get("away_team_id"),
        args.get("start_date"),
    )
    assert sql_out == case["out"]
    assert py_out == case["out"]
    assert sql_out == py_out


def test_migration_canonical_map_matches_python_source() -> None:
    """The VALUES rows in the migration must match CANONICAL_STAT_BY_ALNUM exactly.

    This is the single strongest guarantee that the Python mapping and the
    migration stay synchronized: if anyone touches one, this test fires.
    """
    assert CANONICAL_MAP_FROM_SQL == CANONICAL_STAT_BY_ALNUM


def test_migration_diacritic_pairs_strip_to_ascii() -> None:
    """Every REPLACE(src, dst) in fn_normalize_person_name must strip via NFKD.

    Each pair is (lowercase accented char, lowercase ASCII). Python's NFKD of
    the *same-case* source character must produce the destination, so that
    SQL's REPLACE-chain and Python's normalize_person_name agree after the
    LOWER() step that both sides do first.
    """
    import unicodedata

    assert DIACRITIC_PAIRS, "parser returned no pairs"
    for src_ch, dst_ch in DIACRITIC_PAIRS:
        stripped = "".join(
            c for c in unicodedata.normalize("NFKD", src_ch) if not unicodedata.combining(c)
        )
        assert stripped == dst_ch, (hex(ord(src_ch)), stripped, dst_ch)


def test_normalize_for_join_regresses_without_binary_collation() -> None:
    """Regression guard for the 'Shots on Target' bug fixed in migration 0002.

    Under the default CI collation (simulated by use_binary_collation=False),
    fn_normalize_for_join('Shots on Target') returns the trimmed input
    unchanged instead of the aliased 'Shots On Target', because the
    `@pre <> @trimmed` comparison collapses case differences. This test
    asserts both that the buggy simulator reproduces the bug *and* that
    the fixed simulator does not, so a future edit that removes the
    binary COLLATE clause would break this test before hitting prod.
    """
    buggy = _sql_normalize_for_join("Shots on Target", use_binary_collation=False)
    fixed = _sql_normalize_for_join("Shots on Target", use_binary_collation=True)
    assert buggy == "Shots on Target", (
        "expected the CI-collation simulator to reproduce the original bug "
        f"('Shots on Target' unchanged); got {buggy!r}"
    )
    assert fixed == "Shots On Target"


def test_apply_join_aliases_python_matches_sql_simulator() -> None:
    """Every fixture for normalize_for_join covers apply_join_aliases indirectly,
    but sanity-check the Python and SQL simulators agree directly too."""
    sample_inputs = [
        "Blocks",
        "Blocked Shots",
        "Blks+Stls Something",
        "Blks+Stls",
        "3 Pointers",
        "3 Pointers Made",
        "Goal + Assist",
        "Goals + Assists",
        "Passes",
        "Passes Attempted",
        "",
        None,
        "Foo",
        "Double-Double",
    ]
    for s in sample_inputs:
        py = apply_join_aliases(s or "")
        sim = _sql_apply_join_aliases(s)
        assert py == sim, (s, py, sim)
