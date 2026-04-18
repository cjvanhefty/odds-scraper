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


# ---------------------------------------------------------------------------
# Extract the exact TRANSLATE(src, dst) strings and the VALUES-lookup pairs
# from the migration file. If anyone changes the SQL, this extraction reflects
# that change and the parity tests below run against the new payload.
# ---------------------------------------------------------------------------


def _extract_translate_pairs(sql: str) -> tuple[str, str]:
    m = re.search(
        r"SET\s+@s\s*=\s*TRANSLATE\(\s*@s\s*,\s*N'([^']+)'\s*,\s*\n\s*N'([^']+)'\s*\)\s*;",
        sql,
        re.S,
    )
    if not m:
        raise RuntimeError("could not find TRANSLATE() payload in migration")
    return m.group(1), m.group(2)


def _extract_canonical_map(sql: str) -> dict[str, str]:
    """Pull only the VALUES rows from inside fn_canonical_stat_by_alnum.

    The function body looks like::

        FROM (VALUES
            (N'pts', N'Points'),
            (N'points', N'Points'),
            ...
        ) AS m(k, v)

    We lock onto that block so we don't pick up IN-list pairs elsewhere in
    the migration (e.g. inside fn_apply_join_aliases).
    """
    m = re.search(r"FROM\s*\(\s*VALUES\s*(.+?)\s*\)\s*AS\s+m\(k,\s*v\)", sql, re.S | re.I)
    if not m:
        raise RuntimeError("could not find fn_canonical_stat_by_alnum VALUES block")
    body = m.group(1)
    rows = re.findall(r"\(\s*N'((?:[^']|'')*)'\s*,\s*N'((?:[^']|'')*)'\s*\)", body)
    return {k.replace("''", "'"): v.replace("''", "'") for k, v in rows}


MIGRATION_SQL = MIGRATION_PATH.read_text(encoding="utf-8")
TRANSLATE_SRC, TRANSLATE_DST = _extract_translate_pairs(MIGRATION_SQL)
CANONICAL_MAP_FROM_SQL = _extract_canonical_map(MIGRATION_SQL)


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


def _sql_normalize_for_join(s: str | None) -> str:
    if s is None:
        return ""
    t = s.strip()
    if not t:
        return ""
    pre = _sql_apply_join_aliases(t)
    if pre != t:
        return pre
    mapped = _sql_canonical_stat_by_alnum(_sql_alnum_key(t))
    if mapped is None:
        return t
    return _sql_apply_join_aliases(mapped)


def _sql_normalize_person_name(n: str | None) -> str:
    if n is None:
        return ""
    s = n
    # 1. TRANSLATE using the exact pairs from the migration
    trans = str.maketrans({src: dst for src, dst in zip(TRANSLATE_SRC, TRANSLATE_DST)})
    s = s.translate(trans)
    # 2. lower
    s = s.lower()
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


def test_migration_translate_pairs_strip_to_ascii() -> None:
    """Every TRANSLATE source char must NFKD-strip to exactly its dst char."""
    import unicodedata

    assert len(TRANSLATE_SRC) == len(TRANSLATE_DST)
    for s, d in zip(TRANSLATE_SRC, TRANSLATE_DST):
        stripped = "".join(
            c for c in unicodedata.normalize("NFKD", s) if not unicodedata.combining(c)
        )
        assert stripped == d, (hex(ord(s)), stripped, d)


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
