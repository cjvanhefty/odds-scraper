"""Pytest suite for cross_book_stat_normalize helpers.

Shares fixtures with scripts/check_sql_udfs.py (the live-DB SQL checker) so
SQL-side behavior and Python-side behavior stay locked together.

Run from repo root:

    python3 -m pytest tests/test_normalization.py -v
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from cross_book_stat_normalize import (
    game_natural_key,
    normalize_for_join,
    normalize_person_name,
    normalize_stat_basic,
    normalize_team_abbrev,
)

FIXTURES_PATH = REPO_ROOT / "tests" / "fixtures" / "normalization.json"


def _load_fixtures() -> dict:
    return json.loads(FIXTURES_PATH.read_text(encoding="utf-8"))


def _ids(cases: list[dict]) -> list[str]:
    return [repr(c.get("in")) for c in cases]


FIXTURES = _load_fixtures()


@pytest.mark.parametrize("case", FIXTURES["person_name"], ids=_ids(FIXTURES["person_name"]))
def test_normalize_person_name(case: dict) -> None:
    assert normalize_person_name(case["in"]) == case["out"]


@pytest.mark.parametrize("case", FIXTURES["team_abbrev"], ids=_ids(FIXTURES["team_abbrev"]))
def test_normalize_team_abbrev(case: dict) -> None:
    assert normalize_team_abbrev(case["in"]) == case["out"]


@pytest.mark.parametrize("case", FIXTURES["stat_basic"], ids=_ids(FIXTURES["stat_basic"]))
def test_normalize_stat_basic(case: dict) -> None:
    assert normalize_stat_basic(case["in"]) == case["out"]


@pytest.mark.parametrize("case", FIXTURES["stat_for_join"], ids=_ids(FIXTURES["stat_for_join"]))
def test_normalize_for_join(case: dict) -> None:
    assert normalize_for_join(case["in"]) == case["out"]


@pytest.mark.parametrize(
    "case",
    FIXTURES["game_natural_key"],
    ids=[repr(c.get("in")) for c in FIXTURES["game_natural_key"]],
)
def test_game_natural_key(case: dict) -> None:
    args = case["in"]
    got = game_natural_key(
        args.get("league_id"),
        args.get("home_team_id"),
        args.get("away_team_id"),
        args.get("start_date"),
    )
    assert got == case["out"]
