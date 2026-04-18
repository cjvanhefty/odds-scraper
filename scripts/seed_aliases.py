"""Propose ref.team_alias / ref.person_alias seed rows from current duplicates.

Connects to the live `Props` DB, finds groups of `sportsbook_player` and
`sportsbook_team` rows that likely refer to the same real-world entity
(based on the Python normalization helpers), and writes a PROPOSED seed
SQL file under `schema/migrations/_proposals/` for operator review.

Nothing is written to the database. Nothing is auto-applied. The output
file is intended to be eyeballed, edited if needed, and then committed as
a real numbered migration (`schema/migrations/NNNN_seed_aliases.sql`) via
a follow-up PR.

Usage (from repo root, venv activated with pyodbc installed):

    python scripts/seed_aliases.py
    # then review the printed path, e.g.
    #   schema/migrations/_proposals/seed_aliases_2026-04-18.sql

Exit codes:
    0  proposals written (even if zero rows proposed -- check the file)
    2  DB connect failed / missing tables
"""

from __future__ import annotations

import os
import sys
from collections import defaultdict
from datetime import date
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import db_config  # noqa: F401

from cross_book_stat_normalize import (  # noqa: E402
    normalize_person_name,
    normalize_team_abbrev,
)


PROPOSALS_DIR = REPO_ROOT / "schema" / "migrations" / "_proposals"


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


def _table_exists(cursor, schema: str, name: str) -> bool:
    cursor.execute(
        "SELECT 1 FROM sys.tables t "
        "INNER JOIN sys.schemas s ON s.schema_id = t.schema_id "
        "WHERE s.name = ? AND t.name = ?",
        (schema, name),
    )
    return cursor.fetchone() is not None


def _nstr(s: str) -> str:
    return "N'" + s.replace("'", "''") + "'"


def _pick_survivor(rows: list[dict], key: str) -> dict:
    """Prefer the row with the most populated fields; tie-break by lowest id."""
    def populated(r: dict) -> int:
        return sum(1 for v in r.values() if v not in (None, ""))
    return sorted(rows, key=lambda r: (-populated(r), r["id"]))[0]


def _propose_team_aliases(cursor) -> list[tuple[int, str, str, str, str, str]]:
    """Return (canonical_league_id, source, alias_normalized, canonical_team_abbrev, alias_raw, notes)."""
    if not _table_exists(cursor, "dbo", "sportsbook_team"):
        return []
    cursor.execute(
        """
        SELECT sportsbook_team_id AS id,
               canonical_league_id,
               prizepicks_team_id, underdog_team_id, parlay_play_team_id,
               abbreviation, full_name
        FROM [dbo].[sportsbook_team]
        WHERE canonical_league_id IS NOT NULL
          AND NULLIF(LTRIM(RTRIM(abbreviation)), N'') IS NOT NULL
        """
    )
    cols = [c[0] for c in cursor.description]
    rows = [dict(zip(cols, r)) for r in cursor.fetchall()]

    # Group by (canonical_league_id, normalized_abbrev). Groups with >1 row
    # are duplicates where the "abbreviation" column is already identical
    # after normalization; those don't need an alias row because
    # fn_normalize_team_abbrev already collapses them. We emit rows only
    # when a team has multiple RAW abbreviations mapping to one canonical.
    groups: dict[tuple[int, str], list[dict]] = defaultdict(list)
    for r in rows:
        abbrev = (r.get("abbreviation") or "").strip()
        if not abbrev:
            continue
        norm = normalize_team_abbrev(abbrev, r.get("canonical_league_id"))
        if not norm:
            continue
        groups[(int(r["canonical_league_id"]), norm)].append(r)

    proposals: list[tuple[int, str, str, str, str, str]] = []
    for (league_id, norm), team_rows in groups.items():
        if len(team_rows) < 2:
            continue
        survivor = _pick_survivor(team_rows, "abbreviation")
        canonical_abbrev = (survivor.get("abbreviation") or "").strip().upper()
        raw_abbrevs = {(r.get("abbreviation") or "").strip(): r for r in team_rows}
        for raw, r in raw_abbrevs.items():
            if not raw:
                continue
            raw_norm = normalize_team_abbrev(raw, league_id)
            if raw_norm == canonical_abbrev:
                continue
            src = "prizepicks" if r.get("prizepicks_team_id") else (
                "underdog" if r.get("underdog_team_id") else (
                "parlay_play" if r.get("parlay_play_team_id") else "_any"))
            notes = (
                f"team row {r['id']} abbrev={raw!r} -> {canonical_abbrev!r} "
                f"(group size {len(team_rows)})"
            )
            proposals.append((league_id, src, raw_norm, canonical_abbrev, raw, notes))
    return proposals


def _propose_person_aliases(cursor) -> list[tuple[int, str, str, str, str, str]]:
    """Return (canonical_league_id, source, alias_normalized, canonical_display_name, alias_raw, notes)."""
    if not _table_exists(cursor, "dbo", "sportsbook_player"):
        return []
    cursor.execute(
        """
        SELECT sportsbook_player_id AS id,
               canonical_league_id,
               prizepicks_player_id, underdog_player_id, parlay_play_player_id,
               display_name, team_abbrev, jersey_number
        FROM [dbo].[sportsbook_player]
        WHERE canonical_league_id IS NOT NULL
          AND NULLIF(LTRIM(RTRIM(display_name)), N'') IS NOT NULL
        """
    )
    cols = [c[0] for c in cursor.description]
    rows = [dict(zip(cols, r)) for r in cursor.fetchall()]

    # Group by (canonical_league_id, normalized_display_name). Rows already
    # sharing the same normalized name will dedupe cleanly in plan step
    # 1.6 via the natural-key unique index; no alias needed. We emit rows
    # only when two raw spellings map to ONE normalized key -- those are
    # the spellings that need the alias table to keep their case/accent
    # variations unified in the frontend.
    groups: dict[tuple[int, str], list[dict]] = defaultdict(list)
    for r in rows:
        raw = (r.get("display_name") or "").strip()
        if not raw:
            continue
        norm = normalize_person_name(raw)
        if not norm:
            continue
        groups[(int(r["canonical_league_id"]), norm)].append(r)

    proposals: list[tuple[int, str, str, str, str, str]] = []
    for (league_id, norm), player_rows in groups.items():
        if len(player_rows) < 2:
            continue
        # Survivor preference: row whose raw display_name appears most
        # often, then fewest NULLs, then lowest id. Raw variants that
        # differ from the survivor's raw display_name become alias rows.
        raw_counts: dict[str, int] = defaultdict(int)
        for r in player_rows:
            raw_counts[(r.get("display_name") or "").strip()] += 1
        canonical_raw = max(
            raw_counts.items(),
            key=lambda kv: (kv[1], -min(r["id"] for r in player_rows if (r.get("display_name") or "").strip() == kv[0])),
        )[0]
        for raw, count in raw_counts.items():
            if raw == canonical_raw or not raw:
                continue
            raw_norm = normalize_person_name(raw)
            if raw_norm != norm:
                # Shouldn't happen (we grouped by norm), but guard anyway.
                continue
            # Figure out the source from any row carrying this raw.
            src_row = next(r for r in player_rows if (r.get("display_name") or "").strip() == raw)
            if src_row.get("prizepicks_player_id"):
                src = "prizepicks"
            elif src_row.get("underdog_player_id"):
                src = "underdog"
            elif src_row.get("parlay_play_player_id"):
                src = "parlay_play"
            else:
                src = "_any"
            notes = (
                f"player row {src_row['id']} raw={raw!r} -> {canonical_raw!r} "
                f"(group size {len(player_rows)}, variant count {count})"
            )
            proposals.append((league_id, src, raw_norm, canonical_raw, raw, notes))
    return proposals


def _render_proposals_sql(
    team_rows: list[tuple[int, str, str, str, str, str]],
    person_rows: list[tuple[int, str, str, str, str, str]],
) -> str:
    today = date.today().isoformat()
    lines: list[str] = [
        f"-- seed_aliases_{today}.sql",
        "-- PROPOSED, not yet a numbered migration.",
        "--",
        "-- Auto-generated by scripts/seed_aliases.py from the current",
        "-- sportsbook_player / sportsbook_team tables. Review every row,",
        "-- edit or delete as needed, then move the file to",
        "-- schema/migrations/NNNN_seed_aliases.sql (next unused N) so the",
        "-- migrations runner applies it.",
        "--",
        f"-- team_alias proposals:   {len(team_rows)}",
        f"-- person_alias proposals: {len(person_rows)}",
        "",
        "SET ANSI_NULLS ON;",
        "SET QUOTED_IDENTIFIER ON;",
        "GO",
        "",
    ]
    if team_rows:
        lines.append("-- team aliases")
        lines.append(
            "MERGE [ref].[team_alias] AS t USING (VALUES"
        )
        row_texts = []
        for (league_id, source, alias_norm, canonical_abbrev, alias_raw, notes) in team_rows:
            row_texts.append(
                f"    ({league_id}, {_nstr(source)}, {_nstr(alias_norm)}, "
                f"{_nstr(canonical_abbrev)}, {_nstr(alias_raw)}, {_nstr(notes)})"
            )
        lines.append(",\n".join(row_texts))
        lines.extend([
            ") AS s(canonical_league_id, source, alias_normalized, canonical_team_abbrev, alias_raw, notes)",
            "    ON t.canonical_league_id = s.canonical_league_id",
            "   AND t.source = s.source",
            "   AND t.alias_normalized = s.alias_normalized",
            "WHEN NOT MATCHED BY TARGET THEN",
            "    INSERT (canonical_league_id, source, alias_normalized, canonical_team_abbrev, alias_raw, notes)",
            "    VALUES (s.canonical_league_id, s.source, s.alias_normalized, s.canonical_team_abbrev, s.alias_raw, s.notes);",
            "GO",
            "",
        ])
    if person_rows:
        lines.append("-- person aliases")
        lines.append(
            "MERGE [ref].[person_alias] AS t USING (VALUES"
        )
        row_texts = []
        for (league_id, source, alias_norm, canonical_name, alias_raw, notes) in person_rows:
            row_texts.append(
                f"    ({league_id}, {_nstr(source)}, {_nstr(alias_norm)}, "
                f"{_nstr(canonical_name)}, {_nstr(alias_raw)}, {_nstr(notes)})"
            )
        lines.append(",\n".join(row_texts))
        lines.extend([
            ") AS s(canonical_league_id, source, alias_normalized, canonical_display_name, alias_raw, notes)",
            "    ON t.canonical_league_id = s.canonical_league_id",
            "   AND t.source = s.source",
            "   AND t.alias_normalized = s.alias_normalized",
            "WHEN NOT MATCHED BY TARGET THEN",
            "    INSERT (canonical_league_id, source, alias_normalized, canonical_display_name, alias_raw, notes)",
            "    VALUES (s.canonical_league_id, s.source, s.alias_normalized, s.canonical_display_name, s.alias_raw, s.notes);",
            "GO",
            "",
        ])
    if not team_rows and not person_rows:
        lines.append("-- No alias proposals found. Either there are no obvious")
        lines.append("-- duplicates today, or the normalization helpers already")
        lines.append("-- collapse every case. This file is safe to delete.")
        lines.append("")
    return "\n".join(lines)


def main() -> int:
    try:
        conn = _connect()
    except Exception as exc:
        print(f"DB connect failed: {exc}", file=sys.stderr)
        return 2
    try:
        cursor = conn.cursor()
        team_proposals = _propose_team_aliases(cursor)
        person_proposals = _propose_person_aliases(cursor)
    finally:
        conn.close()

    PROPOSALS_DIR.mkdir(parents=True, exist_ok=True)
    out_path = PROPOSALS_DIR / f"seed_aliases_{date.today().isoformat()}.sql"
    out_path.write_text(
        _render_proposals_sql(team_proposals, person_proposals), encoding="utf-8"
    )
    print(f"wrote proposals: {out_path}")
    print(f"  team_alias rows:   {len(team_proposals)}")
    print(f"  person_alias rows: {len(person_proposals)}")
    print(
        "Review the file, edit as needed, then move it to "
        "schema/migrations/NNNN_seed_aliases.sql (next unused N) and run "
        "python scripts/migrate.py."
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
