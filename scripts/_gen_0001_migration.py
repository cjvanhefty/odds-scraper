"""One-shot generator for schema/migrations/0001_normalization_udfs.sql.

Reads the canonical stat mapping from cross_book_stat_normalize.py and emits
a VALUES-table lookup inside dbo.fn_canonical_stat_by_alnum, so SQL and
Python share a single source of truth.

Usage:

    python3 scripts/_gen_0001_migration.py > schema/migrations/0001_normalization_udfs.sql

The output is committed to the repo. Run this again only when the Python
mapping changes AND step 1.3 (alias table migration) is not yet live; once
1.3 is in place the UDF will read from a table instead.
"""
from __future__ import annotations

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

import unicodedata  # noqa: E402

from cross_book_stat_normalize import CANONICAL_STAT_BY_ALNUM  # noqa: E402


def _nstr(s: str) -> str:
    return "N'" + s.replace("'", "''") + "'"


def _build_translate_pairs() -> tuple[str, str]:
    """Return (src, dst) strings for the SQL TRANSLATE() call.

    We include exactly the characters whose NFKD normalization strips to a
    single ASCII character, matching Python's normalize_person_name behavior.
    Characters that NFKD leaves alone (Ø, ø, Đ, đ, ł, Ł, ı, ...) are
    *intentionally excluded* so SQL leaves them alone too; if Python keeps
    them as-is, SQL must as well for fixture parity.
    """
    src_parts: list[str] = []
    dst_parts: list[str] = []
    for cp in range(0x00C0, 0x0180):
        ch = chr(cp)
        stripped = "".join(
            c for c in unicodedata.normalize("NFKD", ch) if not unicodedata.combining(c)
        )
        if len(stripped) == 1 and stripped != ch and stripped.isascii():
            src_parts.append(ch)
            dst_parts.append(stripped)
    src = "".join(src_parts)
    dst = "".join(dst_parts)
    assert len(src) == len(dst)
    return src, dst


def _sql_unicode_literal(s: str) -> str:
    """Emit the string as ``N'...'`` with literal characters.

    The migration file is committed as UTF-8 and read by scripts/migrate.py
    with the ``utf-8-sig`` codec; pyodbc then sends it over NVARCHAR. This
    round-trip preserves non-ASCII characters. Escapes ``'`` by doubling.
    """
    return "N'" + s.replace("'", "''") + "'"


def _gen_values_rows() -> str:
    rows = [f"        ({_nstr(k)}, {_nstr(v)})" for k, v in CANONICAL_STAT_BY_ALNUM.items()]
    return ",\n".join(rows)


HEADER = """\
-- 0001_normalization_udfs.sql
--
-- Plan step 1.2 — cross-book normalization UDFs.
--
-- Creates the deterministic, SCHEMABINDING-safe scalar UDFs used by
-- cross-book dedup (the 'five LeBrons' fix in plan section 3.0) and by the
-- unified sportsbook_* dimension indexes that arrive in plan step 1.5.
--
-- Every UDF mirrors a Python helper in cross_book_stat_normalize.py. Fixture
-- parity is asserted by:
--   * tests/test_normalization.py (Python side, CI).
--   * scripts/check_sql_udfs.py   (live DB side; operator runs after migrate).
--
-- The CANONICAL_STAT_BY_ALNUM mapping is inlined here as a VALUES table.
-- Plan step 1.3 introduces ref.stat_alias and this function will be replaced
-- (via a new migration) with a read from that table.
--
-- This migration file is auto-generated from cross_book_stat_normalize.py by
-- scripts/_gen_0001_migration.py. To regenerate:
--     python3 scripts/_gen_0001_migration.py > schema/migrations/0001_normalization_udfs.sql
-- Do NOT edit a migration file after it has been applied; add a new
-- migration on top instead (see schema/migrations/README.md).

SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
GO

"""


ALNUM_KEY = """\
-- dbo.fn_alnum_key(@s)
-- Mirror of Python _alnum_key: lowercase @s then strip every character that
-- is not [a-z0-9]. Used by the stat canonicalization lookup.
IF OBJECT_ID(N'dbo.fn_alnum_key', N'FN') IS NOT NULL DROP FUNCTION dbo.fn_alnum_key;
GO
CREATE FUNCTION dbo.fn_alnum_key(@s nvarchar(400))
RETURNS nvarchar(400)
WITH SCHEMABINDING
AS
BEGIN
    IF @s IS NULL RETURN N'';
    DECLARE @r  nvarchar(400) = LOWER(@s);
    DECLARE @n  int = LEN(@r);
    DECLARE @i  int = 1;
    DECLARE @c  nchar(1);
    DECLARE @out nvarchar(400) = N'';
    WHILE @i <= @n
    BEGIN
        SET @c = SUBSTRING(@r, @i, 1);
        IF (@c >= N'a' AND @c <= N'z') OR (@c >= N'0' AND @c <= N'9')
            SET @out = @out + @c;
        SET @i += 1;
    END
    RETURN @out;
END
GO

"""


APPLY_JOIN_ALIASES = """\
-- dbo.fn_apply_join_aliases(@s)
-- Mirror of Python apply_join_aliases: label-level bucketing for cross-book
-- joins. Returns the trimmed input when no alias applies (matches Python's
-- "return t" branch).
IF OBJECT_ID(N'dbo.fn_apply_join_aliases', N'FN') IS NOT NULL DROP FUNCTION dbo.fn_apply_join_aliases;
GO
CREATE FUNCTION dbo.fn_apply_join_aliases(@s nvarchar(120))
RETURNS nvarchar(120)
WITH SCHEMABINDING
AS
BEGIN
    IF @s IS NULL RETURN N'';
    DECLARE @t nvarchar(120) = LTRIM(RTRIM(@s));
    IF LEN(@t) = 0 RETURN N'';
    IF @t IN (N'Blocks', N'Blocked Shots') RETURN N'Blocks__Blocked_Shots';
    IF @t IN (N'Double Doubles', N'Double-Doubles', N'Double-Double', N'Double Double') RETURN N'Double_Doubles';
    IF @t IN (N'Triple Doubles', N'Triple-Doubles', N'Triple-Double') RETURN N'Triple_Doubles';
    IF @t IN (N'Blocks + Steals', N'Blocks+Steals') RETURN N'Blks_Stls';
    IF @t LIKE N'Blks+Stls%' RETURN N'Blks_Stls';
    IF @t IN (N'3-PT Attempted', N'3 Pointers Attempted', N'3s Attempted') RETURN N'FG3A';
    IF @t IN (N'3 Pointers', N'3 Pointers Made', N'3-PT Made', N'3-Pointers Made') RETURN N'FG3M';
    IF @t IN (N'Hits+Runs+RBIs', N'Hits + Runs + RBIs') RETURN N'Hits+Runs+RBIs';
    IF @t IN (N'Shots On Target', N'Shots on Target') RETURN N'Shots On Target';
    IF @t IN (N'Goal + Assist', N'Goals + Assists') RETURN N'Goal + Assist';
    IF @t IN (N'Passes Attempted', N'Passes') RETURN N'Passes Attempted';
    RETURN @t;
END
GO

"""


def canonical_stat_body() -> str:
    return f"""\
-- dbo.fn_canonical_stat_by_alnum(@k)
-- Lookup of alnum key -> PrizePicks-style canonical label. Returns NULL on
-- miss so callers can distinguish "no mapping" from "maps to empty".
-- Contents mirror CANONICAL_STAT_BY_ALNUM in cross_book_stat_normalize.py;
-- plan step 1.3 will move this to a ref.stat_alias table lookup.
IF OBJECT_ID(N'dbo.fn_canonical_stat_by_alnum', N'FN') IS NOT NULL DROP FUNCTION dbo.fn_canonical_stat_by_alnum;
GO
CREATE FUNCTION dbo.fn_canonical_stat_by_alnum(@k nvarchar(120))
RETURNS nvarchar(120)
WITH SCHEMABINDING
AS
BEGIN
    IF @k IS NULL OR LEN(@k) = 0 RETURN NULL;
    DECLARE @v nvarchar(120);
    SELECT @v = m.v
    FROM (VALUES
{_gen_values_rows()}
    ) AS m(k, v)
    WHERE m.k = @k;
    RETURN @v;
END
GO

"""


NORMALIZE_STAT_BASIC = """\
-- dbo.fn_normalize_stat_basic(@s)
-- Mirror of Python normalize_stat_basic: canonicalize a provider stat label
-- to the PrizePicks-style display label, falling back to the trimmed input.
IF OBJECT_ID(N'dbo.fn_normalize_stat_basic', N'FN') IS NOT NULL DROP FUNCTION dbo.fn_normalize_stat_basic;
GO
CREATE FUNCTION dbo.fn_normalize_stat_basic(@s nvarchar(120))
RETURNS nvarchar(120)
WITH SCHEMABINDING
AS
BEGIN
    IF @s IS NULL RETURN N'';
    DECLARE @trimmed nvarchar(120) = LTRIM(RTRIM(@s));
    IF LEN(@trimmed) = 0 RETURN N'';
    DECLARE @mapped nvarchar(120) = dbo.fn_canonical_stat_by_alnum(dbo.fn_alnum_key(@trimmed));
    IF @mapped IS NULL RETURN @trimmed;
    RETURN @mapped;
END
GO

"""


NORMALIZE_FOR_JOIN = """\
-- dbo.fn_normalize_for_join(@s)
-- Mirror of Python normalize_for_join: bucket aliases win first; otherwise
-- canonicalize via the alnum map and re-bucket the canonical form.
IF OBJECT_ID(N'dbo.fn_normalize_for_join', N'FN') IS NOT NULL DROP FUNCTION dbo.fn_normalize_for_join;
GO
CREATE FUNCTION dbo.fn_normalize_for_join(@s nvarchar(120))
RETURNS nvarchar(120)
WITH SCHEMABINDING
AS
BEGIN
    IF @s IS NULL RETURN N'';
    DECLARE @trimmed nvarchar(120) = LTRIM(RTRIM(@s));
    IF LEN(@trimmed) = 0 RETURN N'';
    DECLARE @pre nvarchar(120) = dbo.fn_apply_join_aliases(@trimmed);
    IF @pre <> @trimmed RETURN @pre;
    DECLARE @mapped nvarchar(120) = dbo.fn_canonical_stat_by_alnum(dbo.fn_alnum_key(@trimmed));
    IF @mapped IS NULL RETURN @trimmed;
    RETURN dbo.fn_apply_join_aliases(@mapped);
END
GO

"""


def person_name_body() -> str:
    src, dst = _build_translate_pairs()
    # Emit one REPLACE call per distinct (src_char, dst_char) pair so this
    # migration works on SQL Server 2016 and earlier (TRANSLATE is 2017+).
    # We lowercase first so both the uppercase ('É') and lowercase ('é') forms
    # collapse to a single lowercase ASCII replacement, halving the REPLACE
    # count. The resulting stacked expression is ugly but deterministic and
    # runs on every supported SQL Server version.
    pairs: list[tuple[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for s, d in zip(src, dst):
        s_lower = s.lower()
        d_lower = d.lower()
        if len(s_lower) != 1 or len(d_lower) != 1:
            # Some accented characters (e.g. U+0130 LATIN CAPITAL LETTER I
            # WITH DOT ABOVE) lowercase to multiple chars under the Unicode
            # default case-fold. Those can't be expressed as a single
            # REPLACE(NCHAR(cp), ...) call and are rare enough in our
            # player feeds to skip — if one shows up it just stays
            # unchanged, matching the Python behavior after LOWER()
            # introduces the same expansion.
            continue
        key = (s_lower, d_lower)
        if key in seen:
            continue
        seen.add(key)
        pairs.append(key)

    replace_lines = []
    for s, d in pairs:
        replace_lines.append(
            f"    SET @s = REPLACE(@s, NCHAR({ord(s)}), N'{d}');  -- {s} -> {d}"
        )
    replace_block = "\n".join(replace_lines)

    return f"""\
-- dbo.fn_normalize_person_name(@n)
-- Mirror of Python normalize_person_name. Steps, in order:
--   1. Lowercase.
--   2. Strip diacritics on common Latin-1/Extended-A accented characters
--      via stacked REPLACE(@s, NCHAR(cp), 'ascii'). TRANSLATE would be
--      cleaner but is SQL Server 2017+; this works on 2016 and earlier.
--      The (code point, ascii) pairs are auto-generated from
--      unicodedata.normalize('NFKD', ch) for cp in [0x00C0..0x017F] where
--      the stripped form is exactly one ASCII character. Characters whose
--      NFKD strip is a no-op (O-slash, stroked L, dotless i, long s)
--      are intentionally omitted so SQL leaves them alone, matching
--      Python's NFKD behavior.
--   3. Remove ., , ' " - and treat tab/CR/LF as spaces.
--   4. Collapse consecutive spaces (stacked REPLACE handles up to 16 in a run).
--   5. Trim.
--   6. Drop a single trailing generational suffix from {{jr, sr, ii, iii, iv, v}}.
-- NULL input maps to N'' to match the Python helper.
IF OBJECT_ID(N'dbo.fn_normalize_person_name', N'FN') IS NOT NULL DROP FUNCTION dbo.fn_normalize_person_name;
GO
CREATE FUNCTION dbo.fn_normalize_person_name(@n nvarchar(255))
RETURNS nvarchar(255)
WITH SCHEMABINDING
AS
BEGIN
    IF @n IS NULL RETURN N'';
    DECLARE @s nvarchar(300) = @n;
    -- 1. Lowercase first so we only need one REPLACE per lowercase code point.
    SET @s = LOWER(@s);
    -- 2. Diacritic strip ({len(pairs)} pairs, auto-generated from unicodedata).
{replace_block}
    -- 3. Remove . , ' " - and normalize whitespace chars.
    SET @s = REPLACE(@s, N'.', N'');
    SET @s = REPLACE(@s, N',', N'');
    SET @s = REPLACE(@s, N'''', N'');
    SET @s = REPLACE(@s, N'"', N'');
    SET @s = REPLACE(@s, N'-', N'');
    SET @s = REPLACE(@s, NCHAR(9), N' ');
    SET @s = REPLACE(@s, NCHAR(13), N' ');
    SET @s = REPLACE(@s, NCHAR(10), N' ');
    -- 4. Collapse runs of spaces (stacked REPLACE; handles up to 16 consecutive spaces).
    SET @s = REPLACE(REPLACE(REPLACE(REPLACE(@s, N'  ', N' '), N'  ', N' '), N'  ', N' '), N'  ', N' ');
    -- 5. Trim.
    SET @s = LTRIM(RTRIM(@s));
    IF LEN(@s) = 0 RETURN N'';
    -- 6. Drop trailing generational suffix (only when there is a preceding token).
    IF CHARINDEX(N' ', @s) > 0
    BEGIN
        DECLARE @spc int = LEN(@s) - CHARINDEX(N' ', REVERSE(@s)) + 1;
        DECLARE @last nvarchar(10) = SUBSTRING(@s, @spc + 1, LEN(@s) - @spc);
        IF @last IN (N'jr', N'sr', N'ii', N'iii', N'iv', N'v')
            SET @s = LTRIM(RTRIM(SUBSTRING(@s, 1, @spc - 1)));
    END
    RETURN @s;
END
GO

"""


TEAM_ABBREV = """\
-- dbo.fn_normalize_team_abbrev(@abbrev, @canonical_league_id)
-- Mirror of Python normalize_team_abbrev: uppercase + trim only at step 1.2.
-- The @canonical_league_id parameter is accepted now so callers don't need
-- to change when plan step 1.3 layers a ref.team_alias lookup on top.
IF OBJECT_ID(N'dbo.fn_normalize_team_abbrev', N'FN') IS NOT NULL DROP FUNCTION dbo.fn_normalize_team_abbrev;
GO
CREATE FUNCTION dbo.fn_normalize_team_abbrev(@abbrev nvarchar(20), @canonical_league_id int)
RETURNS nvarchar(20)
WITH SCHEMABINDING
AS
BEGIN
    IF @abbrev IS NULL RETURN N'';
    DECLARE @t nvarchar(20) = LTRIM(RTRIM(@abbrev));
    IF LEN(@t) = 0 RETURN N'';
    RETURN UPPER(@t);
END
GO

"""


GAME_KEY = """\
-- dbo.fn_game_natural_key(@league_id, @home_team_id, @away_team_id, @start_date)
-- Mirror of Python game_natural_key. Always returns a 4-part pipe-delimited
-- string; missing parts appear as empty between pipes.
IF OBJECT_ID(N'dbo.fn_game_natural_key', N'FN') IS NOT NULL DROP FUNCTION dbo.fn_game_natural_key;
GO
CREATE FUNCTION dbo.fn_game_natural_key(
    @league_id     int,
    @home_team_id  bigint,
    @away_team_id  bigint,
    @start_date    date
)
RETURNS nvarchar(80)
WITH SCHEMABINDING
AS
BEGIN
    DECLARE @lid nvarchar(20) = ISNULL(CAST(@league_id AS nvarchar(20)), N'');
    DECLARE @dt  nvarchar(10) =
        CASE WHEN @start_date IS NULL THEN N''
             ELSE
                 CAST(YEAR(@start_date) AS nvarchar(4)) + N'-' +
                 RIGHT(N'00' + CAST(MONTH(@start_date) AS nvarchar(2)), 2) + N'-' +
                 RIGHT(N'00' + CAST(DAY(@start_date) AS nvarchar(2)), 2)
        END;
    DECLARE @h   nvarchar(20) = ISNULL(CAST(@home_team_id AS nvarchar(20)), N'');
    DECLARE @a   nvarchar(20) = ISNULL(CAST(@away_team_id AS nvarchar(20)), N'');
    RETURN @lid + N'|' + @dt + N'|' + @h + N'|' + @a;
END
GO
"""


def render() -> str:
    return (
        HEADER
        + ALNUM_KEY
        + APPLY_JOIN_ALIASES
        + canonical_stat_body()
        + NORMALIZE_STAT_BASIC
        + NORMALIZE_FOR_JOIN
        + person_name_body()
        + TEAM_ABBREV
        + GAME_KEY
    )


def main() -> None:
    sys.stdout.write(render())


if __name__ == "__main__":
    main()
