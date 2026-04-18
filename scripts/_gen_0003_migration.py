"""One-shot generator for schema/migrations/0003_ref_alias_tables.sql.

Plan step 1.3 — creates the `ref` schema and the three alias tables
(`ref.stat_alias`, `ref.team_alias`, `ref.person_alias`), seeds
`ref.stat_alias` from `CANONICAL_STAT_BY_ALNUM` (so cutover is
value-preserving), and ALTERs `dbo.fn_canonical_stat_by_alnum` to read
from the table instead of the inline VALUES list it got in migration 0001.

Usage (from repo root):

    python3 scripts/_gen_0003_migration.py > schema/migrations/0003_ref_alias_tables.sql

The output is committed. Re-run only when the Python mapping changes AND
migration 0003 has not yet been applied anywhere; after 0003 is live, any
correction lands as a new migration on top (standard rule from
schema/migrations/README.md).
"""

from __future__ import annotations

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from cross_book_stat_normalize import CANONICAL_STAT_BY_ALNUM  # noqa: E402


def _nstr(s: str) -> str:
    return "N'" + s.replace("'", "''") + "'"


HEADER = """\
-- 0003_ref_alias_tables.sql
--
-- Plan step 1.3 — `ref` schema + alias tables; make ref.stat_alias the
-- authoritative source for dbo.fn_canonical_stat_by_alnum.
--
-- This migration:
--   1. Creates the `ref` schema.
--   2. Creates ref.stat_alias, ref.team_alias, ref.person_alias.
--   3. Seeds ref.stat_alias from CANONICAL_STAT_BY_ALNUM in
--      cross_book_stat_normalize.py (value-preserving cutover: the next
--      step ALTERs fn_canonical_stat_by_alnum to read from this table
--      and every fixture keeps producing the same output).
--   4. ALTERs dbo.fn_canonical_stat_by_alnum to read ref.stat_alias.
--
-- Design notes:
--   * ref.stat_alias is keyed (source, alias_alnum_key). 'source' is
--     the scraper/book name the alias came from; '_any' means "use this
--     mapping regardless of source", which is how every row from the
--     migration 0001 inline VALUES list is seeded (they came from a
--     cross-book canonicalization dict). Later, when we have per-book
--     alias rows, the lookup prefers an exact (source, alias) hit and
--     falls back to (_any, alias) -- see UDF body below.
--   * No FK to sportsbook_stat_type yet because that FK target doesn't
--     carry a stable canonical_stat_key column until plan step 3.2.
--     Once it does, a later migration adds a nullable
--     canonical_stat_type_id FK that tightens the coupling.
--   * ref.team_alias and ref.person_alias are created empty. They are
--     populated by scripts/seed_aliases.py (which proposes rows for
--     operator review) and by future consolidation migrations.
--     fn_normalize_team_abbrev will read ref.team_alias in a later
--     step; today that UDF still uppercase+trims only.
--
-- All comparisons in the new fn body use binary collation on the
-- control-flow branches (see the 'alias applied' branch, mirroring
-- the rule introduced in migration 0002). The per-row lookup uses
-- default (CI) collation intentionally so casing variants still match
-- the alias table.

SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
GO

"""


REF_SCHEMA = """\
-- 1. Create the `ref` schema if it doesn't exist.
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = N'ref')
BEGIN
    EXEC(N'CREATE SCHEMA [ref] AUTHORIZATION [dbo];');
END
GO

"""


STAT_ALIAS_TABLE = """\
-- 2a. ref.stat_alias: (source, alias_alnum_key) -> canonical_label
--     'source' = scraper/book name or N'_any' for cross-book mappings.
--     'alias_alnum_key' = dbo.fn_alnum_key(alias_raw), for exact match.
--     'canonical_label' = the value dbo.fn_normalize_stat_basic returns.
--
-- PK on (source, alias_alnum_key) so lookups are direct and dedupe is
-- enforced by the DB (no two rows with the same alias from the same
-- source can disagree).
IF OBJECT_ID(N'[ref].[stat_alias]', N'U') IS NULL
BEGIN
    CREATE TABLE [ref].[stat_alias](
        [source] nvarchar(40) NOT NULL,
        [alias_alnum_key] nvarchar(120) NOT NULL,
        [alias_raw] nvarchar(120) NULL,
        [canonical_label] nvarchar(120) NOT NULL,
        [sport_hint] nvarchar(40) NULL,
        [notes] nvarchar(255) NULL,
        [created_at] datetime2(7) NOT NULL
            CONSTRAINT [DF_ref_stat_alias_created_at]
            DEFAULT (CONVERT(datetime2(7), SYSUTCDATETIME()
                AT TIME ZONE 'UTC' AT TIME ZONE 'Central Standard Time')),
        [last_modified_at] datetime2(7) NOT NULL
            CONSTRAINT [DF_ref_stat_alias_last_modified_at]
            DEFAULT (CONVERT(datetime2(7), SYSUTCDATETIME()
                AT TIME ZONE 'UTC' AT TIME ZONE 'Central Standard Time')),
        CONSTRAINT [PK_ref_stat_alias]
            PRIMARY KEY CLUSTERED ([source] ASC, [alias_alnum_key] ASC)
    );
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = N'IX_ref_stat_alias_canonical_label'
      AND object_id = OBJECT_ID(N'ref.stat_alias')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_ref_stat_alias_canonical_label]
        ON [ref].[stat_alias]([canonical_label])
        INCLUDE ([source], [alias_alnum_key]);
END
GO

"""


TEAM_ALIAS_TABLE = """\
-- 2b. ref.team_alias: (canonical_league_id, source, alias_normalized) -> canonical_team_abbrev
--     alias_normalized is dbo.fn_normalize_team_abbrev(raw, canonical_league_id).
--     Starts empty; populated by scripts/seed_aliases.py (proposals file)
--     and reviewed migrations.
IF OBJECT_ID(N'[ref].[team_alias]', N'U') IS NULL
BEGIN
    CREATE TABLE [ref].[team_alias](
        [canonical_league_id] int NOT NULL,
        [source] nvarchar(40) NOT NULL,
        [alias_normalized] nvarchar(40) NOT NULL,
        [canonical_team_abbrev] nvarchar(20) NOT NULL,
        [alias_raw] nvarchar(40) NULL,
        [notes] nvarchar(255) NULL,
        [created_at] datetime2(7) NOT NULL
            CONSTRAINT [DF_ref_team_alias_created_at]
            DEFAULT (CONVERT(datetime2(7), SYSUTCDATETIME()
                AT TIME ZONE 'UTC' AT TIME ZONE 'Central Standard Time')),
        [last_modified_at] datetime2(7) NOT NULL
            CONSTRAINT [DF_ref_team_alias_last_modified_at]
            DEFAULT (CONVERT(datetime2(7), SYSUTCDATETIME()
                AT TIME ZONE 'UTC' AT TIME ZONE 'Central Standard Time')),
        CONSTRAINT [PK_ref_team_alias]
            PRIMARY KEY CLUSTERED ([canonical_league_id] ASC, [source] ASC, [alias_normalized] ASC)
    );
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = N'IX_ref_team_alias_canonical'
      AND object_id = OBJECT_ID(N'ref.team_alias')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_ref_team_alias_canonical]
        ON [ref].[team_alias]([canonical_league_id], [canonical_team_abbrev])
        INCLUDE ([source], [alias_normalized]);
END
GO

"""


PERSON_ALIAS_TABLE = """\
-- 2c. ref.person_alias: (canonical_league_id, source, alias_normalized) -> canonical_display_name
--     alias_normalized is dbo.fn_normalize_person_name(raw).
--     Starts empty; populated by scripts/seed_aliases.py on demand.
IF OBJECT_ID(N'[ref].[person_alias]', N'U') IS NULL
BEGIN
    CREATE TABLE [ref].[person_alias](
        [canonical_league_id] int NOT NULL,
        [source] nvarchar(40) NOT NULL,
        [alias_normalized] nvarchar(255) NOT NULL,
        [canonical_display_name] nvarchar(255) NOT NULL,
        [alias_raw] nvarchar(255) NULL,
        [notes] nvarchar(255) NULL,
        [created_at] datetime2(7) NOT NULL
            CONSTRAINT [DF_ref_person_alias_created_at]
            DEFAULT (CONVERT(datetime2(7), SYSUTCDATETIME()
                AT TIME ZONE 'UTC' AT TIME ZONE 'Central Standard Time')),
        [last_modified_at] datetime2(7) NOT NULL
            CONSTRAINT [DF_ref_person_alias_last_modified_at]
            DEFAULT (CONVERT(datetime2(7), SYSUTCDATETIME()
                AT TIME ZONE 'UTC' AT TIME ZONE 'Central Standard Time')),
        CONSTRAINT [PK_ref_person_alias]
            PRIMARY KEY CLUSTERED ([canonical_league_id] ASC, [source] ASC, [alias_normalized] ASC)
    );
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = N'IX_ref_person_alias_canonical'
      AND object_id = OBJECT_ID(N'ref.person_alias')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_ref_person_alias_canonical]
        ON [ref].[person_alias]([canonical_league_id], [canonical_display_name])
        INCLUDE ([source], [alias_normalized]);
END
GO

"""


def _gen_stat_alias_seed() -> str:
    """MERGE statement that seeds ref.stat_alias from CANONICAL_STAT_BY_ALNUM.

    Idempotent: re-running the migration updates `canonical_label` if the
    Python source changed (it shouldn't after 0003 ships), and inserts any
    missing rows. Uses `source='_any'` because these mappings came from a
    cross-book dict with no per-book provenance.
    """
    rows = []
    for k, v in CANONICAL_STAT_BY_ALNUM.items():
        rows.append(f"        (N'_any', {_nstr(k)}, {_nstr(v)})")
    values_block = ",\n".join(rows)
    return f"""\
-- 3. Seed ref.stat_alias from cross_book_stat_normalize.CANONICAL_STAT_BY_ALNUM
--    ({len(rows)} rows). Value-preserving: after the ALTER below,
--    dbo.fn_canonical_stat_by_alnum returns the same value for the same
--    input. Re-running the migration is a no-op on unchanged rows and
--    updates canonical_label on changed rows.
;WITH seed(source, alias_alnum_key, canonical_label) AS (
    SELECT * FROM (VALUES
{values_block}
    ) AS v(source, alias_alnum_key, canonical_label)
)
MERGE [ref].[stat_alias] AS t
USING seed AS s
    ON t.[source] = s.[source]
   AND t.[alias_alnum_key] = s.[alias_alnum_key]
WHEN MATCHED AND (
        t.[canonical_label] <> s.[canonical_label] COLLATE Latin1_General_BIN2
    )
    THEN UPDATE SET
        t.[canonical_label] = s.[canonical_label],
        t.[last_modified_at] = CONVERT(datetime2(7),
            SYSUTCDATETIME() AT TIME ZONE 'UTC' AT TIME ZONE 'Central Standard Time')
WHEN NOT MATCHED BY TARGET THEN
    INSERT ([source], [alias_alnum_key], [canonical_label])
    VALUES (s.[source], s.[alias_alnum_key], s.[canonical_label]);
GO

"""


ALTER_FN_CANONICAL = """\
-- 4. Repoint dbo.fn_canonical_stat_by_alnum at ref.stat_alias.
--
--    Why DROP + CREATE instead of plain ALTER:
--    SQL Server 2016 raises error 3729 when you ALTER a schemabound
--    function that is referenced by another schemabound function:
--        Cannot ALTER 'dbo.fn_canonical_stat_by_alnum' because it is
--        being referenced by object 'fn_normalize_stat_basic'.
--    fn_normalize_stat_basic and fn_normalize_for_join both depend on
--    fn_canonical_stat_by_alnum WITH SCHEMABINDING. We break the
--    binding temporarily, ALTER the base, then recreate the dependents
--    with bodies identical to migrations 0001/0002. The whole block
--    runs inside the migration runner's transaction, so a failure in
--    any batch rolls everything back -- including the dropped
--    dependents -- and leaves the DB in its pre-migration state.
--
--    fn_normalize_stat_basic body: exactly as created in migration 0001.
--    fn_normalize_for_join    body: exactly as altered in migration 0002
--                                   (binary-collation compare on the
--                                   alias-applied branch).

IF OBJECT_ID(N'dbo.fn_normalize_for_join', N'FN') IS NOT NULL
    DROP FUNCTION dbo.fn_normalize_for_join;
GO

IF OBJECT_ID(N'dbo.fn_normalize_stat_basic', N'FN') IS NOT NULL
    DROP FUNCTION dbo.fn_normalize_stat_basic;
GO

ALTER FUNCTION dbo.fn_canonical_stat_by_alnum(@k nvarchar(120))
RETURNS nvarchar(120)
WITH SCHEMABINDING
AS
BEGIN
    IF @k IS NULL OR LEN(@k) = 0 RETURN NULL;
    DECLARE @v nvarchar(120);
    SELECT TOP (1) @v = sa.[canonical_label]
    FROM [ref].[stat_alias] AS sa
    WHERE sa.[alias_alnum_key] = @k
    ORDER BY CASE WHEN sa.[source] = N'_any' THEN 1 ELSE 0 END, sa.[source];
    RETURN @v;
END
GO

-- Recreate fn_normalize_stat_basic (body copied verbatim from 0001).
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

-- Recreate fn_normalize_for_join (body copied verbatim from 0002, which
-- forces binary collation on the 'alias applied' comparison).
CREATE FUNCTION dbo.fn_normalize_for_join(@s nvarchar(120))
RETURNS nvarchar(120)
WITH SCHEMABINDING
AS
BEGIN
    IF @s IS NULL RETURN N'';
    DECLARE @trimmed nvarchar(120) = LTRIM(RTRIM(@s));
    IF LEN(@trimmed) = 0 RETURN N'';
    DECLARE @pre nvarchar(120) = dbo.fn_apply_join_aliases(@trimmed);
    IF @pre <> @trimmed COLLATE Latin1_General_BIN2 RETURN @pre;
    DECLARE @mapped nvarchar(120) = dbo.fn_canonical_stat_by_alnum(dbo.fn_alnum_key(@trimmed));
    IF @mapped IS NULL RETURN @trimmed;
    RETURN dbo.fn_apply_join_aliases(@mapped);
END
GO
"""


def render() -> str:
    return (
        HEADER
        + REF_SCHEMA
        + STAT_ALIAS_TABLE
        + TEAM_ALIAS_TABLE
        + PERSON_ALIAS_TABLE
        + _gen_stat_alias_seed()
        + ALTER_FN_CANONICAL
    )


def main() -> None:
    sys.stdout.write(render())


if __name__ == "__main__":
    main()
