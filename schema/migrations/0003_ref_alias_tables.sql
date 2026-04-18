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

-- 1. Create the `ref` schema if it doesn't exist.
IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = N'ref')
BEGIN
    EXEC(N'CREATE SCHEMA [ref] AUTHORIZATION [dbo];');
END
GO

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

-- 3. Seed ref.stat_alias from cross_book_stat_normalize.CANONICAL_STAT_BY_ALNUM
--    (165 rows). Value-preserving: after the ALTER below,
--    dbo.fn_canonical_stat_by_alnum returns the same value for the same
--    input. Re-running the migration is a no-op on unchanged rows and
--    updates canonical_label on changed rows.
;WITH seed(source, alias_alnum_key, canonical_label) AS (
    SELECT * FROM (VALUES
        (N'_any', N'pts', N'Points'),
        (N'_any', N'points', N'Points'),
        (N'_any', N'point', N'Points'),
        (N'_any', N'reb', N'Rebounds'),
        (N'_any', N'rebounds', N'Rebounds'),
        (N'_any', N'ast', N'Assists'),
        (N'_any', N'assists', N'Assists'),
        (N'_any', N'stl', N'Steals'),
        (N'_any', N'steals', N'Steals'),
        (N'_any', N'blk', N'Blocks'),
        (N'_any', N'blocks', N'Blocks'),
        (N'_any', N'blockedshots', N'Blocked Shots'),
        (N'_any', N'tov', N'Turnovers'),
        (N'_any', N'to', N'Turnovers'),
        (N'_any', N'turnovers', N'Turnovers'),
        (N'_any', N'turnover', N'Turnovers'),
        (N'_any', N'blksstls', N'Blks+Stls'),
        (N'_any', N'blockssteals', N'Blks+Stls'),
        (N'_any', N'stocks', N'Blks+Stls'),
        (N'_any', N'fantasypoints', N'Fantasy Score'),
        (N'_any', N'fantasyscore', N'Fantasy Score'),
        (N'_any', N'ptsreb', N'Pts+Rebs'),
        (N'_any', N'ptsrebs', N'Pts+Rebs'),
        (N'_any', N'ptsast', N'Pts+Asts'),
        (N'_any', N'ptsasts', N'Pts+Asts'),
        (N'_any', N'rebast', N'Rebs+Asts'),
        (N'_any', N'rebsasts', N'Rebs+Asts'),
        (N'_any', N'ptsrebast', N'Pts+Rebs+Asts'),
        (N'_any', N'ptsrebsasts', N'Pts+Rebs+Asts'),
        (N'_any', N'ptsrebsast', N'Pts+Rebs+Asts'),
        (N'_any', N'pointsreboundsassists', N'Pts+Rebs+Asts'),
        (N'_any', N'pointsrebounds', N'Pts+Rebs'),
        (N'_any', N'pointsassists', N'Pts+Asts'),
        (N'_any', N'reboundsassists', N'Rebs+Asts'),
        (N'_any', N'doubledoubles', N'Double-Double'),
        (N'_any', N'doubledouble', N'Double-Double'),
        (N'_any', N'tripledoubles', N'Triple-Double'),
        (N'_any', N'oreb', N'Offensive Rebounds'),
        (N'_any', N'dreb', N'Defensive Rebounds'),
        (N'_any', N'3pm', N'3-PT Made'),
        (N'_any', N'3ptm', N'3-PT Made'),
        (N'_any', N'3ptmade', N'3-PT Made'),
        (N'_any', N'3pmade', N'3-PT Made'),
        (N'_any', N'3pointersmade', N'3 Pointers Made'),
        (N'_any', N'threepointsmade', N'3 Pointers Made'),
        (N'_any', N'threes', N'3 Pointers Made'),
        (N'_any', N'3pt', N'3 Pointers'),
        (N'_any', N'3sattempted', N'3-PT Attempted'),
        (N'_any', N'threepointsatt', N'3-PT Attempted'),
        (N'_any', N'threepointersattempted', N'3 Pointers Attempted'),
        (N'_any', N'attemptedthrees', N'3-PT Attempted'),
        (N'_any', N'fgattempted', N'FG Attempted'),
        (N'_any', N'fieldgoalsatt', N'FG Attempted'),
        (N'_any', N'fgmade', N'FG Made'),
        (N'_any', N'ftmade', N'Free Throws Made'),
        (N'_any', N'freethrowsmade', N'Free Throws Made'),
        (N'_any', N'freethrowsattempted', N'Free Throws Attempted'),
        (N'_any', N'twopointersmade', N'Two Pointers Made'),
        (N'_any', N'twopointersattempted', N'Two Pointers Attempted'),
        (N'_any', N'bbpoints', N'Points'),
        (N'_any', N'bbrebounds', N'Rebounds'),
        (N'_any', N'bbassists', N'Assists'),
        (N'_any', N'bbsteals', N'Steals'),
        (N'_any', N'bbblocks', N'Blocks'),
        (N'_any', N'bbturnovers', N'Turnovers'),
        (N'_any', N'bbpersonal', N'Personal Fouls'),
        (N'_any', N'bbdreb', N'Defensive Rebounds'),
        (N'_any', N'bboreb', N'Offensive Rebounds'),
        (N'_any', N'bbfgmade', N'FG Made'),
        (N'_any', N'bbfgattempted', N'FG Attempted'),
        (N'_any', N'bbtwopointersmade', N'Two Pointers Made'),
        (N'_any', N'bbtwopointersattempted', N'Two Pointers Attempted'),
        (N'_any', N'bbfreethrowsmade', N'Free Throws Made'),
        (N'_any', N'bbfreethrowsattempted', N'Free Throws Attempted'),
        (N'_any', N'bbptsreb', N'Pts+Rebs'),
        (N'_any', N'bbptsast', N'Pts+Asts'),
        (N'_any', N'bbptsrebast', N'Pts+Rebs+Asts'),
        (N'_any', N'bbrebast', N'Rebs+Asts'),
        (N'_any', N'bbdd', N'Double-Double'),
        (N'_any', N'bbtd', N'Triple-Double'),
        (N'_any', N'bbparlaypoints', N'Fantasy Score'),
        (N'_any', N'bbfirstbasket', N'First Point Scorer'),
        (N'_any', N'bbthreepointersmade', N'3 Pointers Made'),
        (N'_any', N'bbthreepointersattempted', N'3-PT Attempted'),
        (N'_any', N'bbthreepointfieldgoalsattempted', N'3-PT Attempted'),
        (N'_any', N'bbfg3a', N'3-PT Attempted'),
        (N'_any', N'bb3ptattempted', N'3-PT Attempted'),
        (N'_any', N'ptsrebsasts1h', N'1H Pts + Rebs + Asts'),
        (N'_any', N'period1points', N'1Q Points'),
        (N'_any', N'period12points', N'1H Points'),
        (N'_any', N'period1rebounds', N'1Q Rebounds'),
        (N'_any', N'period12rebounds', N'1H Rebounds'),
        (N'_any', N'period1assists', N'1Q Assists'),
        (N'_any', N'period12assists', N'1H Assists'),
        (N'_any', N'period1threepointsmade', N'1Q 3-Pointers Made'),
        (N'_any', N'period12threepointsmade', N'1H 3-Pointers Made'),
        (N'_any', N'period1ptsrebsasts', N'1Q Pts + Rebs + Asts'),
        (N'_any', N'period12ptsrebsasts', N'1H Pts + Rebs + Asts'),
        (N'_any', N'babhrr', N'Hits+Runs+RBIs'),
        (N'_any', N'hitsrunsrbis', N'Hits+Runs+RBIs'),
        (N'_any', N'hrr', N'Hits+Runs+RBIs'),
        (N'_any', N'babtotalbases', N'Total Bases'),
        (N'_any', N'babhits', N'Hits'),
        (N'_any', N'babrbi', N'RBIs'),
        (N'_any', N'babruns', N'Runs'),
        (N'_any', N'babsingles', N'Singles'),
        (N'_any', N'babdoubles', N'Doubles'),
        (N'_any', N'babtriples', N'Triples'),
        (N'_any', N'babhomeruns', N'Home Runs'),
        (N'_any', N'babstolenbases', N'Stolen Bases'),
        (N'_any', N'babwalks', N'Walks'),
        (N'_any', N'babpitchingstrikeouts', N'Pitcher Strikeouts'),
        (N'_any', N'babstrikeouts', N'Hitter Strikeouts'),
        (N'_any', N'babpitchingouts', N'Pitching Outs'),
        (N'_any', N'babpitchesthrown', N'Pitches Thrown'),
        (N'_any', N'babhitsallowed', N'Hits Allowed'),
        (N'_any', N'babwalksallowed', N'Walks Allowed'),
        (N'_any', N'babparlaypoints', N'Hitter Fantasy Score'),
        (N'_any', N'earnedrunsallowed', N'Earned Runs Allowed'),
        (N'_any', N'firstinningrunsallowed', N'1st Inning Runs Allowed'),
        (N'_any', N'socshots', N'Shots'),
        (N'_any', N'socshotsontarget', N'Shots On Target'),
        (N'_any', N'socshotsongoal', N'Shots On Target'),
        (N'_any', N'soctackles', N'Tackles'),
        (N'_any', N'socgoals', N'Goals'),
        (N'_any', N'socassists', N'Assists'),
        (N'_any', N'socfouls', N'Fouls'),
        (N'_any', N'socfouled', N'Fouled'),
        (N'_any', N'soccards', N'Cards'),
        (N'_any', N'socgoalassist', N'Goal + Assist'),
        (N'_any', N'socdribblesattempted', N'Attempted Dribbles'),
        (N'_any', N'soccrosses', N'Crosses'),
        (N'_any', N'socgksaves', N'Goalie Saves'),
        (N'_any', N'socoffsides', N'Offsides'),
        (N'_any', N'passyards', N'Pass Yards'),
        (N'_any', N'passingyards', N'Pass Yards'),
        (N'_any', N'passtds', N'Pass TDs'),
        (N'_any', N'passingtds', N'Pass TDs'),
        (N'_any', N'rushyards', N'Rush Yards'),
        (N'_any', N'rushingyards', N'Rush Yards'),
        (N'_any', N'rushtds', N'Rush TDs'),
        (N'_any', N'rushingtds', N'Rush TDs'),
        (N'_any', N'recyards', N'Receiving Yards'),
        (N'_any', N'receivingyards', N'Receiving Yards'),
        (N'_any', N'rectds', N'Rec TDs'),
        (N'_any', N'receivingtds', N'Rec TDs'),
        (N'_any', N'rushtrectds', N'Rush + Rec TDs'),
        (N'_any', N'rushrectds', N'Rush + Rec TDs'),
        (N'_any', N'significantstrikes', N'Significant Strikes'),
        (N'_any', N'takedowns', N'Takedowns'),
        (N'_any', N'tackledowns', N'Takedowns'),
        (N'_any', N'totalrounds', N'Total Rounds'),
        (N'_any', N'fighttimemins', N'Fight Time (Mins)'),
        (N'_any', N'tentotalgames', N'Total Games'),
        (N'_any', N'tengameswon', N'Total Games Won'),
        (N'_any', N'tensetswon', N'Total Sets'),
        (N'_any', N'tenaces', N'Aces'),
        (N'_any', N'escsgokills', N'MAPS 1-2 Kills'),
        (N'_any', N'escsgoheadshots', N'MAPS 1-2 Headshots'),
        (N'_any', N'eslolkills', N'MAPS 1-2 Kills'),
        (N'_any', N'esvalkills', N'MAPS 1-2 Kills'),
        (N'_any', N'esdota2kills', N'MAPS 1-3 Kills'),
        (N'_any', N'crickruns', N'Runs'),
        (N'_any', N'crickfours', N'Fours'),
        (N'_any', N'cricksixes', N'Sixes')
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
