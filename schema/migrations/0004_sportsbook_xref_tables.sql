-- 0004_sportsbook_xref_tables.sql
--
-- Plan step 1.4 — cross-dimension xref tables.
--
-- Creates five new (book, external_id) -> canonical_id mapping tables and
-- adds a nullable `external_id_kind` column to the existing
-- sportsbook_player_xref (backfilled from the prefix in
-- external_player_id, without disturbing the existing column).
--
-- Dimensions covered:
--   sportsbook_team_xref        -> dbo.sportsbook_team.sportsbook_team_id
--   sportsbook_stat_type_xref   -> dbo.sportsbook_stat_type.sportsbook_stat_type_id
--   sportsbook_game_xref        -> dbo.sportsbook_game.sportsbook_game_id
--   sportsbook_league_xref      -> dbo.sportsbook_league.sportsbook_league_id
--   sportsbook_sport_xref       -> dbo.sportsbook_sport.sportsbook_sport_id
--
-- Why additive-only (no changes to existing player xref PK / columns):
--   sportsbook_dimension_sync.py still writes and reads the prefixed
--   external_player_id ('player_id:123', 'ppid:abc') from migration-0001
--   times. Dropping or reshaping that column would break the live sync
--   today. Instead, add external_id_kind alongside and backfill it from
--   the existing prefix. Plan step 1.7 refactors the sync to xref-first
--   and at that point we can decide whether to split the column for
--   real.
--
-- Shape note:
--   Every new xref has PK (sportsbook, external_id_kind, external_id).
--   Keeping external_id_kind in the PK lets one book register multiple
--   id namespaces per canonical row without collision -- e.g. Underdog
--   'solo_game_id' and 'game_id' for the same sportsbook_game, or
--   PrizePicks 'player_id' and 'ppid' for the same sportsbook_player.
--
-- FK note:
--   The canonical target tables are created by the existing
--   schema/sportsbook_{team,stat_type,game,league,sport}.sql DDL that
--   the dimension sync already installs. Every FK here is guarded by
--   an IF OBJECT_ID(...) check so this migration stays safe on
--   partially-installed databases (e.g. a brand-new dev box that ran
--   --mark-applied 0000 and hasn't populated every sportsbook_* table
--   yet). No behavior change if the parent table is missing; the xref
--   table is simply created without the FK and can be patched later.
--
-- Collation / case-fold:
--   external_id values are always stored exactly as the source book
--   emits them. Lookups in sportsbook_dimension_sync.py and in the
--   future xref-first resolver compare with the default collation,
--   which is what we want: sportsbook ids are ASCII-numeric or
--   opaque tokens where case collisions are already prevented by
--   the book.

SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
GO

-- =====================================================================
-- 1. sportsbook_team_xref
-- =====================================================================
IF OBJECT_ID(N'[dbo].[sportsbook_team_xref]', N'U') IS NULL
BEGIN
    CREATE TABLE [dbo].[sportsbook_team_xref](
        [sportsbook]          nvarchar(30) NOT NULL,
        [external_id_kind]    nvarchar(30) NOT NULL
            CONSTRAINT [DF_sportsbook_team_xref_kind] DEFAULT (N'id'),
        [external_team_id]    nvarchar(64) NOT NULL,
        [sportsbook_team_id]  bigint NOT NULL,
        [created_at] datetime2(7) NOT NULL
            CONSTRAINT [DF_sportsbook_team_xref_created_at]
            DEFAULT (CONVERT(datetime2(7), SYSUTCDATETIME()
                AT TIME ZONE 'UTC' AT TIME ZONE 'Central Standard Time')),
        [last_modified_at] datetime2(7) NOT NULL
            CONSTRAINT [DF_sportsbook_team_xref_last_modified_at]
            DEFAULT (CONVERT(datetime2(7), SYSUTCDATETIME()
                AT TIME ZONE 'UTC' AT TIME ZONE 'Central Standard Time')),
        CONSTRAINT [PK_sportsbook_team_xref]
            PRIMARY KEY CLUSTERED ([sportsbook] ASC, [external_id_kind] ASC, [external_team_id] ASC)
    );
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = N'IX_sportsbook_team_xref_canonical'
      AND object_id = OBJECT_ID(N'dbo.sportsbook_team_xref')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_sportsbook_team_xref_canonical]
        ON [dbo].[sportsbook_team_xref]([sportsbook_team_id])
        INCLUDE ([sportsbook], [external_id_kind], [external_team_id]);
END
GO

IF OBJECT_ID(N'[dbo].[sportsbook_team]', N'U') IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = N'FK_sportsbook_team_xref_team')
BEGIN
    ALTER TABLE [dbo].[sportsbook_team_xref] WITH NOCHECK
        ADD CONSTRAINT [FK_sportsbook_team_xref_team]
        FOREIGN KEY ([sportsbook_team_id])
        REFERENCES [dbo].[sportsbook_team]([sportsbook_team_id]);
END
GO

-- =====================================================================
-- 2. sportsbook_stat_type_xref
-- =====================================================================
IF OBJECT_ID(N'[dbo].[sportsbook_stat_type_xref]', N'U') IS NULL
BEGIN
    CREATE TABLE [dbo].[sportsbook_stat_type_xref](
        [sportsbook]               nvarchar(30) NOT NULL,
        [external_id_kind]         nvarchar(30) NOT NULL
            CONSTRAINT [DF_sportsbook_stat_type_xref_kind] DEFAULT (N'id'),
        [external_stat_type_id]    nvarchar(120) NOT NULL,
        [sportsbook_stat_type_id]  bigint NOT NULL,
        [created_at] datetime2(7) NOT NULL
            CONSTRAINT [DF_sportsbook_stat_type_xref_created_at]
            DEFAULT (CONVERT(datetime2(7), SYSUTCDATETIME()
                AT TIME ZONE 'UTC' AT TIME ZONE 'Central Standard Time')),
        [last_modified_at] datetime2(7) NOT NULL
            CONSTRAINT [DF_sportsbook_stat_type_xref_last_modified_at]
            DEFAULT (CONVERT(datetime2(7), SYSUTCDATETIME()
                AT TIME ZONE 'UTC' AT TIME ZONE 'Central Standard Time')),
        CONSTRAINT [PK_sportsbook_stat_type_xref]
            PRIMARY KEY CLUSTERED ([sportsbook] ASC, [external_id_kind] ASC, [external_stat_type_id] ASC)
    );
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = N'IX_sportsbook_stat_type_xref_canonical'
      AND object_id = OBJECT_ID(N'dbo.sportsbook_stat_type_xref')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_sportsbook_stat_type_xref_canonical]
        ON [dbo].[sportsbook_stat_type_xref]([sportsbook_stat_type_id])
        INCLUDE ([sportsbook], [external_id_kind], [external_stat_type_id]);
END
GO

IF OBJECT_ID(N'[dbo].[sportsbook_stat_type]', N'U') IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = N'FK_sportsbook_stat_type_xref_stat_type')
BEGIN
    ALTER TABLE [dbo].[sportsbook_stat_type_xref] WITH NOCHECK
        ADD CONSTRAINT [FK_sportsbook_stat_type_xref_stat_type]
        FOREIGN KEY ([sportsbook_stat_type_id])
        REFERENCES [dbo].[sportsbook_stat_type]([sportsbook_stat_type_id]);
END
GO

-- =====================================================================
-- 3. sportsbook_game_xref
-- =====================================================================
IF OBJECT_ID(N'[dbo].[sportsbook_game_xref]', N'U') IS NULL
BEGIN
    CREATE TABLE [dbo].[sportsbook_game_xref](
        [sportsbook]          nvarchar(30) NOT NULL,
        [external_id_kind]    nvarchar(30) NOT NULL
            CONSTRAINT [DF_sportsbook_game_xref_kind] DEFAULT (N'id'),
        [external_game_id]    nvarchar(64) NOT NULL,
        [sportsbook_game_id]  bigint NOT NULL,
        [created_at] datetime2(7) NOT NULL
            CONSTRAINT [DF_sportsbook_game_xref_created_at]
            DEFAULT (CONVERT(datetime2(7), SYSUTCDATETIME()
                AT TIME ZONE 'UTC' AT TIME ZONE 'Central Standard Time')),
        [last_modified_at] datetime2(7) NOT NULL
            CONSTRAINT [DF_sportsbook_game_xref_last_modified_at]
            DEFAULT (CONVERT(datetime2(7), SYSUTCDATETIME()
                AT TIME ZONE 'UTC' AT TIME ZONE 'Central Standard Time')),
        CONSTRAINT [PK_sportsbook_game_xref]
            PRIMARY KEY CLUSTERED ([sportsbook] ASC, [external_id_kind] ASC, [external_game_id] ASC)
    );
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = N'IX_sportsbook_game_xref_canonical'
      AND object_id = OBJECT_ID(N'dbo.sportsbook_game_xref')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_sportsbook_game_xref_canonical]
        ON [dbo].[sportsbook_game_xref]([sportsbook_game_id])
        INCLUDE ([sportsbook], [external_id_kind], [external_game_id]);
END
GO

IF OBJECT_ID(N'[dbo].[sportsbook_game]', N'U') IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = N'FK_sportsbook_game_xref_game')
BEGIN
    ALTER TABLE [dbo].[sportsbook_game_xref] WITH NOCHECK
        ADD CONSTRAINT [FK_sportsbook_game_xref_game]
        FOREIGN KEY ([sportsbook_game_id])
        REFERENCES [dbo].[sportsbook_game]([sportsbook_game_id]);
END
GO

-- =====================================================================
-- 4. sportsbook_league_xref
-- =====================================================================
IF OBJECT_ID(N'[dbo].[sportsbook_league_xref]', N'U') IS NULL
BEGIN
    CREATE TABLE [dbo].[sportsbook_league_xref](
        [sportsbook]            nvarchar(30) NOT NULL,
        [external_id_kind]      nvarchar(30) NOT NULL
            CONSTRAINT [DF_sportsbook_league_xref_kind] DEFAULT (N'id'),
        [external_league_id]    nvarchar(64) NOT NULL,
        [sportsbook_league_id]  bigint NOT NULL,
        [created_at] datetime2(7) NOT NULL
            CONSTRAINT [DF_sportsbook_league_xref_created_at]
            DEFAULT (CONVERT(datetime2(7), SYSUTCDATETIME()
                AT TIME ZONE 'UTC' AT TIME ZONE 'Central Standard Time')),
        [last_modified_at] datetime2(7) NOT NULL
            CONSTRAINT [DF_sportsbook_league_xref_last_modified_at]
            DEFAULT (CONVERT(datetime2(7), SYSUTCDATETIME()
                AT TIME ZONE 'UTC' AT TIME ZONE 'Central Standard Time')),
        CONSTRAINT [PK_sportsbook_league_xref]
            PRIMARY KEY CLUSTERED ([sportsbook] ASC, [external_id_kind] ASC, [external_league_id] ASC)
    );
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = N'IX_sportsbook_league_xref_canonical'
      AND object_id = OBJECT_ID(N'dbo.sportsbook_league_xref')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_sportsbook_league_xref_canonical]
        ON [dbo].[sportsbook_league_xref]([sportsbook_league_id])
        INCLUDE ([sportsbook], [external_id_kind], [external_league_id]);
END
GO

IF OBJECT_ID(N'[dbo].[sportsbook_league]', N'U') IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = N'FK_sportsbook_league_xref_league')
BEGIN
    ALTER TABLE [dbo].[sportsbook_league_xref] WITH NOCHECK
        ADD CONSTRAINT [FK_sportsbook_league_xref_league]
        FOREIGN KEY ([sportsbook_league_id])
        REFERENCES [dbo].[sportsbook_league]([sportsbook_league_id]);
END
GO

-- =====================================================================
-- 5. sportsbook_sport_xref
-- =====================================================================
IF OBJECT_ID(N'[dbo].[sportsbook_sport_xref]', N'U') IS NULL
BEGIN
    CREATE TABLE [dbo].[sportsbook_sport_xref](
        [sportsbook]           nvarchar(30) NOT NULL,
        [external_id_kind]     nvarchar(30) NOT NULL
            CONSTRAINT [DF_sportsbook_sport_xref_kind] DEFAULT (N'id'),
        [external_sport_id]    nvarchar(64) NOT NULL,
        [sportsbook_sport_id]  bigint NOT NULL,
        [created_at] datetime2(7) NOT NULL
            CONSTRAINT [DF_sportsbook_sport_xref_created_at]
            DEFAULT (CONVERT(datetime2(7), SYSUTCDATETIME()
                AT TIME ZONE 'UTC' AT TIME ZONE 'Central Standard Time')),
        [last_modified_at] datetime2(7) NOT NULL
            CONSTRAINT [DF_sportsbook_sport_xref_last_modified_at]
            DEFAULT (CONVERT(datetime2(7), SYSUTCDATETIME()
                AT TIME ZONE 'UTC' AT TIME ZONE 'Central Standard Time')),
        CONSTRAINT [PK_sportsbook_sport_xref]
            PRIMARY KEY CLUSTERED ([sportsbook] ASC, [external_id_kind] ASC, [external_sport_id] ASC)
    );
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = N'IX_sportsbook_sport_xref_canonical'
      AND object_id = OBJECT_ID(N'dbo.sportsbook_sport_xref')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_sportsbook_sport_xref_canonical]
        ON [dbo].[sportsbook_sport_xref]([sportsbook_sport_id])
        INCLUDE ([sportsbook], [external_id_kind], [external_sport_id]);
END
GO

IF OBJECT_ID(N'[dbo].[sportsbook_sport]', N'U') IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = N'FK_sportsbook_sport_xref_sport')
BEGIN
    ALTER TABLE [dbo].[sportsbook_sport_xref] WITH NOCHECK
        ADD CONSTRAINT [FK_sportsbook_sport_xref_sport]
        FOREIGN KEY ([sportsbook_sport_id])
        REFERENCES [dbo].[sportsbook_sport]([sportsbook_sport_id]);
END
GO

-- =====================================================================
-- 6. Add external_id_kind to sportsbook_player_xref (additive only).
--
--    The existing PK (sportsbook, external_player_id) stays untouched so
--    sportsbook_dimension_sync.py keeps working against its current
--    prefixed-string convention. The new column is backfilled from the
--    prefix in external_player_id:
--
--        'player_id:12345' -> external_id_kind = 'player_id', value = '12345'
--        'ppid:abc'        -> external_id_kind = 'ppid',      value = 'abc'
--        'unprefixed'      -> external_id_kind = 'id',        value = 'unprefixed'
--
--    external_player_id remains the source of truth until step 1.7.
-- =====================================================================
IF OBJECT_ID(N'[dbo].[sportsbook_player_xref]', N'U') IS NOT NULL
BEGIN
    IF COL_LENGTH(N'dbo.sportsbook_player_xref', N'external_id_kind') IS NULL
    BEGIN
        ALTER TABLE [dbo].[sportsbook_player_xref]
            ADD [external_id_kind] nvarchar(30) NULL;
    END

    IF COL_LENGTH(N'dbo.sportsbook_player_xref', N'external_id_value') IS NULL
    BEGIN
        ALTER TABLE [dbo].[sportsbook_player_xref]
            ADD [external_id_value] nvarchar(64) NULL;
    END
END
GO

-- Backfill: split external_player_id on the first ':' when the prefix
-- is one of the recognized kinds. Leave the column untouched; only the
-- new kind/value columns get written. Safe to re-run (only rows with
-- NULL external_id_kind are updated).
IF OBJECT_ID(N'[dbo].[sportsbook_player_xref]', N'U') IS NOT NULL
   AND COL_LENGTH(N'dbo.sportsbook_player_xref', N'external_id_kind') IS NOT NULL
   AND COL_LENGTH(N'dbo.sportsbook_player_xref', N'external_id_value') IS NOT NULL
BEGIN
    UPDATE [dbo].[sportsbook_player_xref]
    SET
        external_id_kind = CASE
            WHEN external_player_id LIKE N'player_id:%' THEN N'player_id'
            WHEN external_player_id LIKE N'ppid:%'      THEN N'ppid'
            WHEN external_player_id LIKE N'pickem_id:%' THEN N'pickem_id'
            WHEN external_player_id LIKE N'name_team:%' THEN N'name_team'
            WHEN CHARINDEX(N':', external_player_id) > 0 THEN
                SUBSTRING(external_player_id, 1, CHARINDEX(N':', external_player_id) - 1)
            ELSE N'id'
        END,
        external_id_value = CASE
            WHEN CHARINDEX(N':', external_player_id) > 0 THEN
                SUBSTRING(external_player_id,
                          CHARINDEX(N':', external_player_id) + 1,
                          LEN(external_player_id))
            ELSE external_player_id
        END
    WHERE external_id_kind IS NULL;
END
GO

-- Secondary index for xref-first lookups in step 1.7 (sync refactor)
-- without disturbing the existing PK.
IF OBJECT_ID(N'[dbo].[sportsbook_player_xref]', N'U') IS NOT NULL
   AND COL_LENGTH(N'dbo.sportsbook_player_xref', N'external_id_kind')  IS NOT NULL
   AND COL_LENGTH(N'dbo.sportsbook_player_xref', N'external_id_value') IS NOT NULL
   AND NOT EXISTS (
        SELECT 1 FROM sys.indexes
        WHERE name = N'IX_sportsbook_player_xref_kind_value'
          AND object_id = OBJECT_ID(N'dbo.sportsbook_player_xref')
   )
BEGIN
    CREATE NONCLUSTERED INDEX [IX_sportsbook_player_xref_kind_value]
        ON [dbo].[sportsbook_player_xref]([sportsbook] ASC, [external_id_kind] ASC, [external_id_value] ASC)
        INCLUDE ([sportsbook_player_id])
        WHERE [external_id_kind] IS NOT NULL
          AND [external_id_value] IS NOT NULL;
END
GO
