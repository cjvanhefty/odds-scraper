-- 0006_natural_key_computed_columns.sql
--
-- Plan step 1.5 — the "duplicates can't come back" guardrail.
--
-- Adds PERSISTED computed columns and filtered UNIQUE indexes on the
-- natural keys of three sportsbook_* dimensions:
--
--     sportsbook_team     (canonical_league_id, normalized abbrev)
--     sportsbook_player   (canonical_league_id, normalized name, normalized team)
--     sportsbook_game     (game natural key built from league + teams + date)
--     sportsbook_stat_type (canonical_league_id, normalized_stat_key)
--
-- sportsbook_stat_type already stores normalized_stat_key as a plain
-- NVARCHAR column populated by sportsbook_dimension_sync.py, so it only
-- needs a filtered unique index, not a computed column.
--
-- sportsbook_sport is intentionally skipped (four-ish rows, already deduped by
-- the per-book unique indexes added in schema/sportsbook_sport.sql).
--
-- sportsbook_league is intentionally skipped (already has
-- UQ_sportsbook_league_canonical_league_id).
--
-- Why PERSISTED computed columns instead of a regular NVARCHAR column
-- populated by the sync:
--
--   * PERSISTED + SCHEMABINDING + deterministic UDF is enough for SQL
--     Server to allow a unique index on the expression. The "duplicates
--     can't come back" guarantee is then enforced by the storage engine
--     on every INSERT/UPDATE -- not by a sync script that might skip a
--     row or be bypassed by a direct INSERT (as happened with
--     ref.person_alias two migrations ago).
--   * The UDFs from migration 0001 are all WITH SCHEMABINDING and
--     deterministic, so this works without any UDF changes.
--
-- Safety:
--
--   * Every computed column is ADDed only if it is missing (COL_LENGTH check).
--   * Every unique index is created only if it does not already exist.
--   * Before attempting to create the unique index, we look for
--     duplicate groups under the new natural key and, if any exist,
--     RAISERROR with the count so the transaction rolls back with a
--     clear message. This avoids a confusing 2601 "cannot insert
--     duplicate key" error on index creation.
--
--   * sportsbook_dimension_sync.py does not currently write these
--     columns (nothing writes to them; they are computed), so the
--     existing sync keeps working with zero code changes. Step 1.7
--     refactors the sync to xref-first and will start reading these
--     columns as the dedup key.

SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
GO

-- =====================================================================
-- 1. sportsbook_team
-- =====================================================================
-- Natural key: (canonical_league_id, normalized abbreviation).
-- Adds abbrev_normalized AS dbo.fn_normalize_team_abbrev(abbreviation, canonical_league_id) PERSISTED.
-- Filtered unique index on (canonical_league_id, abbrev_normalized) WHERE both NOT NULL.

IF OBJECT_ID(N'[dbo].[sportsbook_team]', N'U') IS NOT NULL
   AND COL_LENGTH(N'dbo.sportsbook_team', N'abbrev_normalized') IS NULL
BEGIN
    ALTER TABLE [dbo].[sportsbook_team]
        ADD [abbrev_normalized] AS
            CAST(dbo.fn_normalize_team_abbrev(abbreviation, canonical_league_id) AS nvarchar(20))
            PERSISTED;
END
GO

-- Pre-flight dedup check. If the natural key has dupes today, surface
-- the count + a representative row set, then abort so the runner rolls
-- back. Operator can then clean the data and rerun the migration.
IF OBJECT_ID(N'[dbo].[sportsbook_team]', N'U') IS NOT NULL
   AND COL_LENGTH(N'dbo.sportsbook_team', N'abbrev_normalized') IS NOT NULL
   AND NOT EXISTS (
        SELECT 1 FROM sys.indexes
        WHERE name = N'UQ_sportsbook_team_league_abbrev_norm'
          AND object_id = OBJECT_ID(N'dbo.sportsbook_team')
   )
BEGIN
    DECLARE @team_dupe_count int;
    SELECT @team_dupe_count = COUNT(*) FROM (
        SELECT canonical_league_id, abbrev_normalized
        FROM [dbo].[sportsbook_team]
        WHERE canonical_league_id IS NOT NULL
          AND abbrev_normalized IS NOT NULL
          AND LEN(abbrev_normalized) > 0
        GROUP BY canonical_league_id, abbrev_normalized
        HAVING COUNT(*) > 1
    ) g;
    IF @team_dupe_count > 0
    BEGIN
        DECLARE @team_msg nvarchar(400) =
            N'sportsbook_team has ' + CAST(@team_dupe_count AS nvarchar(20)) +
            N' duplicate (canonical_league_id, abbrev_normalized) group(s). ' +
            N'Resolve with sportsbook_dimension_sync._consolidate_sportsbook_dimension_dupes '
            + N'or ref.team_alias rows, then rerun migration 0006.';
        RAISERROR(@team_msg, 16, 1);
    END;
END
GO

IF OBJECT_ID(N'[dbo].[sportsbook_team]', N'U') IS NOT NULL
   AND COL_LENGTH(N'dbo.sportsbook_team', N'abbrev_normalized') IS NOT NULL
   AND NOT EXISTS (
        SELECT 1 FROM sys.indexes
        WHERE name = N'UQ_sportsbook_team_league_abbrev_norm'
          AND object_id = OBJECT_ID(N'dbo.sportsbook_team')
   )
BEGIN
    CREATE UNIQUE NONCLUSTERED INDEX [UQ_sportsbook_team_league_abbrev_norm]
        ON [dbo].[sportsbook_team]([canonical_league_id], [abbrev_normalized])
        WHERE [canonical_league_id] IS NOT NULL
          AND [abbrev_normalized] IS NOT NULL;
END
GO

-- =====================================================================
-- 2. sportsbook_player
-- =====================================================================
-- Natural key: (canonical_league_id, normalized display_name, normalized team_abbrev).
-- Matches the dedup key used by scripts/seed_aliases.py (_propose_person_aliases).
-- team_abbrev_normalized uses the same UDF -- uppercase+trim only today;
-- ref.team_alias layers in via a later ALTER if needed.

IF OBJECT_ID(N'[dbo].[sportsbook_player]', N'U') IS NOT NULL
   AND COL_LENGTH(N'dbo.sportsbook_player', N'display_name_normalized') IS NULL
BEGIN
    ALTER TABLE [dbo].[sportsbook_player]
        ADD [display_name_normalized] AS
            CAST(dbo.fn_normalize_person_name(display_name) AS nvarchar(255))
            PERSISTED;
END
GO

IF OBJECT_ID(N'[dbo].[sportsbook_player]', N'U') IS NOT NULL
   AND COL_LENGTH(N'dbo.sportsbook_player', N'team_abbrev_normalized') IS NULL
BEGIN
    ALTER TABLE [dbo].[sportsbook_player]
        ADD [team_abbrev_normalized] AS
            CAST(dbo.fn_normalize_team_abbrev(team_abbrev, canonical_league_id) AS nvarchar(20))
            PERSISTED;
END
GO

IF OBJECT_ID(N'[dbo].[sportsbook_player]', N'U') IS NOT NULL
   AND COL_LENGTH(N'dbo.sportsbook_player', N'display_name_normalized') IS NOT NULL
   AND COL_LENGTH(N'dbo.sportsbook_player', N'team_abbrev_normalized') IS NOT NULL
   AND NOT EXISTS (
        SELECT 1 FROM sys.indexes
        WHERE name = N'UQ_sportsbook_player_natural_key'
          AND object_id = OBJECT_ID(N'dbo.sportsbook_player')
   )
BEGIN
    DECLARE @player_dupe_count int;
    SELECT @player_dupe_count = COUNT(*) FROM (
        SELECT canonical_league_id, display_name_normalized, team_abbrev_normalized
        FROM [dbo].[sportsbook_player]
        WHERE canonical_league_id IS NOT NULL
          AND display_name_normalized IS NOT NULL
          AND LEN(display_name_normalized) > 0
        GROUP BY canonical_league_id, display_name_normalized, team_abbrev_normalized
        HAVING COUNT(*) > 1
    ) g;
    IF @player_dupe_count > 0
    BEGIN
        DECLARE @player_msg nvarchar(400) =
            N'sportsbook_player has ' + CAST(@player_dupe_count AS nvarchar(20)) +
            N' duplicate (canonical_league_id, display_name_normalized, team_abbrev_normalized) group(s). ' +
            N'Review with: SELECT canonical_league_id, display_name_normalized, team_abbrev_normalized, COUNT(*) ' +
            N'FROM dbo.sportsbook_player WHERE canonical_league_id IS NOT NULL AND display_name_normalized IS NOT NULL ' +
            N'GROUP BY canonical_league_id, display_name_normalized, team_abbrev_normalized HAVING COUNT(*) > 1; ' +
            N'Resolve via ref.person_alias or consolidation, then rerun migration 0006.';
        RAISERROR(@player_msg, 16, 1);
    END;
END
GO

IF OBJECT_ID(N'[dbo].[sportsbook_player]', N'U') IS NOT NULL
   AND COL_LENGTH(N'dbo.sportsbook_player', N'display_name_normalized') IS NOT NULL
   AND COL_LENGTH(N'dbo.sportsbook_player', N'team_abbrev_normalized') IS NOT NULL
   AND NOT EXISTS (
        SELECT 1 FROM sys.indexes
        WHERE name = N'UQ_sportsbook_player_natural_key'
          AND object_id = OBJECT_ID(N'dbo.sportsbook_player')
   )
BEGIN
    CREATE UNIQUE NONCLUSTERED INDEX [UQ_sportsbook_player_natural_key]
        ON [dbo].[sportsbook_player]([canonical_league_id], [display_name_normalized], [team_abbrev_normalized])
        WHERE [canonical_league_id] IS NOT NULL
          AND [display_name_normalized] IS NOT NULL
          AND LEN([display_name_normalized]) > 0;
END
GO

-- =====================================================================
-- 3. sportsbook_game
-- =====================================================================
-- Natural key: fn_game_natural_key(canonical_league_id, home_sportsbook_team_id,
--                                   away_sportsbook_team_id, CAST(start_time AS date)).
-- Guard rows with null league_id or null start_time out of the unique
-- index (the natural key would collapse to '|||' for many-to-one).

IF OBJECT_ID(N'[dbo].[sportsbook_game]', N'U') IS NOT NULL
   AND COL_LENGTH(N'dbo.sportsbook_game', N'natural_key') IS NULL
BEGIN
    ALTER TABLE [dbo].[sportsbook_game]
        ADD [natural_key] AS
            CAST(dbo.fn_game_natural_key(
                canonical_league_id,
                home_sportsbook_team_id,
                away_sportsbook_team_id,
                CAST(start_time AS date)
            ) AS nvarchar(80))
            PERSISTED;
END
GO

IF OBJECT_ID(N'[dbo].[sportsbook_game]', N'U') IS NOT NULL
   AND COL_LENGTH(N'dbo.sportsbook_game', N'natural_key') IS NOT NULL
   AND NOT EXISTS (
        SELECT 1 FROM sys.indexes
        WHERE name = N'UQ_sportsbook_game_natural_key'
          AND object_id = OBJECT_ID(N'dbo.sportsbook_game')
   )
BEGIN
    DECLARE @game_dupe_count int;
    SELECT @game_dupe_count = COUNT(*) FROM (
        SELECT natural_key
        FROM [dbo].[sportsbook_game]
        WHERE canonical_league_id IS NOT NULL
          AND start_time IS NOT NULL
          AND home_sportsbook_team_id IS NOT NULL
          AND away_sportsbook_team_id IS NOT NULL
          AND natural_key IS NOT NULL
          AND LEN(natural_key) > 0
        GROUP BY natural_key
        HAVING COUNT(*) > 1
    ) g;
    IF @game_dupe_count > 0
    BEGIN
        DECLARE @game_msg nvarchar(400) =
            N'sportsbook_game has ' + CAST(@game_dupe_count AS nvarchar(20)) +
            N' duplicate natural_key group(s). ' +
            N'Review with: SELECT natural_key, COUNT(*) FROM dbo.sportsbook_game ' +
            N'WHERE natural_key IS NOT NULL GROUP BY natural_key HAVING COUNT(*) > 1; ' +
            N'Resolve via consolidation, then rerun migration 0006.';
        RAISERROR(@game_msg, 16, 1);
    END;
END
GO

IF OBJECT_ID(N'[dbo].[sportsbook_game]', N'U') IS NOT NULL
   AND COL_LENGTH(N'dbo.sportsbook_game', N'natural_key') IS NOT NULL
   AND NOT EXISTS (
        SELECT 1 FROM sys.indexes
        WHERE name = N'UQ_sportsbook_game_natural_key'
          AND object_id = OBJECT_ID(N'dbo.sportsbook_game')
   )
BEGIN
    CREATE UNIQUE NONCLUSTERED INDEX [UQ_sportsbook_game_natural_key]
        ON [dbo].[sportsbook_game]([natural_key])
        WHERE [canonical_league_id] IS NOT NULL
          AND [natural_key] IS NOT NULL
          AND LEN([natural_key]) > 0;
END
GO

-- =====================================================================
-- 4. sportsbook_stat_type
-- =====================================================================
-- normalized_stat_key already exists as a regular NVARCHAR column
-- populated by sportsbook_dimension_sync.py. No computed column needed;
-- just a filtered unique index on (canonical_league_id, normalized_stat_key).

IF OBJECT_ID(N'[dbo].[sportsbook_stat_type]', N'U') IS NOT NULL
   AND COL_LENGTH(N'dbo.sportsbook_stat_type', N'normalized_stat_key') IS NOT NULL
   AND NOT EXISTS (
        SELECT 1 FROM sys.indexes
        WHERE name = N'UQ_sportsbook_stat_type_league_norm_key'
          AND object_id = OBJECT_ID(N'dbo.sportsbook_stat_type')
   )
BEGIN
    DECLARE @stat_dupe_count int;
    SELECT @stat_dupe_count = COUNT(*) FROM (
        SELECT canonical_league_id, normalized_stat_key
        FROM [dbo].[sportsbook_stat_type]
        WHERE canonical_league_id IS NOT NULL
          AND normalized_stat_key IS NOT NULL
          AND LEN(normalized_stat_key) > 0
        GROUP BY canonical_league_id, normalized_stat_key
        HAVING COUNT(*) > 1
    ) g;
    IF @stat_dupe_count > 0
    BEGIN
        DECLARE @stat_msg nvarchar(400) =
            N'sportsbook_stat_type has ' + CAST(@stat_dupe_count AS nvarchar(20)) +
            N' duplicate (canonical_league_id, normalized_stat_key) group(s). ' +
            N'Review with: SELECT canonical_league_id, normalized_stat_key, COUNT(*) ' +
            N'FROM dbo.sportsbook_stat_type GROUP BY canonical_league_id, normalized_stat_key HAVING COUNT(*) > 1; ' +
            N'Resolve via consolidation, then rerun migration 0006.';
        RAISERROR(@stat_msg, 16, 1);
    END;
END
GO

IF OBJECT_ID(N'[dbo].[sportsbook_stat_type]', N'U') IS NOT NULL
   AND COL_LENGTH(N'dbo.sportsbook_stat_type', N'normalized_stat_key') IS NOT NULL
   AND NOT EXISTS (
        SELECT 1 FROM sys.indexes
        WHERE name = N'UQ_sportsbook_stat_type_league_norm_key'
          AND object_id = OBJECT_ID(N'dbo.sportsbook_stat_type')
   )
BEGIN
    CREATE UNIQUE NONCLUSTERED INDEX [UQ_sportsbook_stat_type_league_norm_key]
        ON [dbo].[sportsbook_stat_type]([canonical_league_id], [normalized_stat_key])
        WHERE [canonical_league_id] IS NOT NULL
          AND [normalized_stat_key] IS NOT NULL
          AND LEN([normalized_stat_key]) > 0;
END
GO
