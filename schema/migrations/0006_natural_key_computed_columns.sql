-- 0006_natural_key_computed_columns.sql
--
-- Plan step 1.5 — the "duplicates can't come back" guardrail.
--
-- Before adding PERSISTED computed columns and filtered UNIQUE indexes:
--
--   * Adds dbo.sportsbook_player.dedup_exempt (operator-only; sync never
--     touches it). Rows with dedup_exempt = 1 are excluded from the player
--     natural-key unique index so two real humans can share the same
--     normalized (league, name, team) when explicitly flagged — e.g. two
--     Elias Petterssons on VAN (#40 F vs #25 D).
--
--   * Consolidates 14 operator-approved duplicate pairs (loser -> survivor).
--     Survivor keeps scalar attributes via COALESCE(survivor, loser); xrefs on
--     the loser are repointed or dropped when they duplicate survivor keys.
--     Elias Pettersson is not merged; survivor 6432 / exempt 94241.
--
-- Then adds PERSISTED computed columns and filtered UNIQUE indexes on the
-- natural keys of sportsbook_team, sportsbook_player, sportsbook_game, and
-- sportsbook_stat_type (same design as before, with dedup_exempt = 0 on the
-- player index and dupe pre-flight).

SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
GO

-- =====================================================================
-- 0. sportsbook_player — dedup_exempt + approved merges + Pettersson flag
-- =====================================================================

IF OBJECT_ID(N'[dbo].[sportsbook_player]', N'U') IS NOT NULL
   AND COL_LENGTH(N'dbo.sportsbook_player', N'dedup_exempt') IS NULL
BEGIN
    ALTER TABLE [dbo].[sportsbook_player]
        ADD [dedup_exempt] [bit] NOT NULL
            CONSTRAINT [DF_sportsbook_player_dedup_exempt] DEFAULT (0);
END
GO

IF OBJECT_ID(N'[dbo].[sportsbook_player]', N'U') IS NOT NULL
BEGIN
    -- Idempotent: runs only while at least one loser row still exists.
    IF EXISTS (
        SELECT 1
        FROM (VALUES
            (CAST(96255 AS bigint)), (117377), (114048), (73853), (108060), (94263),
            (17423), (19199), (15380), (15101), (12795), (6877), (16908), (15568)
        ) AS v(loser_id)
        INNER JOIN [dbo].[sportsbook_player] AS p
            ON p.sportsbook_player_id = v.loser_id
    )
    BEGIN
        IF OBJECT_ID(N'tempdb..#player_dedup_merge') IS NOT NULL
            DROP TABLE #player_dedup_merge;

        CREATE TABLE #player_dedup_merge (
            [loser_id]     [bigint] NOT NULL PRIMARY KEY,
            [survivor_id]  [bigint] NOT NULL
        );

        -- Loser -> survivor (operator-approved). Rationale in brief:
        -- Cole O'Hara: keep 99531 (#19, Underdog);96255 is stale #65.
        -- Anderson Duarte, Dje D'Avilla, Gessime Yassine, Harry Howell,
        -- Matteo Cocchi: keep row with PrizePicks id (lower id when both had).
        -- Choi Won-jun: 17423 -> 16496; Kang Baek-ho: 19199 -> 15871.
        -- Tubelis, Musa, Maledon, 910, Köster, Uščins: keep lower sportsbook_player_id.
        INSERT INTO #player_dedup_merge ([loser_id], [survivor_id]) VALUES
            (96255, 99531),
            (117377, 9764),
            (114048, 17319),
            (73853, 10025),
            (108060, 8958),
            (94263, 7866),
            (17423, 16496),
            (19199, 15871),
            (15380, 12794),
            (15101, 6394),
            (12795, 6661),
            (6877, 6740),
            (16908, 6550),
            (15568, 6532);

        IF EXISTS (
            SELECT 1
            FROM #player_dedup_merge AS m1
            INNER JOIN #player_dedup_merge AS m2
                ON m1.survivor_id = m2.loser_id
        )
        BEGIN
            RAISERROR(N'0006: invalid merge map (survivor_id equals another loser_id).', 16, 1);
        END;

        IF EXISTS (
            SELECT 1
            FROM #player_dedup_merge AS m1
            INNER JOIN #player_dedup_merge AS m2
                ON m1.loser_id = m2.survivor_id
        )
        BEGIN
            RAISERROR(N'0006: invalid merge map (loser_id equals another survivor_id).', 16, 1);
        END;

        -- Drop loser xrefs whose (sportsbook, external_player_id) already exists on survivor.
        IF OBJECT_ID(N'[dbo].[sportsbook_player_xref]', N'U') IS NOT NULL
        BEGIN
            DELETE x
            FROM [dbo].[sportsbook_player_xref] AS x
            INNER JOIN #player_dedup_merge AS m
                ON x.sportsbook_player_id = m.loser_id
            WHERE EXISTS (
                SELECT 1
                FROM [dbo].[sportsbook_player_xref] AS x2
                WHERE x2.sportsbook = x.sportsbook
                  AND x2.external_player_id = x.external_player_id
                  AND x2.sportsbook_player_id = m.survivor_id
            );

            UPDATE x
            SET x.sportsbook_player_id = m.survivor_id
            FROM [dbo].[sportsbook_player_xref] AS x
            INNER JOIN #player_dedup_merge AS m
                ON x.sportsbook_player_id = m.loser_id;
        END;

        UPDATE s
        SET
            s.[canonical_league_id]     = COALESCE(s.[canonical_league_id],     l.[canonical_league_id]),
            s.[sportsbook_sport_id]     = COALESCE(s.[sportsbook_sport_id],     l.[sportsbook_sport_id]),
            s.[sportsbook_league_id]    = COALESCE(s.[sportsbook_league_id],    l.[sportsbook_league_id]),
            s.[sportsbook_team_id]      = COALESCE(s.[sportsbook_team_id],      l.[sportsbook_team_id]),
            s.[first_name]              = COALESCE(s.[first_name],              l.[first_name]),
            s.[last_name]               = COALESCE(s.[last_name],               l.[last_name]),
            s.[display_name]            = COALESCE(s.[display_name],            l.[display_name]),
            s.[jersey_number]           = COALESCE(s.[jersey_number],           l.[jersey_number]),
            s.[position]                = COALESCE(s.[position],                l.[position]),
            s.[team_name]               = COALESCE(s.[team_name],               l.[team_name]),
            s.[team_abbrev]             = COALESCE(s.[team_abbrev],             l.[team_abbrev]),
            s.[prizepicks_player_id]    = COALESCE(s.[prizepicks_player_id],    l.[prizepicks_player_id]),
            s.[underdog_player_id]      = COALESCE(s.[underdog_player_id],      l.[underdog_player_id]),
            s.[parlay_play_player_id]   = COALESCE(s.[parlay_play_player_id],   l.[parlay_play_player_id])
        FROM [dbo].[sportsbook_player] AS s
        INNER JOIN #player_dedup_merge AS m
            ON s.sportsbook_player_id = m.survivor_id
        INNER JOIN [dbo].[sportsbook_player] AS l
            ON l.sportsbook_player_id = m.loser_id;

        DELETE l
        FROM [dbo].[sportsbook_player] AS l
        INNER JOIN #player_dedup_merge AS m
            ON l.sportsbook_player_id = m.loser_id;

        DROP TABLE #player_dedup_merge;
    END;

    -- Elias Pettersson (#25 D): exempt from natural-key uniqueness (6432 #40 F stays indexed).
    UPDATE [dbo].[sportsbook_player]
    SET [dedup_exempt] = 1
    WHERE [sportsbook_player_id] = 94241 AND [dedup_exempt] = 0;
END
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
          AND [abbreviation] IS NOT NULL
          AND [abbreviation] <> N'';
END
GO

-- =====================================================================
-- 2. sportsbook_player
-- =====================================================================
-- Natural key: (canonical_league_id, normalized display_name, normalized team_abbrev).
-- dedup_exempt = 1 rows are excluded from the unique index (see plan step 1.5).

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
   AND COL_LENGTH(N'dbo.sportsbook_player', N'dedup_exempt') IS NOT NULL
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
        WHERE [dedup_exempt] = 0
          AND canonical_league_id IS NOT NULL
          AND display_name_normalized IS NOT NULL
          AND LEN(display_name_normalized) > 0
        GROUP BY canonical_league_id, display_name_normalized, team_abbrev_normalized
        HAVING COUNT(*) > 1
    ) g;
    IF @player_dupe_count > 0
    BEGIN
        DECLARE @player_msg nvarchar(400) =
            N'sportsbook_player has ' + CAST(@player_dupe_count AS nvarchar(20)) +
            N' duplicate (canonical_league_id, display_name_normalized, team_abbrev_normalized) group(s) among dedup_exempt=0 rows. ' +
            N'Review with: SELECT canonical_league_id, display_name_normalized, team_abbrev_normalized, COUNT(*) ' +
            N'FROM dbo.sportsbook_player WHERE dedup_exempt = 0 AND canonical_league_id IS NOT NULL AND display_name_normalized IS NOT NULL ' +
            N'GROUP BY canonical_league_id, display_name_normalized, team_abbrev_normalized HAVING COUNT(*) > 1; ' +
            N'Resolve via ref.person_alias, consolidation, or dedup_exempt, then rerun migration 0006.';
        RAISERROR(@player_msg, 16, 1);
    END;
END
GO

IF OBJECT_ID(N'[dbo].[sportsbook_player]', N'U') IS NOT NULL
   AND COL_LENGTH(N'dbo.sportsbook_player', N'display_name_normalized') IS NOT NULL
   AND COL_LENGTH(N'dbo.sportsbook_player', N'team_abbrev_normalized') IS NOT NULL
   AND COL_LENGTH(N'dbo.sportsbook_player', N'dedup_exempt') IS NOT NULL
   AND NOT EXISTS (
        SELECT 1 FROM sys.indexes
        WHERE name = N'UQ_sportsbook_player_natural_key'
          AND object_id = OBJECT_ID(N'dbo.sportsbook_player')
   )
BEGIN
    CREATE UNIQUE NONCLUSTERED INDEX [UQ_sportsbook_player_natural_key]
        ON [dbo].[sportsbook_player]([canonical_league_id], [display_name_normalized], [team_abbrev_normalized])
        WHERE [canonical_league_id] IS NOT NULL
          AND [display_name] IS NOT NULL
          AND [display_name] <> N''
          AND [team_abbrev] IS NOT NULL
          AND [team_abbrev] <> N''
          AND [dedup_exempt] = 0;
END
GO

-- =====================================================================
-- 3. sportsbook_game
-- =====================================================================
-- Natural key: same 4-part pipe format as dbo.fn_game_natural_key / Python
-- game_natural_key. Guard null league/start/teams out of the unique index.
--
-- PERSISTED computed columns must be deterministic. Scalar UDFs in persisted
-- expressions are rejected in practice here (4936). The column is therefore
-- inlined using YEAR/MONTH/DAY for the ISO date fragment (deterministic).
-- fn_game_natural_key is kept in sync for ad-hoc SQL and parity checks.

IF OBJECT_ID(N'dbo.fn_game_natural_key', N'FN') IS NOT NULL
    DROP FUNCTION dbo.fn_game_natural_key;
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

IF OBJECT_ID(N'[dbo].[sportsbook_game]', N'U') IS NOT NULL
   AND COL_LENGTH(N'dbo.sportsbook_game', N'natural_key') IS NULL
BEGIN
    ALTER TABLE [dbo].[sportsbook_game]
        ADD [natural_key] AS
            CAST(
                ISNULL(CAST([canonical_league_id] AS nvarchar(20)), N'') + N'|' +
                CASE WHEN [start_time] IS NULL THEN N''
                     ELSE
                         CAST(YEAR(CAST([start_time] AS date)) AS nvarchar(4)) + N'-' +
                         RIGHT(N'00' + CAST(MONTH(CAST([start_time] AS date)) AS nvarchar(2)), 2) + N'-' +
                         RIGHT(N'00' + CAST(DAY(CAST([start_time] AS date)) AS nvarchar(2)), 2)
                END + N'|' +
                ISNULL(CAST([home_sportsbook_team_id] AS nvarchar(20)), N'') + N'|' +
                ISNULL(CAST([away_sportsbook_team_id] AS nvarchar(20)), N'')
            AS nvarchar(80))
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
          AND [start_time] IS NOT NULL
          AND [home_sportsbook_team_id] IS NOT NULL
          AND [away_sportsbook_team_id] IS NOT NULL;
END
GO

-- =====================================================================
-- 4. sportsbook_stat_type
-- =====================================================================

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
          AND [normalized_stat_key] <> N'';
END
GO
