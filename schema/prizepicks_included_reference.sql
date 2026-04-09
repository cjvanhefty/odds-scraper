-- PrizePicks JSON:API `included` reference data (all types except `score`).
-- Pattern: load rows into *_stage (truncate + insert per scrape), then
-- EXEC [dbo].[MergePrizepicksIncludedReferenceFromStage].
-- MERGE updates only when at least one column differs (unchanged rows are left alone).
-- Game rows use CHECKSUM for change detection; rare hash collisions could skip an update.
--
-- prizepicks_player_stage / prizepicks_player: also created or altered here (ppid, last_modified_at).
-- prizepicks_scraper.upsert_player_from_stage still MERGEs unconditionally; use this procedure for no-op semantics.
-- prizepicks_game / prizepicks_game_stage: created here if missing (matches PRIZEPICKS_GAME_STAGE_COLS in prizepicks_scraper.py).

USE [Props]
GO

SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER ON
GO

-- ---------------------------------------------------------------------------
-- prizepicks_league
-- ---------------------------------------------------------------------------
IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'prizepicks_league_stage' AND schema_id = SCHEMA_ID('dbo'))
BEGIN
    CREATE TABLE [dbo].[prizepicks_league_stage](
        [league_id] [nvarchar](20) NOT NULL,
        [active] [bit] NULL,
        [f2p_enabled] [bit] NULL,
        [has_live_projections] [bit] NULL,
        [icon] [nvarchar](50) NULL,
        [image_url] [nvarchar](2000) NULL,
        [last_five_games_enabled] [bit] NULL,
        [league_icon_id] [int] NULL,
        [name] [nvarchar](100) NOT NULL,
        [parent_id] [nvarchar](20) NULL,
        [parent_name] [nvarchar](100) NULL,
        [projections_count] [int] NULL,
        [rank] [int] NULL,
        [show_trending] [bit] NULL,
        [projection_filters_json] [nvarchar](max) NULL
    ) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
END
GO

IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'prizepicks_league' AND schema_id = SCHEMA_ID('dbo'))
BEGIN
    CREATE TABLE [dbo].[prizepicks_league](
        [league_id] [nvarchar](20) NOT NULL,
        [active] [bit] NULL,
        [f2p_enabled] [bit] NULL,
        [has_live_projections] [bit] NULL,
        [icon] [nvarchar](50) NULL,
        [image_url] [nvarchar](2000) NULL,
        [last_five_games_enabled] [bit] NULL,
        [league_icon_id] [int] NULL,
        [name] [nvarchar](100) NOT NULL,
        [parent_id] [nvarchar](20) NULL,
        [parent_name] [nvarchar](100) NULL,
        [projections_count] [int] NULL,
        [rank] [int] NULL,
        [show_trending] [bit] NULL,
        [projection_filters_json] [nvarchar](max) NULL,
        [last_modified_at] [datetime2](7) NOT NULL CONSTRAINT [DF_prizepicks_league_last_modified_at] DEFAULT (CONVERT(datetime2(7), SYSUTCDATETIME() AT TIME ZONE 'UTC' AT TIME ZONE 'Central Standard Time')),
        CONSTRAINT [PK_prizepicks_league] PRIMARY KEY CLUSTERED ([league_id] ASC)
    ) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
END
GO

-- ---------------------------------------------------------------------------
-- prizepicks_team
-- ---------------------------------------------------------------------------
IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'prizepicks_team_stage' AND schema_id = SCHEMA_ID('dbo'))
BEGIN
    CREATE TABLE [dbo].[prizepicks_team_stage](
        [team_id] [nvarchar](20) NOT NULL,
        [abbreviation] [nvarchar](10) NULL,
        [market] [nvarchar](100) NULL,
        [name] [nvarchar](100) NULL,
        [primary_color] [nvarchar](20) NULL,
        [secondary_color] [nvarchar](20) NULL,
        [tertiary_color] [nvarchar](20) NULL
    ) ON [PRIMARY]
END
GO

IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'prizepicks_team' AND schema_id = SCHEMA_ID('dbo'))
BEGIN
    CREATE TABLE [dbo].[prizepicks_team](
        [team_id] [nvarchar](20) NOT NULL,
        [abbreviation] [nvarchar](10) NULL,
        [market] [nvarchar](100) NULL,
        [name] [nvarchar](100) NULL,
        [primary_color] [nvarchar](20) NULL,
        [secondary_color] [nvarchar](20) NULL,
        [tertiary_color] [nvarchar](20) NULL,
        [last_modified_at] [datetime2](7) NOT NULL CONSTRAINT [DF_prizepicks_team_last_modified_at] DEFAULT (CONVERT(datetime2(7), SYSUTCDATETIME() AT TIME ZONE 'UTC' AT TIME ZONE 'Central Standard Time')),
        CONSTRAINT [PK_prizepicks_team] PRIMARY KEY CLUSTERED ([team_id] ASC)
    ) ON [PRIMARY]
END
GO

-- ---------------------------------------------------------------------------
-- prizepicks_stat_type
-- ---------------------------------------------------------------------------
IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'prizepicks_stat_type_stage' AND schema_id = SCHEMA_ID('dbo'))
BEGIN
    CREATE TABLE [dbo].[prizepicks_stat_type_stage](
        [stat_type_id] [nvarchar](20) NOT NULL,
        [name] [nvarchar](200) NOT NULL,
        [rank] [int] NULL,
        [lfg_ignored_leagues_json] [nvarchar](max) NULL
    ) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
END
GO

IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'prizepicks_stat_type' AND schema_id = SCHEMA_ID('dbo'))
BEGIN
    CREATE TABLE [dbo].[prizepicks_stat_type](
        [stat_type_id] [nvarchar](20) NOT NULL,
        [name] [nvarchar](200) NOT NULL,
        [rank] [int] NULL,
        [lfg_ignored_leagues_json] [nvarchar](max) NULL,
        [last_modified_at] [datetime2](7) NOT NULL CONSTRAINT [DF_prizepicks_stat_type_last_modified_at] DEFAULT (CONVERT(datetime2(7), SYSUTCDATETIME() AT TIME ZONE 'UTC' AT TIME ZONE 'Central Standard Time')),
        CONSTRAINT [PK_prizepicks_stat_type] PRIMARY KEY CLUSTERED ([stat_type_id] ASC)
    ) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
END
GO

-- ---------------------------------------------------------------------------
-- prizepicks_duration
-- ---------------------------------------------------------------------------
IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'prizepicks_duration_stage' AND schema_id = SCHEMA_ID('dbo'))
BEGIN
    CREATE TABLE [dbo].[prizepicks_duration_stage](
        [duration_id] [nvarchar](20) NOT NULL,
        [name] [nvarchar](100) NOT NULL
    ) ON [PRIMARY]
END
GO

IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'prizepicks_duration' AND schema_id = SCHEMA_ID('dbo'))
BEGIN
    CREATE TABLE [dbo].[prizepicks_duration](
        [duration_id] [nvarchar](20) NOT NULL,
        [name] [nvarchar](100) NOT NULL,
        [last_modified_at] [datetime2](7) NOT NULL CONSTRAINT [DF_prizepicks_duration_last_modified_at] DEFAULT (CONVERT(datetime2(7), SYSUTCDATETIME() AT TIME ZONE 'UTC' AT TIME ZONE 'Central Standard Time')),
        CONSTRAINT [PK_prizepicks_duration] PRIMARY KEY CLUSTERED ([duration_id] ASC)
    ) ON [PRIMARY]
END
GO

-- ---------------------------------------------------------------------------
-- prizepicks_projection_type
-- ---------------------------------------------------------------------------
IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'prizepicks_projection_type_stage' AND schema_id = SCHEMA_ID('dbo'))
BEGIN
    CREATE TABLE [dbo].[prizepicks_projection_type_stage](
        [projection_type_id] [nvarchar](20) NOT NULL,
        [name] [nvarchar](100) NOT NULL
    ) ON [PRIMARY]
END
GO

IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'prizepicks_projection_type' AND schema_id = SCHEMA_ID('dbo'))
BEGIN
    CREATE TABLE [dbo].[prizepicks_projection_type](
        [projection_type_id] [nvarchar](20) NOT NULL,
        [name] [nvarchar](100) NOT NULL,
        [last_modified_at] [datetime2](7) NOT NULL CONSTRAINT [DF_prizepicks_projection_type_last_modified_at] DEFAULT (CONVERT(datetime2(7), SYSUTCDATETIME() AT TIME ZONE 'UTC' AT TIME ZONE 'Central Standard Time')),
        CONSTRAINT [PK_prizepicks_projection_type] PRIMARY KEY CLUSTERED ([projection_type_id] ASC)
    ) ON [PRIMARY]
END
GO

-- ---------------------------------------------------------------------------
-- prizepicks_game (matches prizepicks_scraper.parse_to_game_stage_records incl. metadata_json)
-- ---------------------------------------------------------------------------
IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'prizepicks_game_stage' AND schema_id = SCHEMA_ID('dbo'))
BEGIN
    CREATE TABLE [dbo].[prizepicks_game_stage](
        [game_id] [nvarchar](20) NOT NULL,
        [external_game_id] [nvarchar](100) NULL,
        [created_at] [nvarchar](50) NULL,
        [end_time] [nvarchar](50) NULL,
        [start_time] [nvarchar](50) NULL,
        [updated_at] [nvarchar](50) NULL,
        [is_live] [bit] NULL,
        [status] [nvarchar](50) NULL,
        [away_team_id] [nvarchar](20) NULL,
        [home_team_id] [nvarchar](20) NULL,
        [league_name] [nvarchar](50) NULL,
        [metadata_status] [nvarchar](50) NULL,
        [away_abbreviation] [nvarchar](10) NULL,
        [home_abbreviation] [nvarchar](10) NULL,
        [abbreviation] [nvarchar](10) NULL,
        [market] [nvarchar](100) NULL,
        [name] [nvarchar](100) NULL,
        [primary_color] [nvarchar](10) NULL,
        [secondary_color] [nvarchar](10) NULL,
        [tertiary_color] [nvarchar](10) NULL,
        [lfg_ignored_leagues] [nvarchar](500) NULL,
        [rank] [int] NULL,
        [combo] [bit] NULL,
        [display_name] [nvarchar](255) NULL,
        [image_url] [nvarchar](2000) NULL,
        [jersey_number] [nvarchar](10) NULL,
        [league] [nvarchar](50) NULL,
        [league_id] [nvarchar](20) NULL,
        [position] [nvarchar](50) NULL,
        [team] [nvarchar](10) NULL,
        [team_name] [nvarchar](100) NULL,
        [active] [bit] NULL,
        [f2p_enabled] [bit] NULL,
        [has_live_projections] [bit] NULL,
        [icon] [nvarchar](50) NULL,
        [last_five_games_enabled] [bit] NULL,
        [league_icon_id] [int] NULL,
        [parent_id] [nvarchar](20) NULL,
        [parent_name] [nvarchar](100) NULL,
        [projections_count] [int] NULL,
        [show_trending] [bit] NULL,
        [metadata_json] [nvarchar](max) NULL
    ) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
END
GO

IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'prizepicks_game' AND schema_id = SCHEMA_ID('dbo'))
BEGIN
    CREATE TABLE [dbo].[prizepicks_game](
        [game_id] [nvarchar](20) NOT NULL,
        [external_game_id] [nvarchar](100) NULL,
        [created_at] [nvarchar](50) NULL,
        [end_time] [nvarchar](50) NULL,
        [start_time] [nvarchar](50) NULL,
        [updated_at] [nvarchar](50) NULL,
        [is_live] [bit] NULL,
        [status] [nvarchar](50) NULL,
        [away_team_id] [nvarchar](20) NULL,
        [home_team_id] [nvarchar](20) NULL,
        [league_name] [nvarchar](50) NULL,
        [metadata_status] [nvarchar](50) NULL,
        [away_abbreviation] [nvarchar](10) NULL,
        [home_abbreviation] [nvarchar](10) NULL,
        [abbreviation] [nvarchar](10) NULL,
        [market] [nvarchar](100) NULL,
        [name] [nvarchar](100) NULL,
        [primary_color] [nvarchar](10) NULL,
        [secondary_color] [nvarchar](10) NULL,
        [tertiary_color] [nvarchar](10) NULL,
        [lfg_ignored_leagues] [nvarchar](500) NULL,
        [rank] [int] NULL,
        [combo] [bit] NULL,
        [display_name] [nvarchar](255) NULL,
        [image_url] [nvarchar](2000) NULL,
        [jersey_number] [nvarchar](10) NULL,
        [league] [nvarchar](50) NULL,
        [league_id] [nvarchar](20) NULL,
        [position] [nvarchar](50) NULL,
        [team] [nvarchar](10) NULL,
        [team_name] [nvarchar](100) NULL,
        [active] [bit] NULL,
        [f2p_enabled] [bit] NULL,
        [has_live_projections] [bit] NULL,
        [icon] [nvarchar](50) NULL,
        [last_five_games_enabled] [bit] NULL,
        [league_icon_id] [int] NULL,
        [parent_id] [nvarchar](20) NULL,
        [parent_name] [nvarchar](100) NULL,
        [projections_count] [int] NULL,
        [show_trending] [bit] NULL,
        [metadata_json] [nvarchar](max) NULL,
        [last_modified_at] [datetime2](7) NOT NULL CONSTRAINT [DF_prizepicks_game_last_modified_at] DEFAULT (CONVERT(datetime2(7), SYSUTCDATETIME() AT TIME ZONE 'UTC' AT TIME ZONE 'Central Standard Time')),
        CONSTRAINT [PK_prizepicks_game] PRIMARY KEY CLUSTERED ([game_id] ASC)
    ) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
END
GO

-- ---------------------------------------------------------------------------
-- prizepicks_player (new_player) — align with prizepicks_player_stage + optional ppid
-- ---------------------------------------------------------------------------
IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'prizepicks_player_stage' AND schema_id = SCHEMA_ID('dbo'))
BEGIN
    CREATE TABLE [dbo].[prizepicks_player_stage](
        [player_id] [nvarchar](20) NOT NULL,
        [combo] [bit] NOT NULL,
        [display_name] [nvarchar](255) NOT NULL,
        [image_url] [nvarchar](2000) NULL,
        [jersey_number] [nvarchar](10) NULL,
        [league] [nvarchar](50) NULL,
        [market] [nvarchar](100) NULL,
        [name] [nvarchar](255) NOT NULL,
        [position] [nvarchar](50) NULL,
        [team] [nvarchar](10) NULL,
        [team_name] [nvarchar](100) NULL,
        [league_id] [nvarchar](20) NULL,
        [team_id] [nvarchar](20) NULL,
        [ppid] [nvarchar](200) NULL,
        CONSTRAINT [PK_prizepicks_player_stage] PRIMARY KEY CLUSTERED ([player_id] ASC)
    ) ON [PRIMARY]
END
GO

IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'prizepicks_player' AND schema_id = SCHEMA_ID('dbo'))
BEGIN
    CREATE TABLE [dbo].[prizepicks_player](
        [player_id] [nvarchar](20) NOT NULL,
        [combo] [bit] NOT NULL,
        [display_name] [nvarchar](255) NOT NULL,
        [image_url] [nvarchar](2000) NULL,
        [jersey_number] [nvarchar](10) NULL,
        [league] [nvarchar](50) NULL,
        [market] [nvarchar](100) NULL,
        [name] [nvarchar](255) NOT NULL,
        [position] [nvarchar](50) NULL,
        [team] [nvarchar](10) NULL,
        [team_name] [nvarchar](100) NULL,
        [league_id] [nvarchar](20) NULL,
        [team_id] [nvarchar](20) NULL,
        [ppid] [nvarchar](200) NULL,
        [last_modified_at] [datetime2](7) NOT NULL CONSTRAINT [DF_prizepicks_player_last_modified_at] DEFAULT (CONVERT(datetime2(7), SYSUTCDATETIME() AT TIME ZONE 'UTC' AT TIME ZONE 'Central Standard Time')),
        CONSTRAINT [PK_prizepicks_player] PRIMARY KEY CLUSTERED ([player_id] ASC)
    ) ON [PRIMARY]
END
GO

-- Add [ppid] to stage / main when tables already existed (e.g. from Python)
IF EXISTS (SELECT 1 FROM sys.tables WHERE name = 'prizepicks_player_stage' AND schema_id = SCHEMA_ID('dbo'))
   AND NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dbo].[prizepicks_player_stage]') AND name = N'ppid')
BEGIN
    ALTER TABLE [dbo].[prizepicks_player_stage] ADD [ppid] [nvarchar](200) NULL;
END
GO

IF EXISTS (SELECT 1 FROM sys.tables WHERE name = 'prizepicks_player' AND schema_id = SCHEMA_ID('dbo'))
   AND NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dbo].[prizepicks_player]') AND name = N'ppid')
BEGIN
    ALTER TABLE [dbo].[prizepicks_player] ADD [ppid] [nvarchar](200) NULL;
END
GO

IF EXISTS (SELECT 1 FROM sys.tables WHERE name = 'prizepicks_player' AND schema_id = SCHEMA_ID('dbo'))
   AND NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dbo].[prizepicks_player]') AND name = N'last_modified_at')
BEGIN
    ALTER TABLE [dbo].[prizepicks_player] ADD [last_modified_at] [datetime2](7) NOT NULL CONSTRAINT [DF_prizepicks_player_last_modified_at_existing] DEFAULT (CONVERT(datetime2(7), SYSUTCDATETIME() AT TIME ZONE 'UTC' AT TIME ZONE 'Central Standard Time'));
END
GO

-- Align existing game tables (created outside this script) with metadata_json / last_modified_at
IF EXISTS (SELECT 1 FROM sys.tables WHERE name = 'prizepicks_game_stage' AND schema_id = SCHEMA_ID('dbo'))
   AND NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dbo].[prizepicks_game_stage]') AND name = N'metadata_json')
BEGIN
    ALTER TABLE [dbo].[prizepicks_game_stage] ADD [metadata_json] [nvarchar](max) NULL;
END
GO

IF EXISTS (SELECT 1 FROM sys.tables WHERE name = 'prizepicks_game' AND schema_id = SCHEMA_ID('dbo'))
   AND NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dbo].[prizepicks_game]') AND name = N'metadata_json')
BEGIN
    ALTER TABLE [dbo].[prizepicks_game] ADD [metadata_json] [nvarchar](max) NULL;
END
GO

IF EXISTS (SELECT 1 FROM sys.tables WHERE name = 'prizepicks_game' AND schema_id = SCHEMA_ID('dbo'))
   AND NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dbo].[prizepicks_game]') AND name = N'last_modified_at')
BEGIN
    ALTER TABLE [dbo].[prizepicks_game] ADD [last_modified_at] [datetime2](7) NOT NULL CONSTRAINT [DF_prizepicks_game_last_modified_at_existing] DEFAULT (CONVERT(datetime2(7), SYSUTCDATETIME() AT TIME ZONE 'UTC' AT TIME ZONE 'Central Standard Time'));
END
GO

-- If these tables existed before this script ran, CREATE was skipped and they may lack last_modified_at (MERGE requires it).
IF EXISTS (SELECT 1 FROM sys.tables WHERE name = 'prizepicks_league' AND schema_id = SCHEMA_ID('dbo'))
   AND NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dbo].[prizepicks_league]') AND name = N'last_modified_at')
BEGIN
    ALTER TABLE [dbo].[prizepicks_league] ADD [last_modified_at] [datetime2](7) NOT NULL CONSTRAINT [DF_prizepicks_league_lma] DEFAULT (CONVERT(datetime2(7), SYSUTCDATETIME() AT TIME ZONE 'UTC' AT TIME ZONE 'Central Standard Time'));
END
GO

IF EXISTS (SELECT 1 FROM sys.tables WHERE name = 'prizepicks_team' AND schema_id = SCHEMA_ID('dbo'))
   AND NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dbo].[prizepicks_team]') AND name = N'last_modified_at')
BEGIN
    ALTER TABLE [dbo].[prizepicks_team] ADD [last_modified_at] [datetime2](7) NOT NULL CONSTRAINT [DF_prizepicks_team_lma] DEFAULT (CONVERT(datetime2(7), SYSUTCDATETIME() AT TIME ZONE 'UTC' AT TIME ZONE 'Central Standard Time'));
END
GO

IF EXISTS (SELECT 1 FROM sys.tables WHERE name = 'prizepicks_stat_type' AND schema_id = SCHEMA_ID('dbo'))
   AND NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dbo].[prizepicks_stat_type]') AND name = N'last_modified_at')
BEGIN
    ALTER TABLE [dbo].[prizepicks_stat_type] ADD [last_modified_at] [datetime2](7) NOT NULL CONSTRAINT [DF_prizepicks_stat_type_lma] DEFAULT (CONVERT(datetime2(7), SYSUTCDATETIME() AT TIME ZONE 'UTC' AT TIME ZONE 'Central Standard Time'));
END
GO

IF EXISTS (SELECT 1 FROM sys.tables WHERE name = 'prizepicks_duration' AND schema_id = SCHEMA_ID('dbo'))
   AND NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dbo].[prizepicks_duration]') AND name = N'last_modified_at')
BEGIN
    ALTER TABLE [dbo].[prizepicks_duration] ADD [last_modified_at] [datetime2](7) NOT NULL CONSTRAINT [DF_prizepicks_duration_lma] DEFAULT (CONVERT(datetime2(7), SYSUTCDATETIME() AT TIME ZONE 'UTC' AT TIME ZONE 'Central Standard Time'));
END
GO

IF EXISTS (SELECT 1 FROM sys.tables WHERE name = 'prizepicks_projection_type' AND schema_id = SCHEMA_ID('dbo'))
   AND NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dbo].[prizepicks_projection_type]') AND name = N'last_modified_at')
BEGIN
    ALTER TABLE [dbo].[prizepicks_projection_type] ADD [last_modified_at] [datetime2](7) NOT NULL CONSTRAINT [DF_prizepicks_projection_type_lma] DEFAULT (CONVERT(datetime2(7), SYSUTCDATETIME() AT TIME ZONE 'UTC' AT TIME ZONE 'Central Standard Time'));
END
GO

-- ---------------------------------------------------------------------------
-- MERGE: conditional updates (no row update when stage matches target)
-- ---------------------------------------------------------------------------
IF OBJECT_ID(N'[dbo].[MergePrizepicksIncludedReferenceFromStage]', N'P') IS NOT NULL
    DROP PROCEDURE [dbo].[MergePrizepicksIncludedReferenceFromStage];
GO

CREATE PROCEDURE [dbo].[MergePrizepicksIncludedReferenceFromStage]
AS
BEGIN
    SET NOCOUNT ON;

    ;MERGE [dbo].[prizepicks_league] AS t
    USING [dbo].[prizepicks_league_stage] AS s ON t.[league_id] = s.[league_id]
    WHEN MATCHED AND NOT (
            (t.[active] = s.[active] OR (t.[active] IS NULL AND s.[active] IS NULL))
        AND (t.[f2p_enabled] = s.[f2p_enabled] OR (t.[f2p_enabled] IS NULL AND s.[f2p_enabled] IS NULL))
        AND (t.[has_live_projections] = s.[has_live_projections] OR (t.[has_live_projections] IS NULL AND s.[has_live_projections] IS NULL))
        AND (t.[icon] = s.[icon] OR (t.[icon] IS NULL AND s.[icon] IS NULL))
        AND (t.[image_url] = s.[image_url] OR (t.[image_url] IS NULL AND s.[image_url] IS NULL))
        AND (t.[last_five_games_enabled] = s.[last_five_games_enabled] OR (t.[last_five_games_enabled] IS NULL AND s.[last_five_games_enabled] IS NULL))
        AND (t.[league_icon_id] = s.[league_icon_id] OR (t.[league_icon_id] IS NULL AND s.[league_icon_id] IS NULL))
        AND (t.[name] = s.[name] OR (t.[name] IS NULL AND s.[name] IS NULL))
        AND (t.[parent_id] = s.[parent_id] OR (t.[parent_id] IS NULL AND s.[parent_id] IS NULL))
        AND (t.[parent_name] = s.[parent_name] OR (t.[parent_name] IS NULL AND s.[parent_name] IS NULL))
        AND (t.[projections_count] = s.[projections_count] OR (t.[projections_count] IS NULL AND s.[projections_count] IS NULL))
        AND (t.[rank] = s.[rank] OR (t.[rank] IS NULL AND s.[rank] IS NULL))
        AND (t.[show_trending] = s.[show_trending] OR (t.[show_trending] IS NULL AND s.[show_trending] IS NULL))
        AND (t.[projection_filters_json] = s.[projection_filters_json] OR (t.[projection_filters_json] IS NULL AND s.[projection_filters_json] IS NULL))
    ) THEN UPDATE SET
        [active] = s.[active],
        [f2p_enabled] = s.[f2p_enabled],
        [has_live_projections] = s.[has_live_projections],
        [icon] = s.[icon],
        [image_url] = s.[image_url],
        [last_five_games_enabled] = s.[last_five_games_enabled],
        [league_icon_id] = s.[league_icon_id],
        [name] = s.[name],
        [parent_id] = s.[parent_id],
        [parent_name] = s.[parent_name],
        [projections_count] = s.[projections_count],
        [rank] = s.[rank],
        [show_trending] = s.[show_trending],
        [projection_filters_json] = s.[projection_filters_json],
        [last_modified_at] = GETDATE()
    WHEN NOT MATCHED BY TARGET THEN INSERT (
        [league_id], [active], [f2p_enabled], [has_live_projections], [icon], [image_url], [last_five_games_enabled],
        [league_icon_id], [name], [parent_id], [parent_name], [projections_count], [rank], [show_trending],
        [projection_filters_json], [last_modified_at]
    ) VALUES (
        s.[league_id], s.[active], s.[f2p_enabled], s.[has_live_projections], s.[icon], s.[image_url], s.[last_five_games_enabled],
        s.[league_icon_id], s.[name], s.[parent_id], s.[parent_name], s.[projections_count], s.[rank], s.[show_trending],
        s.[projection_filters_json], GETDATE()
    );

    ;MERGE [dbo].[prizepicks_projection_type] AS t
    USING [dbo].[prizepicks_projection_type_stage] AS s ON t.[projection_type_id] = s.[projection_type_id]
    WHEN MATCHED AND NOT (
        (t.[name] = s.[name] OR (t.[name] IS NULL AND s.[name] IS NULL))
    ) THEN UPDATE SET
        [name] = s.[name],
        [last_modified_at] = GETDATE()
    WHEN NOT MATCHED BY TARGET THEN INSERT ([projection_type_id], [name], [last_modified_at])
    VALUES (s.[projection_type_id], s.[name], GETDATE());

    ;MERGE [dbo].[prizepicks_duration] AS t
    USING [dbo].[prizepicks_duration_stage] AS s ON t.[duration_id] = s.[duration_id]
    WHEN MATCHED AND NOT (
        (t.[name] = s.[name] OR (t.[name] IS NULL AND s.[name] IS NULL))
    ) THEN UPDATE SET
        [name] = s.[name],
        [last_modified_at] = GETDATE()
    WHEN NOT MATCHED BY TARGET THEN INSERT ([duration_id], [name], [last_modified_at])
    VALUES (s.[duration_id], s.[name], GETDATE());

    ;MERGE [dbo].[prizepicks_team] AS t
    USING [dbo].[prizepicks_team_stage] AS s ON t.[team_id] = s.[team_id]
    WHEN MATCHED AND NOT (
            (t.[abbreviation] = s.[abbreviation] OR (t.[abbreviation] IS NULL AND s.[abbreviation] IS NULL))
        AND (t.[market] = s.[market] OR (t.[market] IS NULL AND s.[market] IS NULL))
        AND (t.[name] = s.[name] OR (t.[name] IS NULL AND s.[name] IS NULL))
        AND (t.[primary_color] = s.[primary_color] OR (t.[primary_color] IS NULL AND s.[primary_color] IS NULL))
        AND (t.[secondary_color] = s.[secondary_color] OR (t.[secondary_color] IS NULL AND s.[secondary_color] IS NULL))
        AND (t.[tertiary_color] = s.[tertiary_color] OR (t.[tertiary_color] IS NULL AND s.[tertiary_color] IS NULL))
    ) THEN UPDATE SET
        [abbreviation] = s.[abbreviation],
        [market] = s.[market],
        [name] = s.[name],
        [primary_color] = s.[primary_color],
        [secondary_color] = s.[secondary_color],
        [tertiary_color] = s.[tertiary_color],
        [last_modified_at] = GETDATE()
    WHEN NOT MATCHED BY TARGET THEN INSERT (
        [team_id], [abbreviation], [market], [name], [primary_color], [secondary_color], [tertiary_color], [last_modified_at]
    ) VALUES (
        s.[team_id], s.[abbreviation], s.[market], s.[name], s.[primary_color], s.[secondary_color], s.[tertiary_color], GETDATE()
    );

    ;MERGE [dbo].[prizepicks_stat_type] AS t
    USING [dbo].[prizepicks_stat_type_stage] AS s ON t.[stat_type_id] = s.[stat_type_id]
    WHEN MATCHED AND NOT (
            (t.[name] = s.[name] OR (t.[name] IS NULL AND s.[name] IS NULL))
        AND (t.[rank] = s.[rank] OR (t.[rank] IS NULL AND s.[rank] IS NULL))
        AND (t.[lfg_ignored_leagues_json] = s.[lfg_ignored_leagues_json] OR (t.[lfg_ignored_leagues_json] IS NULL AND s.[lfg_ignored_leagues_json] IS NULL))
    ) THEN UPDATE SET
        [name] = s.[name],
        [rank] = s.[rank],
        [lfg_ignored_leagues_json] = s.[lfg_ignored_leagues_json],
        [last_modified_at] = GETDATE()
    WHEN NOT MATCHED BY TARGET THEN INSERT (
        [stat_type_id], [name], [rank], [lfg_ignored_leagues_json], [last_modified_at]
    ) VALUES (
        s.[stat_type_id], s.[name], s.[rank], s.[lfg_ignored_leagues_json], GETDATE()
    );

    ;MERGE [dbo].[prizepicks_game] AS t
    USING [dbo].[prizepicks_game_stage] AS s ON t.[game_id] = s.[game_id]
    WHEN MATCHED AND NOT (
            CHECKSUM(
                ISNULL(s.[external_game_id], NCHAR(0)), ISNULL(s.[created_at], NCHAR(0)), ISNULL(s.[end_time], NCHAR(0)),
                ISNULL(s.[start_time], NCHAR(0)), ISNULL(s.[updated_at], NCHAR(0)), ISNULL(CONVERT(tinyint, s.[is_live]), 0),
                ISNULL(s.[status], NCHAR(0)), ISNULL(s.[away_team_id], NCHAR(0)), ISNULL(s.[home_team_id], NCHAR(0)),
                ISNULL(s.[league_name], NCHAR(0)), ISNULL(s.[metadata_status], NCHAR(0)), ISNULL(s.[away_abbreviation], NCHAR(0)),
                ISNULL(s.[home_abbreviation], NCHAR(0)), ISNULL(s.[abbreviation], NCHAR(0)), ISNULL(s.[market], NCHAR(0)),
                ISNULL(s.[name], NCHAR(0)), ISNULL(s.[primary_color], NCHAR(0)), ISNULL(s.[secondary_color], NCHAR(0)),
                ISNULL(s.[tertiary_color], NCHAR(0)), ISNULL(s.[lfg_ignored_leagues], NCHAR(0)), ISNULL(CONVERT(nvarchar(20), s.[rank]), NCHAR(0)),
                ISNULL(CONVERT(tinyint, s.[combo]), 0), ISNULL(s.[display_name], NCHAR(0)), ISNULL(s.[image_url], NCHAR(0)),
                ISNULL(s.[jersey_number], NCHAR(0)), ISNULL(s.[league], NCHAR(0)), ISNULL(s.[league_id], NCHAR(0)),
                ISNULL(s.[position], NCHAR(0)), ISNULL(s.[team], NCHAR(0)), ISNULL(s.[team_name], NCHAR(0)),
                ISNULL(CONVERT(tinyint, s.[active]), 0), ISNULL(CONVERT(tinyint, s.[f2p_enabled]), 0),
                ISNULL(CONVERT(tinyint, s.[has_live_projections]), 0), ISNULL(s.[icon], NCHAR(0)),
                ISNULL(CONVERT(tinyint, s.[last_five_games_enabled]), 0), ISNULL(CONVERT(nvarchar(20), s.[league_icon_id]), NCHAR(0)),
                ISNULL(s.[parent_id], NCHAR(0)), ISNULL(s.[parent_name], NCHAR(0)), ISNULL(CONVERT(nvarchar(20), s.[projections_count]), NCHAR(0)),
                ISNULL(CONVERT(tinyint, s.[show_trending]), 0), ISNULL(s.[metadata_json], NCHAR(0))
            )
            =
            CHECKSUM(
                ISNULL(t.[external_game_id], NCHAR(0)), ISNULL(t.[created_at], NCHAR(0)), ISNULL(t.[end_time], NCHAR(0)),
                ISNULL(t.[start_time], NCHAR(0)), ISNULL(t.[updated_at], NCHAR(0)), ISNULL(CONVERT(tinyint, t.[is_live]), 0),
                ISNULL(t.[status], NCHAR(0)), ISNULL(t.[away_team_id], NCHAR(0)), ISNULL(t.[home_team_id], NCHAR(0)),
                ISNULL(t.[league_name], NCHAR(0)), ISNULL(t.[metadata_status], NCHAR(0)), ISNULL(t.[away_abbreviation], NCHAR(0)),
                ISNULL(t.[home_abbreviation], NCHAR(0)), ISNULL(t.[abbreviation], NCHAR(0)), ISNULL(t.[market], NCHAR(0)),
                ISNULL(t.[name], NCHAR(0)), ISNULL(t.[primary_color], NCHAR(0)), ISNULL(t.[secondary_color], NCHAR(0)),
                ISNULL(t.[tertiary_color], NCHAR(0)), ISNULL(t.[lfg_ignored_leagues], NCHAR(0)), ISNULL(CONVERT(nvarchar(20), t.[rank]), NCHAR(0)),
                ISNULL(CONVERT(tinyint, t.[combo]), 0), ISNULL(t.[display_name], NCHAR(0)), ISNULL(t.[image_url], NCHAR(0)),
                ISNULL(t.[jersey_number], NCHAR(0)), ISNULL(t.[league], NCHAR(0)), ISNULL(t.[league_id], NCHAR(0)),
                ISNULL(t.[position], NCHAR(0)), ISNULL(t.[team], NCHAR(0)), ISNULL(t.[team_name], NCHAR(0)),
                ISNULL(CONVERT(tinyint, t.[active]), 0), ISNULL(CONVERT(tinyint, t.[f2p_enabled]), 0),
                ISNULL(CONVERT(tinyint, t.[has_live_projections]), 0), ISNULL(t.[icon], NCHAR(0)),
                ISNULL(CONVERT(tinyint, t.[last_five_games_enabled]), 0), ISNULL(CONVERT(nvarchar(20), t.[league_icon_id]), NCHAR(0)),
                ISNULL(t.[parent_id], NCHAR(0)), ISNULL(t.[parent_name], NCHAR(0)), ISNULL(CONVERT(nvarchar(20), t.[projections_count]), NCHAR(0)),
                ISNULL(CONVERT(tinyint, t.[show_trending]), 0), ISNULL(t.[metadata_json], NCHAR(0))
            )
    ) THEN UPDATE SET
        [external_game_id] = s.[external_game_id], [created_at] = s.[created_at], [end_time] = s.[end_time], [start_time] = s.[start_time],
        [updated_at] = s.[updated_at], [is_live] = s.[is_live], [status] = s.[status], [away_team_id] = s.[away_team_id], [home_team_id] = s.[home_team_id],
        [league_name] = s.[league_name], [metadata_status] = s.[metadata_status], [away_abbreviation] = s.[away_abbreviation], [home_abbreviation] = s.[home_abbreviation],
        [abbreviation] = s.[abbreviation], [market] = s.[market], [name] = s.[name], [primary_color] = s.[primary_color], [secondary_color] = s.[secondary_color],
        [tertiary_color] = s.[tertiary_color], [lfg_ignored_leagues] = s.[lfg_ignored_leagues], [rank] = s.[rank], [combo] = s.[combo],
        [display_name] = s.[display_name], [image_url] = s.[image_url], [jersey_number] = s.[jersey_number], [league] = s.[league], [league_id] = s.[league_id],
        [position] = s.[position], [team] = s.[team], [team_name] = s.[team_name], [active] = s.[active], [f2p_enabled] = s.[f2p_enabled],
        [has_live_projections] = s.[has_live_projections], [icon] = s.[icon], [last_five_games_enabled] = s.[last_five_games_enabled],
        [league_icon_id] = s.[league_icon_id], [parent_id] = s.[parent_id], [parent_name] = s.[parent_name], [projections_count] = s.[projections_count],
        [show_trending] = s.[show_trending], [metadata_json] = s.[metadata_json], [last_modified_at] = GETDATE()
    WHEN NOT MATCHED BY TARGET THEN INSERT (
        [game_id], [external_game_id], [created_at], [end_time], [start_time], [updated_at], [is_live], [status], [away_team_id], [home_team_id],
        [league_name], [metadata_status], [away_abbreviation], [home_abbreviation], [abbreviation], [market], [name], [primary_color], [secondary_color],
        [tertiary_color], [lfg_ignored_leagues], [rank], [combo], [display_name], [image_url], [jersey_number], [league], [league_id], [position], [team],
        [team_name], [active], [f2p_enabled], [has_live_projections], [icon], [last_five_games_enabled], [league_icon_id], [parent_id], [parent_name],
        [projections_count], [show_trending], [metadata_json], [last_modified_at]
    ) VALUES (
        s.[game_id], s.[external_game_id], s.[created_at], s.[end_time], s.[start_time], s.[updated_at], s.[is_live], s.[status], s.[away_team_id], s.[home_team_id],
        s.[league_name], s.[metadata_status], s.[away_abbreviation], s.[home_abbreviation], s.[abbreviation], s.[market], s.[name], s.[primary_color], s.[secondary_color],
        s.[tertiary_color], s.[lfg_ignored_leagues], s.[rank], s.[combo], s.[display_name], s.[image_url], s.[jersey_number], s.[league], s.[league_id], s.[position], s.[team],
        s.[team_name], s.[active], s.[f2p_enabled], s.[has_live_projections], s.[icon], s.[last_five_games_enabled], s.[league_icon_id], s.[parent_id], s.[parent_name],
        s.[projections_count], s.[show_trending], s.[metadata_json], GETDATE()
    );

    IF EXISTS (SELECT 1 FROM sys.tables WHERE name = 'prizepicks_player_stage' AND schema_id = SCHEMA_ID('dbo'))
       AND EXISTS (SELECT 1 FROM sys.tables WHERE name = 'prizepicks_player' AND schema_id = SCHEMA_ID('dbo'))
    BEGIN
        ;MERGE [dbo].[prizepicks_player] AS t
        USING [dbo].[prizepicks_player_stage] AS s ON t.[player_id] = s.[player_id]
        WHEN MATCHED AND NOT (
                CHECKSUM(
                    ISNULL(CONVERT(tinyint, s.[combo]), 0), ISNULL(s.[display_name], NCHAR(0)), ISNULL(s.[image_url], NCHAR(0)),
                    ISNULL(s.[jersey_number], NCHAR(0)), ISNULL(s.[league], NCHAR(0)), ISNULL(s.[market], NCHAR(0)), ISNULL(s.[name], NCHAR(0)),
                    ISNULL(s.[position], NCHAR(0)), ISNULL(s.[team], NCHAR(0)), ISNULL(s.[team_name], NCHAR(0)), ISNULL(s.[league_id], NCHAR(0)),
                    ISNULL(s.[team_id], NCHAR(0)), ISNULL(s.[ppid], NCHAR(0))
                )
                =
                CHECKSUM(
                    ISNULL(CONVERT(tinyint, t.[combo]), 0), ISNULL(t.[display_name], NCHAR(0)), ISNULL(t.[image_url], NCHAR(0)),
                    ISNULL(t.[jersey_number], NCHAR(0)), ISNULL(t.[league], NCHAR(0)), ISNULL(t.[market], NCHAR(0)), ISNULL(t.[name], NCHAR(0)),
                    ISNULL(t.[position], NCHAR(0)), ISNULL(t.[team], NCHAR(0)), ISNULL(t.[team_name], NCHAR(0)), ISNULL(t.[league_id], NCHAR(0)),
                    ISNULL(t.[team_id], NCHAR(0)), ISNULL(t.[ppid], NCHAR(0))
                )
        ) THEN UPDATE SET
            [combo] = s.[combo], [display_name] = s.[display_name], [image_url] = s.[image_url], [jersey_number] = s.[jersey_number],
            [league] = s.[league], [market] = s.[market], [name] = s.[name], [position] = s.[position], [team] = s.[team], [team_name] = s.[team_name],
            [league_id] = s.[league_id], [team_id] = s.[team_id], [ppid] = s.[ppid], [last_modified_at] = GETDATE()
        WHEN NOT MATCHED BY TARGET THEN INSERT (
            [player_id], [combo], [display_name], [image_url], [jersey_number], [league], [market], [name], [position], [team], [team_name],
            [league_id], [team_id], [ppid], [last_modified_at]
        ) VALUES (
            s.[player_id], s.[combo], s.[display_name], s.[image_url], s.[jersey_number], s.[league], s.[market], s.[name], s.[position], s.[team], s.[team_name],
            s.[league_id], s.[team_id], s.[ppid], GETDATE()
        );
    END
END
GO
