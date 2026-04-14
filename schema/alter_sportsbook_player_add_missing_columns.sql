-- Backfill columns on dbo.sportsbook_player when the table predates full dimension DDL.
-- CREATE TABLE in sportsbook_player.sql only runs for new databases; existing tables need ALTERs.
USE [Props]
GO

SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER ON
GO

IF OBJECT_ID(N'[dbo].[sportsbook_player]', N'U') IS NOT NULL
BEGIN
    IF COL_LENGTH(N'dbo.sportsbook_player', N'canonical_league_id') IS NULL
        ALTER TABLE [dbo].[sportsbook_player] ADD [canonical_league_id] [int] NULL;

    IF COL_LENGTH(N'dbo.sportsbook_player', N'sportsbook_sport_id') IS NULL
        ALTER TABLE [dbo].[sportsbook_player] ADD [sportsbook_sport_id] [bigint] NULL;

    IF COL_LENGTH(N'dbo.sportsbook_player', N'sportsbook_league_id') IS NULL
        ALTER TABLE [dbo].[sportsbook_player] ADD [sportsbook_league_id] [bigint] NULL;

    IF COL_LENGTH(N'dbo.sportsbook_player', N'sportsbook_team_id') IS NULL
        ALTER TABLE [dbo].[sportsbook_player] ADD [sportsbook_team_id] [bigint] NULL;

    IF COL_LENGTH(N'dbo.sportsbook_player', N'first_name') IS NULL
        ALTER TABLE [dbo].[sportsbook_player] ADD [first_name] [nvarchar](100) NULL;

    IF COL_LENGTH(N'dbo.sportsbook_player', N'last_name') IS NULL
        ALTER TABLE [dbo].[sportsbook_player] ADD [last_name] [nvarchar](100) NULL;

    IF COL_LENGTH(N'dbo.sportsbook_player', N'jersey_number') IS NULL
        ALTER TABLE [dbo].[sportsbook_player] ADD [jersey_number] [nvarchar](20) NULL;

    IF COL_LENGTH(N'dbo.sportsbook_player', N'position') IS NULL
        ALTER TABLE [dbo].[sportsbook_player] ADD [position] [nvarchar](50) NULL;

    IF COL_LENGTH(N'dbo.sportsbook_player', N'team_abbrev') IS NULL
        ALTER TABLE [dbo].[sportsbook_player] ADD [team_abbrev] [nvarchar](20) NULL;

    IF COL_LENGTH(N'dbo.sportsbook_player', N'team_name') IS NULL
        ALTER TABLE [dbo].[sportsbook_player] ADD [team_name] [nvarchar](150) NULL;

    IF COL_LENGTH(N'dbo.sportsbook_player', N'prizepicks_player_id') IS NULL
        ALTER TABLE [dbo].[sportsbook_player] ADD [prizepicks_player_id] [nvarchar](20) NULL;

    IF COL_LENGTH(N'dbo.sportsbook_player', N'underdog_player_id') IS NULL
        ALTER TABLE [dbo].[sportsbook_player] ADD [underdog_player_id] [nvarchar](64) NULL;

    IF COL_LENGTH(N'dbo.sportsbook_player', N'parlay_play_player_id') IS NULL
        ALTER TABLE [dbo].[sportsbook_player] ADD [parlay_play_player_id] [int] NULL;
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = N'UQ_sportsbook_player_pp'
      AND object_id = OBJECT_ID(N'dbo.sportsbook_player')
)
   AND COL_LENGTH(N'dbo.sportsbook_player', N'prizepicks_player_id') IS NOT NULL
BEGIN
    CREATE UNIQUE NONCLUSTERED INDEX [UQ_sportsbook_player_pp]
        ON [dbo].[sportsbook_player]([prizepicks_player_id])
        WHERE [prizepicks_player_id] IS NOT NULL;
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = N'UQ_sportsbook_player_ud'
      AND object_id = OBJECT_ID(N'dbo.sportsbook_player')
)
   AND COL_LENGTH(N'dbo.sportsbook_player', N'underdog_player_id') IS NOT NULL
BEGIN
    CREATE UNIQUE NONCLUSTERED INDEX [UQ_sportsbook_player_ud]
        ON [dbo].[sportsbook_player]([underdog_player_id])
        WHERE [underdog_player_id] IS NOT NULL;
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = N'UQ_sportsbook_player_parlay'
      AND object_id = OBJECT_ID(N'dbo.sportsbook_player')
)
   AND COL_LENGTH(N'dbo.sportsbook_player', N'parlay_play_player_id') IS NOT NULL
BEGIN
    CREATE UNIQUE NONCLUSTERED INDEX [UQ_sportsbook_player_parlay]
        ON [dbo].[sportsbook_player]([parlay_play_player_id])
        WHERE [parlay_play_player_id] IS NOT NULL;
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = N'IX_sportsbook_player_name_league'
      AND object_id = OBJECT_ID(N'dbo.sportsbook_player')
)
   AND COL_LENGTH(N'dbo.sportsbook_player', N'canonical_league_id') IS NOT NULL
   AND COL_LENGTH(N'dbo.sportsbook_player', N'display_name') IS NOT NULL
   AND COL_LENGTH(N'dbo.sportsbook_player', N'team_abbrev') IS NOT NULL
   AND COL_LENGTH(N'dbo.sportsbook_player', N'team_name') IS NOT NULL
BEGIN
    CREATE NONCLUSTERED INDEX [IX_sportsbook_player_name_league]
        ON [dbo].[sportsbook_player]([canonical_league_id], [display_name])
        INCLUDE ([team_abbrev], [team_name])
        WHERE [display_name] IS NOT NULL;
END
GO
