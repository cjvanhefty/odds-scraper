-- Unified sportsbook player dimension (cross-book). Holds book-specific player ids and canonical attributes.
USE [Props]
GO

IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'sportsbook_player' AND schema_id = SCHEMA_ID('dbo'))
BEGIN
CREATE TABLE [dbo].[sportsbook_player](
    [sportsbook_player_id] [bigint] IDENTITY(1,1) NOT NULL,
    [canonical_league_id] [int] NULL,
    [sportsbook_sport_id] [bigint] NULL,
    [sportsbook_league_id] [bigint] NULL,
    [sportsbook_team_id] [bigint] NULL,
    [display_name] [nvarchar](255) NOT NULL,
    [first_name] [nvarchar](100) NULL,
    [last_name] [nvarchar](100) NULL,
    [jersey_number] [nvarchar](20) NULL,
    [position] [nvarchar](50) NULL,
    [team_abbrev] [nvarchar](20) NULL,
    [team_name] [nvarchar](150) NULL,
    [prizepicks_player_id] [nvarchar](20) NULL,
    [underdog_player_id] [nvarchar](64) NULL,
    [parlay_play_player_id] [int] NULL,
    [created_at] [datetime2](7) NOT NULL
        CONSTRAINT [DF_sportsbook_player_created_at]
        DEFAULT (CONVERT(datetime2(7), SYSUTCDATETIME() AT TIME ZONE 'UTC' AT TIME ZONE 'Central Standard Time')),
    [last_modified_at] [datetime2](7) NOT NULL
        CONSTRAINT [DF_sportsbook_player_last_modified_at]
        DEFAULT (CONVERT(datetime2(7), SYSUTCDATETIME() AT TIME ZONE 'UTC' AT TIME ZONE 'Central Standard Time')),
    CONSTRAINT [PK_sportsbook_player] PRIMARY KEY CLUSTERED ([sportsbook_player_id] ASC)
) ON [PRIMARY];
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'UQ_sportsbook_player_pp'
      AND object_id = OBJECT_ID('dbo.sportsbook_player')
)
BEGIN
    CREATE UNIQUE NONCLUSTERED INDEX [UQ_sportsbook_player_pp]
        ON [dbo].[sportsbook_player]([prizepicks_player_id])
        WHERE [prizepicks_player_id] IS NOT NULL;
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'UQ_sportsbook_player_ud'
      AND object_id = OBJECT_ID('dbo.sportsbook_player')
)
BEGIN
    CREATE UNIQUE NONCLUSTERED INDEX [UQ_sportsbook_player_ud]
        ON [dbo].[sportsbook_player]([underdog_player_id])
        WHERE [underdog_player_id] IS NOT NULL;
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'UQ_sportsbook_player_parlay'
      AND object_id = OBJECT_ID('dbo.sportsbook_player')
)
BEGIN
    CREATE UNIQUE NONCLUSTERED INDEX [UQ_sportsbook_player_parlay]
        ON [dbo].[sportsbook_player]([parlay_play_player_id])
        WHERE [parlay_play_player_id] IS NOT NULL;
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_sportsbook_player_name_league'
      AND object_id = OBJECT_ID('dbo.sportsbook_player')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_sportsbook_player_name_league]
        ON [dbo].[sportsbook_player]([canonical_league_id], [display_name])
        INCLUDE ([team_abbrev], [team_name])
        WHERE [display_name] IS NOT NULL;
END
GO
