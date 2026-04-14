-- Unified sportsbook game dimension (cross-book). Links home/away teams and book-specific game ids.
USE [Props]
GO

IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'sportsbook_game' AND schema_id = SCHEMA_ID('dbo'))
BEGIN
CREATE TABLE [dbo].[sportsbook_game](
    [sportsbook_game_id] [bigint] IDENTITY(1,1) NOT NULL,
    [canonical_league_id] [int] NULL,
    [start_time] [datetime2](3) NULL,
    [home_sportsbook_team_id] [bigint] NULL,
    [away_sportsbook_team_id] [bigint] NULL,
    [home_team_abbrev] [nvarchar](20) NULL,
    [away_team_abbrev] [nvarchar](20) NULL,
    [event_name] [nvarchar](200) NULL,
    [prizepicks_game_id] [nvarchar](20) NULL,
    [underdog_game_id] [nvarchar](64) NULL,
    [parlay_play_match_id] [int] NULL,
    [created_at] [datetime2](7) NOT NULL
        CONSTRAINT [DF_sportsbook_game_created_at]
        DEFAULT (CONVERT(datetime2(7), SYSUTCDATETIME() AT TIME ZONE 'UTC' AT TIME ZONE 'Central Standard Time')),
    [last_modified_at] [datetime2](7) NOT NULL
        CONSTRAINT [DF_sportsbook_game_last_modified_at]
        DEFAULT (CONVERT(datetime2(7), SYSUTCDATETIME() AT TIME ZONE 'UTC' AT TIME ZONE 'Central Standard Time')),
    CONSTRAINT [PK_sportsbook_game] PRIMARY KEY CLUSTERED ([sportsbook_game_id] ASC)
) ON [PRIMARY];
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'UQ_sportsbook_game_pp'
      AND object_id = OBJECT_ID('dbo.sportsbook_game')
)
BEGIN
    CREATE UNIQUE NONCLUSTERED INDEX [UQ_sportsbook_game_pp]
        ON [dbo].[sportsbook_game]([prizepicks_game_id])
        WHERE [prizepicks_game_id] IS NOT NULL;
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'UQ_sportsbook_game_ud'
      AND object_id = OBJECT_ID('dbo.sportsbook_game')
)
BEGIN
    CREATE UNIQUE NONCLUSTERED INDEX [UQ_sportsbook_game_ud]
        ON [dbo].[sportsbook_game]([underdog_game_id])
        WHERE [underdog_game_id] IS NOT NULL;
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'UQ_sportsbook_game_parlay'
      AND object_id = OBJECT_ID('dbo.sportsbook_game')
)
BEGIN
    CREATE UNIQUE NONCLUSTERED INDEX [UQ_sportsbook_game_parlay]
        ON [dbo].[sportsbook_game]([parlay_play_match_id])
        WHERE [parlay_play_match_id] IS NOT NULL;
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_sportsbook_game_date_league_teams'
      AND object_id = OBJECT_ID('dbo.sportsbook_game')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_sportsbook_game_date_league_teams]
        ON [dbo].[sportsbook_game]([canonical_league_id], [start_time], [home_team_abbrev], [away_team_abbrev])
        WHERE [start_time] IS NOT NULL;
END
GO
