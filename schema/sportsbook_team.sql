-- Unified sportsbook team dimension (cross-book). Heuristic linking uses abbreviation + canonical_league_id.
USE [Props]
GO

IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'sportsbook_team' AND schema_id = SCHEMA_ID('dbo'))
BEGIN
CREATE TABLE [dbo].[sportsbook_team](
    [sportsbook_team_id] [bigint] IDENTITY(1,1) NOT NULL,
    [canonical_league_id] [int] NULL,
    [sportsbook_league_id] [bigint] NULL,
    [prizepicks_team_id] [nvarchar](20) NULL,
    [underdog_team_id] [nvarchar](50) NULL,
    [parlay_play_team_id] [int] NULL,
    [abbreviation] [nvarchar](20) NULL,
    [full_name] [nvarchar](150) NULL,
    [created_at] [datetime2](7) NOT NULL
        CONSTRAINT [DF_sportsbook_team_created_at]
        DEFAULT (CONVERT(datetime2(7), SYSUTCDATETIME() AT TIME ZONE 'UTC' AT TIME ZONE 'Central Standard Time')),
    [last_modified_at] [datetime2](7) NOT NULL
        CONSTRAINT [DF_sportsbook_team_last_modified_at]
        DEFAULT (CONVERT(datetime2(7), SYSUTCDATETIME() AT TIME ZONE 'UTC' AT TIME ZONE 'Central Standard Time')),
    CONSTRAINT [PK_sportsbook_team] PRIMARY KEY CLUSTERED ([sportsbook_team_id] ASC)
) ON [PRIMARY];
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'UQ_sportsbook_team_prizepicks_team_id'
      AND object_id = OBJECT_ID('dbo.sportsbook_team')
)
BEGIN
    CREATE UNIQUE NONCLUSTERED INDEX [UQ_sportsbook_team_prizepicks_team_id]
        ON [dbo].[sportsbook_team]([prizepicks_team_id])
        WHERE [prizepicks_team_id] IS NOT NULL;
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'UQ_sportsbook_team_underdog_team_id'
      AND object_id = OBJECT_ID('dbo.sportsbook_team')
)
BEGIN
    CREATE UNIQUE NONCLUSTERED INDEX [UQ_sportsbook_team_underdog_team_id]
        ON [dbo].[sportsbook_team]([underdog_team_id])
        WHERE [underdog_team_id] IS NOT NULL;
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'UQ_sportsbook_team_parlay_play_team_id'
      AND object_id = OBJECT_ID('dbo.sportsbook_team')
)
BEGIN
    CREATE UNIQUE NONCLUSTERED INDEX [UQ_sportsbook_team_parlay_play_team_id]
        ON [dbo].[sportsbook_team]([parlay_play_team_id])
        WHERE [parlay_play_team_id] IS NOT NULL;
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_sportsbook_team_abbrev_league'
      AND object_id = OBJECT_ID('dbo.sportsbook_team')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_sportsbook_team_abbrev_league]
        ON [dbo].[sportsbook_team]([canonical_league_id], [abbreviation])
        WHERE [abbreviation] IS NOT NULL;
END
GO
