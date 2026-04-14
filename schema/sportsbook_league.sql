-- Unified sportsbook league dimension (cross-book). Aligns leagues by canonical_league_id mapping only.
USE [Props]
GO

IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'sportsbook_league' AND schema_id = SCHEMA_ID('dbo'))
BEGIN
CREATE TABLE [dbo].[sportsbook_league](
    [sportsbook_league_id] [bigint] IDENTITY(1,1) NOT NULL,
    [canonical_league_id] [int] NULL,
    [prizepicks_league_id] [nvarchar](20) NULL,
    [underdog_sport_id] [int] NULL,
    [parlay_play_league_id] [int] NULL,
    [sportsbook_sport_id] [bigint] NULL,
    [display_name] [nvarchar](150) NULL,
    [created_at] [datetime2](7) NOT NULL
        CONSTRAINT [DF_sportsbook_league_created_at]
        DEFAULT (CONVERT(datetime2(7), SYSUTCDATETIME() AT TIME ZONE 'UTC' AT TIME ZONE 'Central Standard Time')),
    [last_modified_at] [datetime2](7) NOT NULL
        CONSTRAINT [DF_sportsbook_league_last_modified_at]
        DEFAULT (CONVERT(datetime2(7), SYSUTCDATETIME() AT TIME ZONE 'UTC' AT TIME ZONE 'Central Standard Time')),
    CONSTRAINT [PK_sportsbook_league] PRIMARY KEY CLUSTERED ([sportsbook_league_id] ASC)
) ON [PRIMARY];
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'UQ_sportsbook_league_canonical_league_id'
      AND object_id = OBJECT_ID('dbo.sportsbook_league')
)
BEGIN
    CREATE UNIQUE NONCLUSTERED INDEX [UQ_sportsbook_league_canonical_league_id]
        ON [dbo].[sportsbook_league]([canonical_league_id])
        WHERE [canonical_league_id] IS NOT NULL;
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'UQ_sportsbook_league_prizepicks_league_id'
      AND object_id = OBJECT_ID('dbo.sportsbook_league')
)
BEGIN
    CREATE UNIQUE NONCLUSTERED INDEX [UQ_sportsbook_league_prizepicks_league_id]
        ON [dbo].[sportsbook_league]([prizepicks_league_id])
        WHERE [prizepicks_league_id] IS NOT NULL;
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'UQ_sportsbook_league_parlay_play_league_id'
      AND object_id = OBJECT_ID('dbo.sportsbook_league')
)
BEGIN
    CREATE UNIQUE NONCLUSTERED INDEX [UQ_sportsbook_league_parlay_play_league_id]
        ON [dbo].[sportsbook_league]([parlay_play_league_id])
        WHERE [parlay_play_league_id] IS NOT NULL;
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'UQ_sportsbook_league_underdog_sport_id'
      AND object_id = OBJECT_ID('dbo.sportsbook_league')
)
BEGIN
    CREATE UNIQUE NONCLUSTERED INDEX [UQ_sportsbook_league_underdog_sport_id]
        ON [dbo].[sportsbook_league]([underdog_sport_id])
        WHERE [underdog_sport_id] IS NOT NULL;
END
GO
