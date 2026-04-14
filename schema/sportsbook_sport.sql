-- Unified sportsbook sport dimension (cross-book). Links to Parlay sport_id and Underdog sport_id.
USE [Props]
GO

IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'sportsbook_sport' AND schema_id = SCHEMA_ID('dbo'))
BEGIN
CREATE TABLE [dbo].[sportsbook_sport](
    [sportsbook_sport_id] [bigint] IDENTITY(1,1) NOT NULL,
    [underdog_sport_id] [int] NULL,
    [parlay_play_sport_id] [int] NULL,
    [display_name] [nvarchar](100) NULL,
    [created_at] [datetime2](7) NOT NULL
        CONSTRAINT [DF_sportsbook_sport_created_at]
        DEFAULT (CONVERT(datetime2(7), SYSUTCDATETIME() AT TIME ZONE 'UTC' AT TIME ZONE 'Central Standard Time')),
    [last_modified_at] [datetime2](7) NOT NULL
        CONSTRAINT [DF_sportsbook_sport_last_modified_at]
        DEFAULT (CONVERT(datetime2(7), SYSUTCDATETIME() AT TIME ZONE 'UTC' AT TIME ZONE 'Central Standard Time')),
    CONSTRAINT [PK_sportsbook_sport] PRIMARY KEY CLUSTERED ([sportsbook_sport_id] ASC)
) ON [PRIMARY];
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'UQ_sportsbook_sport_underdog_sport_id'
      AND object_id = OBJECT_ID('dbo.sportsbook_sport')
)
BEGIN
    CREATE UNIQUE NONCLUSTERED INDEX [UQ_sportsbook_sport_underdog_sport_id]
        ON [dbo].[sportsbook_sport]([underdog_sport_id])
        WHERE [underdog_sport_id] IS NOT NULL;
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'UQ_sportsbook_sport_parlay_play_sport_id'
      AND object_id = OBJECT_ID('dbo.sportsbook_sport')
)
BEGIN
    CREATE UNIQUE NONCLUSTERED INDEX [UQ_sportsbook_sport_parlay_play_sport_id]
        ON [dbo].[sportsbook_sport]([parlay_play_sport_id])
        WHERE [parlay_play_sport_id] IS NOT NULL;
END
GO
