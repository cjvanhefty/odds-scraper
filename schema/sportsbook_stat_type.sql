-- Unified sportsbook stat type dimension (cross-book). Cross-book identity is normalized_stat_key (map-driven).
USE [Props]
GO

IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'sportsbook_stat_type' AND schema_id = SCHEMA_ID('dbo'))
BEGIN
CREATE TABLE [dbo].[sportsbook_stat_type](
    [sportsbook_stat_type_id] [bigint] IDENTITY(1,1) NOT NULL,
    [canonical_league_id] [int] NULL,
    [normalized_stat_key] [nvarchar](120) NULL,
    [stat_display_name] [nvarchar](200) NULL,
    [prizepicks_stat_type_id] [nvarchar](20) NULL,
    [underdog_pickem_stat_id] [nvarchar](36) NULL,
    [parlay_play_challenge_option] [nvarchar](50) NULL,
    [created_at] [datetime2](7) NOT NULL
        CONSTRAINT [DF_sportsbook_stat_type_created_at]
        DEFAULT (CONVERT(datetime2(7), SYSUTCDATETIME() AT TIME ZONE 'UTC' AT TIME ZONE 'Central Standard Time')),
    [last_modified_at] [datetime2](7) NOT NULL
        CONSTRAINT [DF_sportsbook_stat_type_last_modified_at]
        DEFAULT (CONVERT(datetime2(7), SYSUTCDATETIME() AT TIME ZONE 'UTC' AT TIME ZONE 'Central Standard Time')),
    CONSTRAINT [PK_sportsbook_stat_type] PRIMARY KEY CLUSTERED ([sportsbook_stat_type_id] ASC)
) ON [PRIMARY];
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'UQ_sportsbook_stat_type_pp'
      AND object_id = OBJECT_ID('dbo.sportsbook_stat_type')
)
BEGIN
    CREATE UNIQUE NONCLUSTERED INDEX [UQ_sportsbook_stat_type_pp]
        ON [dbo].[sportsbook_stat_type]([prizepicks_stat_type_id])
        WHERE [prizepicks_stat_type_id] IS NOT NULL;
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'UQ_sportsbook_stat_type_ud'
      AND object_id = OBJECT_ID('dbo.sportsbook_stat_type')
)
BEGIN
    CREATE UNIQUE NONCLUSTERED INDEX [UQ_sportsbook_stat_type_ud]
        ON [dbo].[sportsbook_stat_type]([underdog_pickem_stat_id])
        WHERE [underdog_pickem_stat_id] IS NOT NULL;
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'UQ_sportsbook_stat_type_parlay'
      AND object_id = OBJECT_ID('dbo.sportsbook_stat_type')
)
BEGIN
    CREATE UNIQUE NONCLUSTERED INDEX [UQ_sportsbook_stat_type_parlay]
        ON [dbo].[sportsbook_stat_type]([parlay_play_challenge_option])
        WHERE [parlay_play_challenge_option] IS NOT NULL;
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_sportsbook_stat_type_norm_key'
      AND object_id = OBJECT_ID('dbo.sportsbook_stat_type')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_sportsbook_stat_type_norm_key]
        ON [dbo].[sportsbook_stat_type]([canonical_league_id], [normalized_stat_key])
        WHERE [normalized_stat_key] IS NOT NULL;
END
GO
