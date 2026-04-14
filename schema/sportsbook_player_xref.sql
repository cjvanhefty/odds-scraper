-- Unified sportsbook player ID mapping (allows multiple external IDs per book to map to one canonical player).
USE [Props]
GO

IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'sportsbook_player_xref' AND schema_id = SCHEMA_ID('dbo'))
BEGIN
CREATE TABLE [dbo].[sportsbook_player_xref](
    [sportsbook] [nvarchar](30) NOT NULL,
    [external_player_id] [nvarchar](64) NOT NULL,
    [sportsbook_player_id] [bigint] NOT NULL,
    [created_at] [datetime2](7) NOT NULL
        CONSTRAINT [DF_sportsbook_player_xref_created_at]
        DEFAULT (CONVERT(datetime2(7), SYSUTCDATETIME() AT TIME ZONE 'UTC' AT TIME ZONE 'Central Standard Time')),
    [last_modified_at] [datetime2](7) NOT NULL
        CONSTRAINT [DF_sportsbook_player_xref_last_modified_at]
        DEFAULT (CONVERT(datetime2(7), SYSUTCDATETIME() AT TIME ZONE 'UTC' AT TIME ZONE 'Central Standard Time')),
    CONSTRAINT [PK_sportsbook_player_xref] PRIMARY KEY CLUSTERED ([sportsbook] ASC, [external_player_id] ASC),
    CONSTRAINT [FK_sportsbook_player_xref_player]
        FOREIGN KEY ([sportsbook_player_id]) REFERENCES [dbo].[sportsbook_player] ([sportsbook_player_id])
) ON [PRIMARY];
END
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.indexes
    WHERE name = 'IX_sportsbook_player_xref_player'
      AND object_id = OBJECT_ID('dbo.sportsbook_player_xref')
)
BEGIN
    CREATE NONCLUSTERED INDEX [IX_sportsbook_player_xref_player]
        ON [dbo].[sportsbook_player_xref]([sportsbook_player_id])
        INCLUDE ([sportsbook], [external_player_id]);
END
GO
