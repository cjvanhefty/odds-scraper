-- Link [player] to parlay_play_player for cross-site projection joins (PrizePicks, Underdog, Parlay Play by player + stat).
-- Run after parlay_play_player exists. Backfill via full_name = display_first_last (or your ETL).
USE [Props]
GO

IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'player')
    RAISERROR('Table [dbo].[player] does not exist. Create it first.', 16, 1);
GO

IF EXISTS (SELECT * FROM sys.tables WHERE name = 'player')
   AND NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dbo].[player]') AND name = N'parlay_play_player_id')
BEGIN
    ALTER TABLE [dbo].[player]
    ADD [parlay_play_player_id] [int] NULL;

    IF EXISTS (SELECT * FROM sys.tables WHERE name = 'parlay_play_player')
    BEGIN
        ALTER TABLE [dbo].[player]
        ADD CONSTRAINT [FK_player_parlay_play_player]
        FOREIGN KEY ([parlay_play_player_id]) REFERENCES [dbo].[parlay_play_player] ([id]);

        CREATE NONCLUSTERED INDEX [IX_player_parlay_play_player_id]
        ON [dbo].[player] ([parlay_play_player_id])
        WHERE [parlay_play_player_id] IS NOT NULL;
    END
END
GO

-- Optional: backfill parlay_play_player_id by matching full_name to player.display_first_last.
/*
UPDATE pl
SET pl.parlay_play_player_id = pp.id
FROM [dbo].[player] pl
INNER JOIN [dbo].[parlay_play_player] pp
    ON LTRIM(RTRIM(pp.full_name)) = LTRIM(RTRIM(pl.display_first_last))
WHERE pl.parlay_play_player_id IS NULL;
*/
