-- Link prizepicks_player to player (NBA) by adding nba_player_id on prizepicks_player.
-- One column, nullable FK. No junction table: many PrizePicks rows can point to one NBA player.
-- Fill nba_player_id via name match (display_name = display_first_last) or your ETL.

USE [Props]
GO

IF NOT EXISTS (
    SELECT 1 FROM sys.columns
    WHERE object_id = OBJECT_ID(N'[dbo].[prizepicks_player]') AND name = N'nba_player_id'
)
BEGIN
    ALTER TABLE [dbo].[prizepicks_player]
    ADD [nba_player_id] [bigint] NULL;

    ALTER TABLE [dbo].[prizepicks_player]
    ADD CONSTRAINT [FK_prizepicks_player_player]
    FOREIGN KEY ([nba_player_id]) REFERENCES [dbo].[player] ([player_id]);

    -- Optional: index for joins and lookups
    CREATE NONCLUSTERED INDEX [IX_prizepicks_player_nba_player_id]
    ON [dbo].[prizepicks_player] ([nba_player_id])
    WHERE [nba_player_id] IS NOT NULL;
END
GO

-- Optional: backfill nba_player_id by matching display_name to player.display_first_last.
-- Run after adding the column; then maintain via ETL when new prizepicks players are added.
/*
UPDATE pp
SET pp.nba_player_id = pl.player_id
FROM [dbo].[prizepicks_player] pp
INNER JOIN [dbo].[player] pl
    ON LTRIM(RTRIM(pp.display_name)) = LTRIM(RTRIM(pl.display_first_last))
WHERE pp.nba_player_id IS NULL
  AND pp.combo = 0;
*/
