-- Materialized table: last N games per player (from player_stat) for fast last-five / streak reads.
-- Refresh via: EXEC [dbo].[usp_refresh_player_stat_last_n] @max_games_per_player = 20;
-- API reads from this table in one query instead of ROW_NUMBER() over full player_stat.
USE [Props]
GO

IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'player_stat_last_n')
BEGIN
CREATE TABLE [dbo].[player_stat_last_n](
	[player_id] [bigint] NOT NULL,
	[game_date] [varchar](20) NOT NULL,
	[matchup] [varchar](20) NOT NULL,
	[pts] [smallint] NULL,
	[reb] [smallint] NULL,
	[ast] [smallint] NULL,
	[stl] [smallint] NULL,
	[blk] [smallint] NULL,
	[tov] [smallint] NULL,
	[dreb] [smallint] NULL,
	[oreb] [smallint] NULL,
	[fg3m] [smallint] NULL,
	[fg3a] [smallint] NULL,
	[fgm] [smallint] NULL,
	[fga] [smallint] NULL,
	[pf] [smallint] NULL,
	[ftm] [smallint] NULL,
	[fta] [smallint] NULL,
	[rn] [smallint] NOT NULL,
	CONSTRAINT [PK_player_stat_last_n] PRIMARY KEY CLUSTERED ([player_id] ASC, [rn] ASC)
) ON [PRIMARY]
END
GO

IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = N'IX_player_stat_last_n_player_date' AND object_id = OBJECT_ID(N'[dbo].[player_stat_last_n]'))
CREATE NONCLUSTERED INDEX [IX_player_stat_last_n_player_date] ON [dbo].[player_stat_last_n] ([player_id], [game_date] DESC);
GO

-- Refresh procedure: repopulate from player_stat (last @max_games_per_player games per player).
-- Run after nba_scraper / player_stat updates: EXEC [dbo].[usp_refresh_player_stat_last_n] @max_games_per_player = 20;
IF OBJECT_ID(N'[dbo].[usp_refresh_player_stat_last_n]', N'P') IS NOT NULL
    DROP PROCEDURE [dbo].[usp_refresh_player_stat_last_n];
GO

CREATE PROCEDURE [dbo].[usp_refresh_player_stat_last_n]
    @max_games_per_player smallint = 20
AS
BEGIN
    SET NOCOUNT ON;
    TRUNCATE TABLE [dbo].[player_stat_last_n];
    ;WITH ranked AS (
        SELECT
            player_id,
            CAST(game_date AS VARCHAR(20)) AS game_date,
            ISNULL(matchup, '') AS matchup,
            pts, reb, ast, stl, blk, tov, dreb, oreb,
            fg3m, fg3a, fgm, fga, pf, ftm, fta,
            ROW_NUMBER() OVER (PARTITION BY player_id ORDER BY game_date DESC) AS rn
        FROM [dbo].[player_stat]
    )
    INSERT INTO [dbo].[player_stat_last_n] (player_id, game_date, matchup, pts, reb, ast, stl, blk, tov, dreb, oreb, fg3m, fg3a, fgm, fga, pf, ftm, fta, rn)
    SELECT player_id, game_date, matchup, pts, reb, ast, stl, blk, tov, dreb, oreb, fg3m, fg3a, fgm, fga, pf, ftm, fta, CAST(rn AS smallint)
    FROM ranked
    WHERE rn <= @max_games_per_player;
END
GO
