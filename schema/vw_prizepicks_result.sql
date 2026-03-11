-- View: projection vs actual result for settled games.
-- Uses the most recent row per projection_id from prizepicks_projection_history (latest line if it changed).
-- Joins to player_stat to get actual stat for that player on the game date (start_time date).
-- Use for analytics: accuracy of lines, over/under hit rate, margin distribution, etc.

USE [Props]
GO

IF OBJECT_ID(N'[dbo].[vw_prizepicks_result]', N'V') IS NOT NULL
    DROP VIEW [dbo].[vw_prizepicks_result];
GO

CREATE VIEW [dbo].[vw_prizepicks_result] AS
WITH
-- Single stat types only (exclude Combo)
allowed_stats AS (
    SELECT s.[value] AS stat_type_name
    FROM (SELECT N'Points' AS [value] UNION ALL SELECT N'Rebounds' UNION ALL SELECT N'Assists'
          UNION ALL SELECT N'Steals' UNION ALL SELECT N'Blocks' UNION ALL SELECT N'Turnovers'
          UNION ALL SELECT N'Defensive Rebounds' UNION ALL SELECT N'Offensive Rebounds'
          UNION ALL SELECT N'3 Pointers Made' UNION ALL SELECT N'3 Pointers') s
),
-- Most recent history row per projection_id (latest line / state)
latest_history AS (
    SELECT
        projection_id, player_id, line_score, stat_type_name, odds_type, description, start_time,
        last_modified_at,
        ROW_NUMBER() OVER (PARTITION BY projection_id ORDER BY last_modified_at DESC) AS rn
    FROM [dbo].[prizepicks_projection_history]
    WHERE player_id IS NOT NULL
      AND line_score IS NOT NULL
      AND start_time IS NOT NULL
),
latest AS (
    SELECT projection_id, player_id, line_score, stat_type_name, odds_type, description, start_time
    FROM latest_history
    WHERE rn = 1
),
-- Resolve PrizePicks player to NBA player (nba_player_id)
proj_with_nba AS (
    SELECT
        l.projection_id,
        l.line_score,
        l.stat_type_name,
        l.odds_type,
        l.description,
        l.start_time,
        pp.display_name AS player_name,
        COALESCE(pp.nba_player_id, pl.player_id) AS nba_player_id
    FROM latest l
    INNER JOIN [dbo].[prizepicks_player] pp
        ON pp.player_id = CAST(l.player_id AS NVARCHAR(20))
    LEFT JOIN [dbo].[player] pl
        ON pp.nba_player_id IS NULL
       AND LTRIM(RTRIM(pp.display_name)) = LTRIM(RTRIM(pl.display_first_last))
    WHERE l.stat_type_name IN (SELECT stat_type_name FROM allowed_stats)
      AND (pp.nba_player_id IS NOT NULL OR pl.player_id IS NOT NULL)
),
-- Game date from projection start_time (date of the game)
game_date_from_proj AS (
    SELECT
        projection_id,
        player_name,
        stat_type_name,
        odds_type,
        line_score,
        description,
        start_time,
        nba_player_id,
        CAST(start_time AS date) AS game_date
    FROM proj_with_nba
),
-- Join to actual game stat
result AS (
    SELECT
        g.projection_id,
        g.player_name,
        g.stat_type_name,
        g.line_score,
        g.start_time,
        g.game_date,
        g.description,
        s.player_id AS nba_player_id,
        CASE g.stat_type_name
            WHEN N'Points' THEN s.pts
            WHEN N'Rebounds' THEN s.reb
            WHEN N'Assists' THEN s.ast
            WHEN N'Steals' THEN s.stl
            WHEN N'Blocks' THEN s.blk
            WHEN N'Turnovers' THEN s.tov
            WHEN N'Defensive Rebounds' THEN s.dreb
            WHEN N'Offensive Rebounds' THEN s.oreb
            WHEN N'3 Pointers Made' THEN s.fg3m
            WHEN N'3 Pointers' THEN s.fg3m
            ELSE NULL
        END AS actual_value
    FROM game_date_from_proj g
    INNER JOIN [dbo].[player_stat] s
        ON s.player_id = g.nba_player_id
       AND s.game_date = g.game_date
)
SELECT
    projection_id,
    player_name,
    stat_type_name,
    odds_type,
    line_score,
    actual_value,
    CASE WHEN actual_value > line_score THEN 1 ELSE 0 END AS hit_over,
    actual_value - line_score AS margin,
    start_time,
    game_date,
    description
FROM result
WHERE actual_value IS NOT NULL;
GO
