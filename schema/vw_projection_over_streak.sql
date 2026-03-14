-- View: Projections (standard odds) where the player exceeded the line in each of last 5 games.
-- Requires: prizepicks_projection, prizepicks_player, player, player_stat.
-- Used by API to LEFT JOIN and get favored/risk for each projection (default N=5).

USE [Props]
GO

IF OBJECT_ID(N'[dbo].[vw_projection_over_streak]', N'V') IS NOT NULL
    DROP VIEW [dbo].[vw_projection_over_streak];
GO

CREATE VIEW [dbo].[vw_projection_over_streak] AS
WITH
last5 AS (
    SELECT
        player_id,
        game_date,
        pts, reb, ast, stl, blk, tov, dreb, oreb, fg3m, fg3a, fgm, fga, pf, ftm, fta,
        ROW_NUMBER() OVER (PARTITION BY player_id ORDER BY game_date DESC) AS rn
    FROM [dbo].[player_stat]
),
last5_only AS (
    SELECT * FROM last5 WHERE rn <= 5
),
proj AS (
    SELECT
        p.projection_id,
        p.line_score,
        p.stat_type_name,
        p.description,
        CONVERT(NVARCHAR(50), p.start_time, 127) AS start_time,
        pp.display_name,
        COALESCE(pp.nba_player_id, pl.player_id) AS nba_player_id
    FROM [dbo].[prizepicks_projection] p
    INNER JOIN [dbo].[prizepicks_player] pp
        ON pp.player_id = CAST(p.player_id AS NVARCHAR(20))
    LEFT JOIN [dbo].[player] pl
        ON pp.nba_player_id IS NULL
       AND LTRIM(RTRIM(pp.display_name)) = LTRIM(RTRIM(pl.display_first_last))
    WHERE p.odds_type = N'standard'
      AND p.player_id IS NOT NULL
      AND p.stat_type_name NOT LIKE N'%(Combo)%'
      AND p.stat_type_name NOT LIKE N'%Combo%'
      AND p.league_id = 7
      AND (pp.nba_player_id IS NOT NULL OR pl.player_id IS NOT NULL)
),
game_values AS (
    SELECT
        proj.projection_id,
        proj.display_name,
        proj.stat_type_name,
        proj.line_score,
        proj.description,
        proj.start_time,
        l.game_date,
        CASE proj.stat_type_name
            WHEN N'Points' THEN l.pts
            WHEN N'Rebounds' THEN l.reb
            WHEN N'Assists' THEN l.ast
            WHEN N'Steals' THEN l.stl
            WHEN N'Blocks' THEN l.blk
            WHEN N'Blocked Shots' THEN l.blk
            WHEN N'Turnovers' THEN l.tov
            WHEN N'Defensive Rebounds' THEN l.dreb
            WHEN N'Offensive Rebounds' THEN l.oreb
            WHEN N'3 Pointers Made' THEN l.fg3m
            WHEN N'3 Pointers' THEN l.fg3m
            WHEN N'FG Made' THEN l.fgm
            WHEN N'FG Attempted' THEN l.fga
            WHEN N'Personal Fouls' THEN l.pf
            WHEN N'Blks+Stls' THEN l.blk + l.stl
            WHEN N'Free Throws Made' THEN l.ftm
            WHEN N'Free Throws Attempted' THEN l.fta
            WHEN N'Pts+Asts' THEN l.pts + l.ast
            WHEN N'Pts+Rebs' THEN l.pts + l.reb
            WHEN N'Pts+Rebs+Asts' THEN l.pts + l.reb + l.ast
            WHEN N'Rebs+Asts' THEN l.reb + l.ast
            WHEN N'Two Pointers Made' THEN l.fgm - l.fg3m
            WHEN N'Two Pointers Attempted' THEN l.fga - l.fg3a
            ELSE NULL
        END AS stat_value
    FROM proj
    INNER JOIN last5_only l ON l.player_id = proj.nba_player_id
    WHERE proj.stat_type_name IN (
        N'Points', N'Rebounds', N'Assists', N'Steals', N'Blocks', N'Blocked Shots', N'Turnovers',
        N'Defensive Rebounds', N'Offensive Rebounds', N'3 Pointers Made', N'3 Pointers',
        N'FG Made', N'FG Attempted', N'Personal Fouls', N'Blks+Stls',
        N'Free Throws Made', N'Free Throws Attempted',         N'Pts+Asts', N'Pts+Rebs', N'Pts+Rebs+Asts', N'Rebs+Asts',
        N'Two Pointers Made', N'Two Pointers Attempted'
    )
),
qualified AS (
    SELECT
        projection_id,
        display_name,
        stat_type_name,
        line_score,
        description,
        start_time,
        COUNT(*) AS game_count,
        MIN(stat_value) AS min_stat_value,
        MAX(stat_value) AS max_stat_value,
        AVG(CAST(stat_value AS FLOAT)) AS stat_mean,
        MIN(game_date) AS min_game_date,
        MAX(game_date) AS max_game_date
    FROM game_values
    WHERE stat_value IS NOT NULL
    GROUP BY projection_id, display_name, stat_type_name, line_score, description, start_time
    HAVING COUNT(*) = 5 AND MIN(stat_value) > line_score
)
SELECT
    q.projection_id,
    q.stat_type_name,
    q.display_name AS player_name,
    q.line_score,
    q.description,
    q.start_time,
    q.game_count AS last_5_games_count,
    q.min_game_date,
    q.max_game_date,
    DATEDIFF(day, q.min_game_date, q.max_game_date) AS days_span,
    q.min_stat_value AS stat_low,
    q.max_stat_value AS stat_high,
    ROUND(q.stat_mean, 2) AS stat_mean,
    q.min_stat_value - q.line_score AS cushion,
    q.max_stat_value - q.min_stat_value AS spread,
    CASE
        WHEN q.min_stat_value - q.line_score >= 3 THEN N'Low'
        WHEN q.min_stat_value - q.line_score >= 1 THEN N'Medium'
        ELSE N'High'
    END AS risk
FROM qualified q;
GO
