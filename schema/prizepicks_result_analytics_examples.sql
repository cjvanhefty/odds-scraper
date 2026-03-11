-- Example analytics on vw_prizepicks_result (projection vs actual).
-- Run after creating vw_prizepicks_result.

USE [Props]
GO

-- 1. Over hit rate by stat type (how often the line was exceeded)
SELECT
    stat_type_name,
    COUNT(*) AS total_picks,
    SUM(hit_over) AS hits_over,
    CAST(100.0 * SUM(hit_over) / NULLIF(COUNT(*), 0) AS decimal(5,2)) AS pct_over
FROM [dbo].[vw_prizepicks_result]
GROUP BY stat_type_name
ORDER BY stat_type_name;

-- 2. Over / under / on-line by stat type AND odds_type (hit rate per stat per market)
SELECT
    stat_type_name,
    odds_type,
    COUNT(*) AS total_picks,
    CAST(100.0 * SUM(hit_over) / NULLIF(COUNT(*), 0) AS decimal(5,2)) AS pct_over,
    CAST(100.0 * SUM(CASE WHEN margin < 0 THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0) AS decimal(5,2)) AS pct_under,
    CAST(100.0 * SUM(CASE WHEN margin = 0 THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0) AS decimal(5,2)) AS pct_on_line
FROM [dbo].[vw_prizepicks_result]
GROUP BY stat_type_name, odds_type
ORDER BY stat_type_name, odds_type;

-- 3. Hit rate by odds_type only (standard vs demon vs goblin overall)
SELECT
    odds_type,
    COUNT(*) AS total_picks,
    SUM(hit_over) AS hits_over,
    CAST(100.0 * SUM(hit_over) / NULLIF(COUNT(*), 0) AS decimal(5,2)) AS pct_over,
    AVG(margin) AS avg_margin
FROM [dbo].[vw_prizepicks_result]
GROUP BY odds_type
ORDER BY pct_over DESC;

-- 4. Average margin (actual - line) by stat type
SELECT
    stat_type_name,
    COUNT(*) AS n,
    AVG(margin) AS avg_margin,
    AVG(line_score) AS avg_line
FROM [dbo].[vw_prizepicks_result]
GROUP BY stat_type_name
ORDER BY stat_type_name;

-- 5. Average margin by stat type AND odds_type (are demon lines tougher?)
SELECT
    stat_type_name,
    odds_type,
    COUNT(*) AS n,
    AVG(margin) AS avg_margin,
    CAST(100.0 * SUM(hit_over) / NULLIF(COUNT(*), 0) AS decimal(5,2)) AS pct_over
FROM [dbo].[vw_prizepicks_result]
GROUP BY stat_type_name, odds_type
HAVING COUNT(*) >= 5
ORDER BY stat_type_name, odds_type;

-- 6. Lines that were “easy” (high hit_over rate) vs “hard” (low hit_over rate)
SELECT
    stat_type_name,
    odds_type,
    line_score,
    COUNT(*) AS n,
    SUM(hit_over) AS hits,
    CAST(100.0 * SUM(hit_over) / COUNT(*) AS decimal(5,2)) AS pct_over
FROM [dbo].[vw_prizepicks_result]
GROUP BY stat_type_name, odds_type, line_score
HAVING COUNT(*) >= 3
ORDER BY stat_type_name, odds_type, line_score;

-- 7. Player-level: who beats the line most often (min 10 results), with odds_type
SELECT
    player_name,
    stat_type_name,
    odds_type,
    COUNT(*) AS total,
    SUM(hit_over) AS hits_over,
    CAST(100.0 * SUM(hit_over) / COUNT(*) AS decimal(5,2)) AS pct_over,
    AVG(margin) AS avg_margin
FROM [dbo].[vw_prizepicks_result]
GROUP BY player_name, stat_type_name, odds_type
HAVING COUNT(*) >= 10
ORDER BY pct_over DESC, total DESC;

-- 8. Best odds_type per stat type: over / under / on-line % (min 20 picks)
SELECT
    stat_type_name,
    odds_type,
    COUNT(*) AS n,
    CAST(100.0 * SUM(hit_over) / NULLIF(COUNT(*), 0) AS decimal(5,2)) AS pct_over,
    CAST(100.0 * SUM(CASE WHEN margin < 0 THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0) AS decimal(5,2)) AS pct_under,
    CAST(100.0 * SUM(CASE WHEN margin = 0 THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0) AS decimal(5,2)) AS pct_on_line
FROM [dbo].[vw_prizepicks_result]
GROUP BY stat_type_name, odds_type
HAVING COUNT(*) >= 20
ORDER BY stat_type_name, pct_over DESC;

-- 9. Recent results (last 30 days of game_date), include odds_type
SELECT TOP 200
    game_date,
    player_name,
    stat_type_name,
    odds_type,
    line_score,
    actual_value,
    hit_over,
    margin
FROM [dbo].[vw_prizepicks_result]
WHERE game_date >= DATEADD(day, -30, CAST(GETDATE() AS date))
ORDER BY game_date DESC, player_name, stat_type_name, odds_type;
