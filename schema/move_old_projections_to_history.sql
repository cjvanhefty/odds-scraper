-- Move prizepicks_projection rows to prizepicks_projection_history where start_time is before today.
-- Step 1: Copy matching rows into history (skip if projection_id already in history).
-- Step 2: Delete those rows from projection.

USE [Props]
GO

-- 1. Insert into history (only rows not already in history)
INSERT INTO [dbo].[prizepicks_projection_history] (
    projection_id, projection_type, adjusted_odds, board_time, custom_image,
    description, end_time, event_type, flash_sale_line_score, game_id,
    group_key, hr_20, in_game, is_live, is_live_scored, is_promo,
    line_score, odds_type, projection_display_type, rank, refundable,
    start_time, stat_display_name, stat_type_name, status, today,
    tv_channel, updated_at, duration_id, game_rel_id, league_id,
    player_id, projection_type_id, score_id, stat_type_id,
    created_at, last_modified_at
)
SELECT
    p.projection_id, p.projection_type, p.adjusted_odds, p.board_time, p.custom_image,
    p.description, p.end_time, p.event_type, p.flash_sale_line_score, p.game_id,
    p.group_key, p.hr_20, p.in_game, p.is_live, p.is_live_scored, p.is_promo,
    p.line_score, p.odds_type, p.projection_display_type, p.rank, p.refundable,
    p.start_time, p.stat_display_name, p.stat_type_name, p.status, p.today,
    p.tv_channel, p.updated_at, p.duration_id, p.game_rel_id, p.league_id,
    p.player_id, p.projection_type_id, p.score_id, p.stat_type_id,
    p.created_at, p.last_modified_at
FROM [dbo].[prizepicks_projection] p
WHERE p.start_time < CAST(GETDATE() AS DATE)
  AND NOT EXISTS (SELECT 1 FROM [dbo].[prizepicks_projection_history] h WHERE h.projection_id = p.projection_id);

-- 2. Delete moved rows from projection
DELETE FROM [dbo].[prizepicks_projection]
WHERE start_time < CAST(GETDATE() AS DATE);
