-- 1. MERGE prizepicks_projection_stage into prizepicks_projection (upsert).
-- 2. Archive to history any rows in projection that are not in stage (before deleting).
-- 3. DELETE from prizepicks_projection records that were not in the stage (not upserted).

USE [Props]
GO

-- Step 1: MERGE (upsert) stage into projection
MERGE [dbo].[prizepicks_projection] AS t
USING [dbo].[prizepicks_projection_stage] AS s
ON t.projection_id = s.projection_id
WHEN MATCHED THEN UPDATE SET
    t.projection_type = s.projection_type,
    t.adjusted_odds = s.adjusted_odds,
    t.board_time = s.board_time,
    t.custom_image = s.custom_image,
    t.description = s.description,
    t.end_time = s.end_time,
    t.event_type = s.event_type,
    t.flash_sale_line_score = s.flash_sale_line_score,
    t.game_id = s.game_id,
    t.group_key = s.group_key,
    t.hr_20 = s.hr_20,
    t.in_game = s.in_game,
    t.is_live = s.is_live,
    t.is_live_scored = s.is_live_scored,
    t.is_promo = s.is_promo,
    t.line_score = s.line_score,
    t.odds_type = s.odds_type,
    t.projection_display_type = s.projection_display_type,
    t.rank = s.rank,
    t.refundable = s.refundable,
    t.start_time = s.start_time,
    t.stat_display_name = s.stat_display_name,
    t.stat_type_name = s.stat_type_name,
    t.status = s.status,
    t.today = s.today,
    t.tv_channel = s.tv_channel,
    t.updated_at = s.updated_at,
    t.duration_id = s.duration_id,
    t.game_rel_id = s.game_rel_id,
    t.league_id = s.league_id,
    t.player_id = s.player_id,
    t.projection_type_id = s.projection_type_id,
    t.score_id = s.score_id,
    t.stat_type_id = s.stat_type_id,
    t.last_modified_at = SYSDATETIME()
WHEN NOT MATCHED BY TARGET THEN INSERT (
    projection_id, projection_type, adjusted_odds, board_time, custom_image,
    description, end_time, event_type, flash_sale_line_score, game_id,
    group_key, hr_20, in_game, is_live, is_live_scored, is_promo,
    line_score, odds_type, projection_display_type, rank, refundable,
    start_time, stat_display_name, stat_type_name, status, today,
    tv_channel, updated_at, duration_id, game_rel_id, league_id,
    player_id, projection_type_id, score_id, stat_type_id,
    created_at, last_modified_at
) VALUES (
    s.projection_id, s.projection_type, s.adjusted_odds, s.board_time, s.custom_image,
    s.description, s.end_time, s.event_type, s.flash_sale_line_score, s.game_id,
    s.group_key, s.hr_20, s.in_game, s.is_live, s.is_live_scored, s.is_promo,
    s.line_score, s.odds_type, s.projection_display_type, s.rank, s.refundable,
    s.start_time, s.stat_display_name, s.stat_type_name, s.status, s.today,
    s.tv_channel, s.updated_at, s.duration_id, s.game_rel_id, s.league_id,
    s.player_id, s.projection_type_id, s.score_id, s.stat_type_id,
    SYSDATETIME(), SYSDATETIME()
);

-- Step 2: Archive to history any projection rows that are not in the current stage (so we don't lose them)
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
WHERE p.projection_id NOT IN (SELECT projection_id FROM [dbo].[prizepicks_projection_stage])
  AND NOT EXISTS (SELECT 1 FROM [dbo].[prizepicks_projection_history] h WHERE h.projection_id = p.projection_id);

-- Step 3: Delete from projection records that were not upserted (not in stage)
DELETE FROM [dbo].[prizepicks_projection]
WHERE projection_id NOT IN (SELECT projection_id FROM [dbo].[prizepicks_projection_stage]);
