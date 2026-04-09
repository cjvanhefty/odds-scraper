-- MERGE all parlay_play_*_stage tables into main tables. Run after loading stage from JSON.
-- Order: sport -> league -> team -> match -> player -> stat_type -> projection (respects FKs).
USE [Props]
GO

-- 1. Sport
MERGE [dbo].[parlay_play_sport] AS t
USING [dbo].[parlay_play_sport_stage] AS s ON t.id = s.id
WHEN MATCHED THEN UPDATE SET
    t.sport_name = s.sport_name, t.slug = s.slug, t.symbol = s.symbol,
    t.illustration = s.illustration, t.popularity = s.popularity, t.last_modified_at = GETDATE()
WHEN NOT MATCHED BY TARGET THEN INSERT (id, sport_name, slug, symbol, illustration, popularity, last_modified_at)
VALUES (s.id, s.sport_name, s.slug, s.symbol, s.illustration, s.popularity, GETDATE());
GO

-- 2. League
MERGE [dbo].[parlay_play_league] AS t
USING [dbo].[parlay_play_league_stage] AS s ON t.id = s.id
WHEN MATCHED THEN UPDATE SET
    t.sport_id = s.sport_id, t.league_name = s.league_name, t.league_name_short = s.league_name_short,
    t.slug = s.slug, t.popularity = s.popularity, t.allowed_players_per_match = s.allowed_players_per_match, t.last_modified_at = GETDATE()
WHEN NOT MATCHED BY TARGET THEN INSERT (id, sport_id, league_name, league_name_short, slug, popularity, allowed_players_per_match, last_modified_at)
VALUES (s.id, s.sport_id, s.league_name, s.league_name_short, s.slug, s.popularity, s.allowed_players_per_match, GETDATE());
GO

-- 3. Team
MERGE [dbo].[parlay_play_team] AS t
USING [dbo].[parlay_play_team_stage] AS s ON t.id = s.id
WHEN MATCHED THEN UPDATE SET
    t.sport_id = s.sport_id, t.league_id = s.league_id, t.teamname = s.teamname, t.teamname_abbr = s.teamname_abbr,
    t.team_abbreviation = s.team_abbreviation, t.slug = s.slug, t.venue = s.venue, t.logo = s.logo,
    t.conference = s.conference, t.rank = s.rank, t.record = s.record, t.last_modified_at = GETDATE()
WHEN NOT MATCHED BY TARGET THEN INSERT (id, sport_id, league_id, teamname, teamname_abbr, team_abbreviation, slug, venue, logo, conference, rank, record, last_modified_at)
VALUES (s.id, s.sport_id, s.league_id, s.teamname, s.teamname_abbr, s.team_abbreviation, s.slug, s.venue, s.logo, s.conference, s.rank, s.record, GETDATE());
GO

-- 4. Match
MERGE [dbo].[parlay_play_match] AS t
USING [dbo].[parlay_play_match_stage] AS s ON t.id = s.id
WHEN MATCHED THEN UPDATE SET
    t.sport_id = s.sport_id, t.league_id = s.league_id, t.home_team_id = s.home_team_id, t.away_team_id = s.away_team_id,
    t.slug = s.slug, t.match_date = s.match_date, t.match_type = s.match_type, t.match_status = s.match_status,
    t.match_period = s.match_period, t.score_home = s.score_home, t.score_away = s.score_away,
    t.time_left = s.time_left, t.time_to_start = s.time_to_start, t.time_to_start_min = s.time_to_start_min,
    t.home_win_prob = s.home_win_prob, t.away_win_prob = s.away_win_prob, t.draw_prob = s.draw_prob, t.last_modified_at = GETDATE()
WHEN NOT MATCHED BY TARGET THEN INSERT (id, sport_id, league_id, home_team_id, away_team_id, slug, match_date, match_type, match_status, match_period, score_home, score_away, time_left, time_to_start, time_to_start_min, home_win_prob, away_win_prob, draw_prob, last_modified_at)
VALUES (s.id, s.sport_id, s.league_id, s.home_team_id, s.away_team_id, s.slug, s.match_date, s.match_type, s.match_status, s.match_period, s.score_home, s.score_away, s.time_left, s.time_to_start, s.time_to_start_min, s.home_win_prob, s.away_win_prob, s.draw_prob, GETDATE());
GO

-- 5. Player
MERGE [dbo].[parlay_play_player] AS t
USING [dbo].[parlay_play_player_stage] AS s ON t.id = s.id
WHEN MATCHED THEN UPDATE SET
    t.sport_id = s.sport_id, t.team_id = s.team_id, t.first_name = s.first_name, t.last_name = s.last_name,
    t.full_name = s.full_name, t.name_initial = s.name_initial, t.image = s.image, t.position = s.position,
    t.gender = s.gender, t.popularity = s.popularity, t.show_alt_lines = s.show_alt_lines, t.last_modified_at = GETDATE()
WHEN NOT MATCHED BY TARGET THEN INSERT (id, sport_id, team_id, first_name, last_name, full_name, name_initial, image, position, gender, popularity, show_alt_lines, last_modified_at)
VALUES (s.id, s.sport_id, s.team_id, s.first_name, s.last_name, s.full_name, s.name_initial, s.image, s.position, s.gender, s.popularity, s.show_alt_lines, GETDATE());
GO

-- 6. Stat type
MERGE [dbo].[parlay_play_stat_type] AS t
USING [dbo].[parlay_play_stat_type_stage] AS s ON t.challenge_option = s.challenge_option
WHEN MATCHED THEN UPDATE SET
    t.challenge_name = s.challenge_name, t.challenge_units = s.challenge_units, t.last_modified_at = GETDATE()
WHEN NOT MATCHED BY TARGET THEN INSERT (challenge_option, challenge_name, challenge_units, last_modified_at)
VALUES (s.challenge_option, s.challenge_name, s.challenge_units, GETDATE());
GO

-- 7. Projection (one row per line: main + alt)
MERGE [dbo].[parlay_play_projection] AS t
USING [dbo].[parlay_play_projection_stage] AS s ON t.projection_id = s.projection_id
WHEN MATCHED THEN UPDATE SET
    t.match_id = s.match_id, t.player_id = s.player_id, t.challenge_option = s.challenge_option,
    t.line_score = s.line_score, t.is_main_line = s.is_main_line, t.decimal_price_over = s.decimal_price_over, t.decimal_price_under = s.decimal_price_under,
    t.market_name = s.market_name, t.match_period = s.match_period, t.show_default = s.show_default,
    t.display_name = s.display_name, t.stat_type_name = s.stat_type_name, t.start_time = s.start_time,
    t.promo_deadline = s.promo_deadline, t.promo_max_entry = s.promo_max_entry, t.player_promo_id = s.player_promo_id, t.player_promo_type = s.player_promo_type,
    t.is_boosted_payout = s.is_boosted_payout, t.is_player_promo = s.is_player_promo, t.default_multiplier = s.default_multiplier, t.promo_multiplier = s.promo_multiplier,
    t.payout_boost_selection = s.payout_boost_selection, t.is_public = s.is_public, t.is_slashed_line = s.is_slashed_line, t.alt_line_count = s.alt_line_count, t.last_modified_at = GETDATE()
WHEN NOT MATCHED BY TARGET THEN INSERT (projection_id, match_id, player_id, challenge_option, line_score, is_main_line, decimal_price_over, decimal_price_under, market_name, match_period, show_default, display_name, stat_type_name, start_time, promo_deadline, promo_max_entry, player_promo_id, player_promo_type, is_boosted_payout, is_player_promo, default_multiplier, promo_multiplier, payout_boost_selection, is_public, is_slashed_line, alt_line_count, last_modified_at)
VALUES (s.projection_id, s.match_id, s.player_id, s.challenge_option, s.line_score, s.is_main_line, s.decimal_price_over, s.decimal_price_under, s.market_name, s.match_period, s.show_default, s.display_name, s.stat_type_name, s.start_time, s.promo_deadline, s.promo_max_entry, s.player_promo_id, s.player_promo_type, s.is_boosted_payout, s.is_player_promo, s.default_multiplier, s.promo_multiplier, s.payout_boost_selection, s.is_public, s.is_slashed_line, s.alt_line_count, GETDATE());
GO
