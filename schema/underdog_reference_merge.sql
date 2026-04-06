-- Underdog reference tables: stage -> main (PrizePicks-like change detection).
-- Load *_stage (truncate + insert per scrape), then EXEC dbo.MergeUnderdogReferenceFromStage.
-- Projection + history remain in underdog_scraper.upsert_underdog_from_stage (separate pipeline).
--
-- Join semantics (for queries / BI):
--   underdog_projection.appearance_id = underdog_appearance.id
--   underdog_appearance.player_id      = underdog_player.id
--
-- MERGE order: stat_type -> game -> player -> solo_game -> appearance (appearance references match/player ids from API).
USE [Props]
GO

SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER ON
GO

IF OBJECT_ID(N'[dbo].[MergeUnderdogReferenceFromStage]', N'P') IS NOT NULL
    DROP PROCEDURE [dbo].[MergeUnderdogReferenceFromStage];
GO

CREATE PROCEDURE [dbo].[MergeUnderdogReferenceFromStage]
AS
BEGIN
    SET NOCOUNT ON;

    -- 1) stat_type (from projection scrape; no FK dependencies)
    IF OBJECT_ID(N'[dbo].[underdog_stat_type_stage]', N'U') IS NOT NULL
       AND OBJECT_ID(N'[dbo].[underdog_stat_type]', N'U') IS NOT NULL
    BEGIN
        ;MERGE [dbo].[underdog_stat_type] AS t
        USING [dbo].[underdog_stat_type_stage] AS s ON t.[pickem_stat_id] = s.[pickem_stat_id]
        WHEN MATCHED AND NOT (
                (t.[stat_type_name] = s.[stat_type_name] OR (t.[stat_type_name] IS NULL AND s.[stat_type_name] IS NULL))
            AND (t.[display_stat] = s.[display_stat] OR (t.[display_stat] IS NULL AND s.[display_stat] IS NULL))
            AND (t.[stat] = s.[stat] OR (t.[stat] IS NULL AND s.[stat] IS NULL))
        ) THEN UPDATE SET
            [stat_type_name] = s.[stat_type_name],
            [display_stat] = s.[display_stat],
            [stat] = s.[stat],
            [last_modified_at] = GETUTCDATE()
        WHEN NOT MATCHED BY TARGET THEN INSERT (
            [pickem_stat_id], [stat_type_name], [display_stat], [stat], [last_modified_at]
        ) VALUES (
            s.[pickem_stat_id], s.[stat_type_name], s.[display_stat], s.[stat], GETUTCDATE()
        );
    END

    -- 2) game (wide row) — CHECKSUM change detection
    IF OBJECT_ID(N'[dbo].[underdog_game_stage]', N'U') IS NOT NULL
       AND OBJECT_ID(N'[dbo].[underdog_game]', N'U') IS NOT NULL
    BEGIN
        ;MERGE [dbo].[underdog_game] AS t
        USING [dbo].[underdog_game_stage] AS s ON t.[id] = s.[id]
        WHEN MATCHED AND NOT (
            CHECKSUM(
                ISNULL(CONVERT(nvarchar(50), s.[scheduled_at]), NCHAR(0)),
                ISNULL(s.[home_team_id], NCHAR(0)),
                ISNULL(s.[away_team_id], NCHAR(0)),
                ISNULL(s.[title], NCHAR(0)),
                ISNULL(s.[short_title], NCHAR(0)),
                ISNULL(s.[abbreviated_title], NCHAR(0)),
                ISNULL(s.[full_team_names_title], NCHAR(0)),
                ISNULL(s.[status], NCHAR(0)),
                ISNULL(s.[sport_id], NCHAR(0)),
                ISNULL(s.[type], NCHAR(0)),
                ISNULL(CONVERT(nvarchar(20), s.[period]), NCHAR(0)),
                ISNULL(s.[match_progress], NCHAR(0)),
                ISNULL(CONVERT(nvarchar(20), s.[away_team_score]), NCHAR(0)),
                ISNULL(CONVERT(nvarchar(20), s.[home_team_score]), NCHAR(0)),
                ISNULL(CONVERT(nvarchar(20), s.[rank]), NCHAR(0)),
                ISNULL(CONVERT(nvarchar(20), s.[year]), NCHAR(0)),
                ISNULL(s.[season_type], NCHAR(0)),
                ISNULL(CONVERT(nvarchar(50), s.[updated_at]), NCHAR(0)),
                ISNULL(CONVERT(nvarchar(30), s.[rescheduled_from]), NCHAR(0)),
                ISNULL(s.[title_suffix], NCHAR(0)),
                ISNULL(CONVERT(tinyint, s.[manually_created]), 0),
                ISNULL(s.[pre_game_data], NCHAR(0))
            )
            =
            CHECKSUM(
                ISNULL(CONVERT(nvarchar(50), t.[scheduled_at]), NCHAR(0)),
                ISNULL(t.[home_team_id], NCHAR(0)),
                ISNULL(t.[away_team_id], NCHAR(0)),
                ISNULL(t.[title], NCHAR(0)),
                ISNULL(t.[short_title], NCHAR(0)),
                ISNULL(t.[abbreviated_title], NCHAR(0)),
                ISNULL(t.[full_team_names_title], NCHAR(0)),
                ISNULL(t.[status], NCHAR(0)),
                ISNULL(t.[sport_id], NCHAR(0)),
                ISNULL(t.[type], NCHAR(0)),
                ISNULL(CONVERT(nvarchar(20), t.[period]), NCHAR(0)),
                ISNULL(t.[match_progress], NCHAR(0)),
                ISNULL(CONVERT(nvarchar(20), t.[away_team_score]), NCHAR(0)),
                ISNULL(CONVERT(nvarchar(20), t.[home_team_score]), NCHAR(0)),
                ISNULL(CONVERT(nvarchar(20), t.[rank]), NCHAR(0)),
                ISNULL(CONVERT(nvarchar(20), t.[year]), NCHAR(0)),
                ISNULL(t.[season_type], NCHAR(0)),
                ISNULL(CONVERT(nvarchar(50), t.[updated_at]), NCHAR(0)),
                ISNULL(CONVERT(nvarchar(30), t.[rescheduled_from]), NCHAR(0)),
                ISNULL(t.[title_suffix], NCHAR(0)),
                ISNULL(CONVERT(tinyint, t.[manually_created]), 0),
                ISNULL(t.[pre_game_data], NCHAR(0))
            )
        ) THEN UPDATE SET
            [scheduled_at] = s.[scheduled_at],
            [home_team_id] = s.[home_team_id],
            [away_team_id] = s.[away_team_id],
            [title] = s.[title],
            [short_title] = s.[short_title],
            [abbreviated_title] = s.[abbreviated_title],
            [full_team_names_title] = s.[full_team_names_title],
            [status] = s.[status],
            [sport_id] = s.[sport_id],
            [type] = s.[type],
            [period] = s.[period],
            [match_progress] = s.[match_progress],
            [away_team_score] = s.[away_team_score],
            [home_team_score] = s.[home_team_score],
            [rank] = s.[rank],
            [year] = s.[year],
            [season_type] = s.[season_type],
            [updated_at] = s.[updated_at],
            [rescheduled_from] = s.[rescheduled_from],
            [title_suffix] = s.[title_suffix],
            [manually_created] = s.[manually_created],
            [pre_game_data] = s.[pre_game_data],
            [last_modified_at] = GETUTCDATE()
        WHEN NOT MATCHED BY TARGET THEN INSERT (
            [id], [scheduled_at], [home_team_id], [away_team_id],
            [title], [short_title], [abbreviated_title], [full_team_names_title],
            [status], [sport_id], [type], [period], [match_progress],
            [away_team_score], [home_team_score], [rank], [year], [season_type],
            [updated_at], [rescheduled_from], [title_suffix], [manually_created],
            [pre_game_data], [last_modified_at]
        ) VALUES (
            s.[id], s.[scheduled_at], s.[home_team_id], s.[away_team_id],
            s.[title], s.[short_title], s.[abbreviated_title], s.[full_team_names_title],
            s.[status], s.[sport_id], s.[type], s.[period], s.[match_progress],
            s.[away_team_score], s.[home_team_score], s.[rank], s.[year], s.[season_type],
            s.[updated_at], s.[rescheduled_from], s.[title_suffix], s.[manually_created],
            s.[pre_game_data], GETUTCDATE()
        );
    END

    -- 3) player
    IF OBJECT_ID(N'[dbo].[underdog_player_stage]', N'U') IS NOT NULL
       AND OBJECT_ID(N'[dbo].[underdog_player]', N'U') IS NOT NULL
    BEGIN
        ;MERGE [dbo].[underdog_player] AS t
        USING [dbo].[underdog_player_stage] AS s ON t.[id] = s.[id]
        WHEN MATCHED AND NOT (
                (t.[first_name] = s.[first_name] OR (t.[first_name] IS NULL AND s.[first_name] IS NULL))
            AND (t.[last_name] = s.[last_name] OR (t.[last_name] IS NULL AND s.[last_name] IS NULL))
            AND (t.[position_display_name] = s.[position_display_name] OR (t.[position_display_name] IS NULL AND s.[position_display_name] IS NULL))
            AND (t.[position_id] = s.[position_id] OR (t.[position_id] IS NULL AND s.[position_id] IS NULL))
            AND (t.[position_name] = s.[position_name] OR (t.[position_name] IS NULL AND s.[position_name] IS NULL))
            AND (t.[team_id] = s.[team_id] OR (t.[team_id] IS NULL AND s.[team_id] IS NULL))
            AND (t.[sport_id] = s.[sport_id] OR (t.[sport_id] IS NULL AND s.[sport_id] IS NULL))
            AND (t.[jersey_number] = s.[jersey_number] OR (t.[jersey_number] IS NULL AND s.[jersey_number] IS NULL))
            AND (t.[image_url] = s.[image_url] OR (t.[image_url] IS NULL AND s.[image_url] IS NULL))
            AND (t.[dark_image_url] = s.[dark_image_url] OR (t.[dark_image_url] IS NULL AND s.[dark_image_url] IS NULL))
            AND (t.[light_image_url] = s.[light_image_url] OR (t.[light_image_url] IS NULL AND s.[light_image_url] IS NULL))
            AND (t.[action_path] = s.[action_path] OR (t.[action_path] IS NULL AND s.[action_path] IS NULL))
            AND (t.[country] = s.[country] OR (t.[country] IS NULL AND s.[country] IS NULL))
        ) THEN UPDATE SET
            [first_name] = s.[first_name],
            [last_name] = s.[last_name],
            [position_display_name] = s.[position_display_name],
            [position_id] = s.[position_id],
            [position_name] = s.[position_name],
            [team_id] = s.[team_id],
            [sport_id] = s.[sport_id],
            [jersey_number] = s.[jersey_number],
            [image_url] = s.[image_url],
            [dark_image_url] = s.[dark_image_url],
            [light_image_url] = s.[light_image_url],
            [action_path] = s.[action_path],
            [country] = s.[country],
            [last_modified_at] = GETUTCDATE()
        WHEN NOT MATCHED BY TARGET THEN INSERT (
            [id], [first_name], [last_name], [position_display_name], [position_id],
            [position_name], [team_id], [sport_id], [jersey_number], [image_url],
            [dark_image_url], [light_image_url], [action_path], [country], [last_modified_at]
        ) VALUES (
            s.[id], s.[first_name], s.[last_name], s.[position_display_name], s.[position_id],
            s.[position_name], s.[team_id], s.[sport_id], s.[jersey_number], s.[image_url],
            s.[dark_image_url], s.[light_image_url], s.[action_path], s.[country], GETUTCDATE()
        );
    END

    -- 4) solo_game (wide) — CHECKSUM
    IF OBJECT_ID(N'[dbo].[underdog_solo_game_stage]', N'U') IS NOT NULL
       AND OBJECT_ID(N'[dbo].[underdog_solo_game]', N'U') IS NOT NULL
    BEGIN
        ;MERGE [dbo].[underdog_solo_game] AS t
        USING [dbo].[underdog_solo_game_stage] AS s ON t.[id] = s.[id]
        WHEN MATCHED AND NOT (
            CHECKSUM(
                ISNULL(CONVERT(nvarchar(50), s.[scheduled_at]), NCHAR(0)),
                ISNULL(s.[home_player_id], NCHAR(0)),
                ISNULL(s.[away_player_id], NCHAR(0)),
                ISNULL(s.[title], NCHAR(0)),
                ISNULL(s.[short_title], NCHAR(0)),
                ISNULL(s.[abbreviated_title], NCHAR(0)),
                ISNULL(s.[full_title], NCHAR(0)),
                ISNULL(s.[status], NCHAR(0)),
                ISNULL(s.[sport_id], NCHAR(0)),
                ISNULL(s.[type], NCHAR(0)),
                ISNULL(s.[competition_id], NCHAR(0)),
                ISNULL(CONVERT(nvarchar(20), s.[rank]), NCHAR(0)),
                ISNULL(CONVERT(nvarchar(20), s.[period]), NCHAR(0)),
                ISNULL(s.[match_progress], NCHAR(0)),
                ISNULL(s.[score], NCHAR(0)),
                ISNULL(CONVERT(nvarchar(50), s.[updated_at]), NCHAR(0)),
                ISNULL(CONVERT(tinyint, s.[manually_created]), 0),
                ISNULL(s.[sport_tournament_round_id], NCHAR(0)),
                ISNULL(s.[pre_game_data], NCHAR(0))
            )
            =
            CHECKSUM(
                ISNULL(CONVERT(nvarchar(50), t.[scheduled_at]), NCHAR(0)),
                ISNULL(t.[home_player_id], NCHAR(0)),
                ISNULL(t.[away_player_id], NCHAR(0)),
                ISNULL(t.[title], NCHAR(0)),
                ISNULL(t.[short_title], NCHAR(0)),
                ISNULL(t.[abbreviated_title], NCHAR(0)),
                ISNULL(t.[full_title], NCHAR(0)),
                ISNULL(t.[status], NCHAR(0)),
                ISNULL(t.[sport_id], NCHAR(0)),
                ISNULL(t.[type], NCHAR(0)),
                ISNULL(t.[competition_id], NCHAR(0)),
                ISNULL(CONVERT(nvarchar(20), t.[rank]), NCHAR(0)),
                ISNULL(CONVERT(nvarchar(20), t.[period]), NCHAR(0)),
                ISNULL(t.[match_progress], NCHAR(0)),
                ISNULL(t.[score], NCHAR(0)),
                ISNULL(CONVERT(nvarchar(50), t.[updated_at]), NCHAR(0)),
                ISNULL(CONVERT(tinyint, t.[manually_created]), 0),
                ISNULL(t.[sport_tournament_round_id], NCHAR(0)),
                ISNULL(t.[pre_game_data], NCHAR(0))
            )
        ) THEN UPDATE SET
            [scheduled_at] = s.[scheduled_at],
            [home_player_id] = s.[home_player_id],
            [away_player_id] = s.[away_player_id],
            [title] = s.[title],
            [short_title] = s.[short_title],
            [abbreviated_title] = s.[abbreviated_title],
            [full_title] = s.[full_title],
            [status] = s.[status],
            [sport_id] = s.[sport_id],
            [type] = s.[type],
            [competition_id] = s.[competition_id],
            [rank] = s.[rank],
            [period] = s.[period],
            [match_progress] = s.[match_progress],
            [score] = s.[score],
            [updated_at] = s.[updated_at],
            [manually_created] = s.[manually_created],
            [sport_tournament_round_id] = s.[sport_tournament_round_id],
            [pre_game_data] = s.[pre_game_data],
            [last_modified_at] = GETUTCDATE()
        WHEN NOT MATCHED BY TARGET THEN INSERT (
            [id], [scheduled_at], [home_player_id], [away_player_id],
            [title], [short_title], [abbreviated_title], [full_title],
            [status], [sport_id], [type], [competition_id], [rank],
            [period], [match_progress], [score], [updated_at],
            [manually_created], [sport_tournament_round_id], [pre_game_data],
            [last_modified_at]
        ) VALUES (
            s.[id], s.[scheduled_at], s.[home_player_id], s.[away_player_id],
            s.[title], s.[short_title], s.[abbreviated_title], s.[full_title],
            s.[status], s.[sport_id], s.[type], s.[competition_id], s.[rank],
            s.[period], s.[match_progress], s.[score], s.[updated_at],
            s.[manually_created], s.[sport_tournament_round_id], s.[pre_game_data],
            GETUTCDATE()
        );
    END

    -- 5) appearance (player_id -> underdog_player.id; match_id -> underdog_game.id conceptually)
    IF OBJECT_ID(N'[dbo].[underdog_appearance_stage]', N'U') IS NOT NULL
       AND OBJECT_ID(N'[dbo].[underdog_appearance]', N'U') IS NOT NULL
    BEGIN
        ;MERGE [dbo].[underdog_appearance] AS t
        USING [dbo].[underdog_appearance_stage] AS s ON t.[id] = s.[id]
        WHEN MATCHED AND NOT (
                (t.[player_id] = s.[player_id] OR (t.[player_id] IS NULL AND s.[player_id] IS NULL))
            AND (t.[match_id] = s.[match_id] OR (t.[match_id] IS NULL AND s.[match_id] IS NULL))
            AND (t.[match_type] = s.[match_type] OR (t.[match_type] IS NULL AND s.[match_type] IS NULL))
            AND (t.[team_id] = s.[team_id] OR (t.[team_id] IS NULL AND s.[team_id] IS NULL))
            AND (t.[position_id] = s.[position_id] OR (t.[position_id] IS NULL AND s.[position_id] IS NULL))
            AND (t.[lineup_status_id] = s.[lineup_status_id] OR (t.[lineup_status_id] IS NULL AND s.[lineup_status_id] IS NULL))
            AND (t.[sort_by] = s.[sort_by] OR (t.[sort_by] IS NULL AND s.[sort_by] IS NULL))
            AND (t.[multiple_picks_allowed] = s.[multiple_picks_allowed] OR (t.[multiple_picks_allowed] IS NULL AND s.[multiple_picks_allowed] IS NULL))
            AND (t.[type] = s.[type] OR (t.[type] IS NULL AND s.[type] IS NULL))
        ) THEN UPDATE SET
            [player_id] = s.[player_id],
            [match_id] = s.[match_id],
            [match_type] = s.[match_type],
            [team_id] = s.[team_id],
            [position_id] = s.[position_id],
            [lineup_status_id] = s.[lineup_status_id],
            [sort_by] = s.[sort_by],
            [multiple_picks_allowed] = s.[multiple_picks_allowed],
            [type] = s.[type],
            [last_modified_at] = GETUTCDATE()
        WHEN NOT MATCHED BY TARGET THEN INSERT (
            [id], [player_id], [match_id], [match_type], [team_id],
            [position_id], [lineup_status_id], [sort_by],
            [multiple_picks_allowed], [type], [last_modified_at]
        ) VALUES (
            s.[id], s.[player_id], s.[match_id], s.[match_type], s.[team_id],
            s.[position_id], s.[lineup_status_id], s.[sort_by],
            s.[multiple_picks_allowed], s.[type], GETUTCDATE()
        );
    END
END
GO
