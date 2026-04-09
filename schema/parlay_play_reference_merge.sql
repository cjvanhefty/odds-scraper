-- Parlay Play stage -> main merge procedure (PrizePicks-like semantics).
-- Pattern: load *_stage (truncate+insert), then EXEC dbo.MergeParlayPlayFromStage.
-- Change detection: matched rows are UPDATED only when at least one compared column differs.
USE [Props]
GO

SET ANSI_NULLS ON
SET QUOTED_IDENTIFIER ON
GO

IF OBJECT_ID(N'[dbo].[MergeParlayPlayFromStage]', N'P') IS NOT NULL
    DROP PROCEDURE [dbo].[MergeParlayPlayFromStage];
GO

CREATE PROCEDURE [dbo].[MergeParlayPlayFromStage]
AS
BEGIN
    SET NOCOUNT ON;

    -- 1) sport
    IF OBJECT_ID(N'[dbo].[parlay_play_sport_stage]', N'U') IS NOT NULL
       AND OBJECT_ID(N'[dbo].[parlay_play_sport]', N'U') IS NOT NULL
    BEGIN
        IF COL_LENGTH(N'dbo.parlay_play_sport', N'id') IS NOT NULL
        BEGIN
            ;MERGE [dbo].[parlay_play_sport] AS t
            USING [dbo].[parlay_play_sport_stage] AS s ON t.[id] = s.[id]
            WHEN MATCHED AND NOT (
                    (t.[sport_name] = s.[sport_name] OR (t.[sport_name] IS NULL AND s.[sport_name] IS NULL))
                AND (t.[slug] = s.[slug] OR (t.[slug] IS NULL AND s.[slug] IS NULL))
                AND (t.[symbol] = s.[symbol] OR (t.[symbol] IS NULL AND s.[symbol] IS NULL))
                AND (t.[illustration] = s.[illustration] OR (t.[illustration] IS NULL AND s.[illustration] IS NULL))
                AND (t.[popularity] = s.[popularity] OR (t.[popularity] IS NULL AND s.[popularity] IS NULL))
            ) THEN UPDATE SET
                [sport_name] = s.[sport_name],
                [slug] = s.[slug],
                [symbol] = s.[symbol],
                [illustration] = s.[illustration],
                [popularity] = s.[popularity],
                [last_modified_at] = GETDATE()
            WHEN NOT MATCHED BY TARGET THEN INSERT (
                [id], [sport_name], [slug], [symbol], [illustration], [popularity], [last_modified_at]
            ) VALUES (
                s.[id], s.[sport_name], s.[slug], s.[symbol], s.[illustration], s.[popularity], GETDATE()
            );
        END
        ELSE IF COL_LENGTH(N'dbo.parlay_play_sport', N'parlay_play_sport_id') IS NOT NULL
        BEGIN
            ;MERGE [dbo].[parlay_play_sport] AS t
            USING [dbo].[parlay_play_sport_stage] AS s ON t.[parlay_play_sport_id] = s.[id]
            WHEN MATCHED AND NOT (
                    (t.[sport_name] = s.[sport_name] OR (t.[sport_name] IS NULL AND s.[sport_name] IS NULL))
                AND (t.[slug] = s.[slug] OR (t.[slug] IS NULL AND s.[slug] IS NULL))
                AND (t.[symbol] = s.[symbol] OR (t.[symbol] IS NULL AND s.[symbol] IS NULL))
                AND (t.[illustration] = s.[illustration] OR (t.[illustration] IS NULL AND s.[illustration] IS NULL))
                AND (t.[popularity] = s.[popularity] OR (t.[popularity] IS NULL AND s.[popularity] IS NULL))
            ) THEN UPDATE SET
                [sport_name] = s.[sport_name],
                [slug] = s.[slug],
                [symbol] = s.[symbol],
                [illustration] = s.[illustration],
                [popularity] = s.[popularity],
                [last_modified_at] = GETDATE()
            WHEN NOT MATCHED BY TARGET THEN INSERT (
                [parlay_play_sport_id], [sport_name], [slug], [symbol], [illustration], [popularity], [last_modified_at]
            ) VALUES (
                s.[id], s.[sport_name], s.[slug], s.[symbol], s.[illustration], s.[popularity], GETDATE()
            );
        END
        ELSE
            THROW 50002, 'parlay_play_sport is missing expected PK column (id or parlay_play_sport_id).', 1;
    END

    -- 2) league
    IF OBJECT_ID(N'[dbo].[parlay_play_league_stage]', N'U') IS NOT NULL
       AND OBJECT_ID(N'[dbo].[parlay_play_league]', N'U') IS NOT NULL
    BEGIN
        IF COL_LENGTH(N'dbo.parlay_play_league', N'id') IS NOT NULL
        BEGIN
            ;MERGE [dbo].[parlay_play_league] AS t
            USING [dbo].[parlay_play_league_stage] AS s ON t.[id] = s.[id]
            WHEN MATCHED AND NOT (
                    (t.[sport_id] = s.[sport_id] OR (t.[sport_id] IS NULL AND s.[sport_id] IS NULL))
                AND (t.[league_name] = s.[league_name] OR (t.[league_name] IS NULL AND s.[league_name] IS NULL))
                AND (t.[league_name_short] = s.[league_name_short] OR (t.[league_name_short] IS NULL AND s.[league_name_short] IS NULL))
                AND (t.[slug] = s.[slug] OR (t.[slug] IS NULL AND s.[slug] IS NULL))
                AND (t.[popularity] = s.[popularity] OR (t.[popularity] IS NULL AND s.[popularity] IS NULL))
                AND (t.[allowed_players_per_match] = s.[allowed_players_per_match] OR (t.[allowed_players_per_match] IS NULL AND s.[allowed_players_per_match] IS NULL))
            ) THEN UPDATE SET
                [sport_id] = s.[sport_id],
                [league_name] = s.[league_name],
                [league_name_short] = s.[league_name_short],
                [slug] = s.[slug],
                [popularity] = s.[popularity],
                [allowed_players_per_match] = s.[allowed_players_per_match],
                [last_modified_at] = GETDATE()
            WHEN NOT MATCHED BY TARGET THEN INSERT (
                [id], [sport_id], [league_name], [league_name_short], [slug], [popularity], [allowed_players_per_match], [last_modified_at]
            ) VALUES (
                s.[id], s.[sport_id], s.[league_name], s.[league_name_short], s.[slug], s.[popularity], s.[allowed_players_per_match], GETDATE()
            );
        END
        ELSE IF COL_LENGTH(N'dbo.parlay_play_league', N'parlay_play_league_id') IS NOT NULL
        BEGIN
            ;MERGE [dbo].[parlay_play_league] AS t
            USING [dbo].[parlay_play_league_stage] AS s ON t.[parlay_play_league_id] = s.[id]
            WHEN MATCHED AND NOT (
                    (t.[sport_id] = s.[sport_id] OR (t.[sport_id] IS NULL AND s.[sport_id] IS NULL))
                AND (t.[league_name] = s.[league_name] OR (t.[league_name] IS NULL AND s.[league_name] IS NULL))
                AND (t.[league_name_short] = s.[league_name_short] OR (t.[league_name_short] IS NULL AND s.[league_name_short] IS NULL))
                AND (t.[slug] = s.[slug] OR (t.[slug] IS NULL AND s.[slug] IS NULL))
                AND (t.[popularity] = s.[popularity] OR (t.[popularity] IS NULL AND s.[popularity] IS NULL))
                AND (t.[allowed_players_per_match] = s.[allowed_players_per_match] OR (t.[allowed_players_per_match] IS NULL AND s.[allowed_players_per_match] IS NULL))
            ) THEN UPDATE SET
                [sport_id] = s.[sport_id],
                [league_name] = s.[league_name],
                [league_name_short] = s.[league_name_short],
                [slug] = s.[slug],
                [popularity] = s.[popularity],
                [allowed_players_per_match] = s.[allowed_players_per_match],
                [last_modified_at] = GETDATE()
            WHEN NOT MATCHED BY TARGET THEN INSERT (
                [parlay_play_league_id], [sport_id], [league_name], [league_name_short], [slug], [popularity], [allowed_players_per_match], [last_modified_at]
            ) VALUES (
                s.[id], s.[sport_id], s.[league_name], s.[league_name_short], s.[slug], s.[popularity], s.[allowed_players_per_match], GETDATE()
            );
        END
        ELSE
            THROW 50003, 'parlay_play_league is missing expected PK column (id or parlay_play_league_id).', 1;
    END

    -- 3) team
    IF OBJECT_ID(N'[dbo].[parlay_play_team_stage]', N'U') IS NOT NULL
       AND OBJECT_ID(N'[dbo].[parlay_play_team]', N'U') IS NOT NULL
    BEGIN
        IF COL_LENGTH(N'dbo.parlay_play_team', N'id') IS NOT NULL
        BEGIN
            ;MERGE [dbo].[parlay_play_team] AS t
            USING [dbo].[parlay_play_team_stage] AS s ON t.[id] = s.[id]
            WHEN MATCHED AND NOT (
                    (t.[sport_id] = s.[sport_id] OR (t.[sport_id] IS NULL AND s.[sport_id] IS NULL))
                AND (t.[league_id] = s.[league_id] OR (t.[league_id] IS NULL AND s.[league_id] IS NULL))
                AND (t.[teamname] = s.[teamname] OR (t.[teamname] IS NULL AND s.[teamname] IS NULL))
                AND (t.[teamname_abbr] = s.[teamname_abbr] OR (t.[teamname_abbr] IS NULL AND s.[teamname_abbr] IS NULL))
                AND (t.[team_abbreviation] = s.[team_abbreviation] OR (t.[team_abbreviation] IS NULL AND s.[team_abbreviation] IS NULL))
                AND (t.[slug] = s.[slug] OR (t.[slug] IS NULL AND s.[slug] IS NULL))
                AND (t.[venue] = s.[venue] OR (t.[venue] IS NULL AND s.[venue] IS NULL))
                AND (t.[logo] = s.[logo] OR (t.[logo] IS NULL AND s.[logo] IS NULL))
                AND (t.[conference] = s.[conference] OR (t.[conference] IS NULL AND s.[conference] IS NULL))
                AND (t.[rank] = s.[rank] OR (t.[rank] IS NULL AND s.[rank] IS NULL))
                AND (t.[record] = s.[record] OR (t.[record] IS NULL AND s.[record] IS NULL))
            ) THEN UPDATE SET
                [sport_id] = s.[sport_id],
                [league_id] = s.[league_id],
                [teamname] = s.[teamname],
                [teamname_abbr] = s.[teamname_abbr],
                [team_abbreviation] = s.[team_abbreviation],
                [slug] = s.[slug],
                [venue] = s.[venue],
                [logo] = s.[logo],
                [conference] = s.[conference],
                [rank] = s.[rank],
                [record] = s.[record],
                [last_modified_at] = GETDATE()
            WHEN NOT MATCHED BY TARGET THEN INSERT (
                [id], [sport_id], [league_id], [teamname], [teamname_abbr], [team_abbreviation], [slug], [venue], [logo],
                [conference], [rank], [record], [last_modified_at]
            ) VALUES (
                s.[id], s.[sport_id], s.[league_id], s.[teamname], s.[teamname_abbr], s.[team_abbreviation], s.[slug], s.[venue], s.[logo],
                s.[conference], s.[rank], s.[record], GETDATE()
            );
        END
        ELSE IF COL_LENGTH(N'dbo.parlay_play_team', N'parlay_play_team_id') IS NOT NULL
        BEGIN
            ;MERGE [dbo].[parlay_play_team] AS t
            USING [dbo].[parlay_play_team_stage] AS s ON t.[parlay_play_team_id] = s.[id]
            WHEN MATCHED AND NOT (
                    (t.[sport_id] = s.[sport_id] OR (t.[sport_id] IS NULL AND s.[sport_id] IS NULL))
                AND (t.[league_id] = s.[league_id] OR (t.[league_id] IS NULL AND s.[league_id] IS NULL))
                AND (t.[teamname] = s.[teamname] OR (t.[teamname] IS NULL AND s.[teamname] IS NULL))
                AND (t.[teamname_abbr] = s.[teamname_abbr] OR (t.[teamname_abbr] IS NULL AND s.[teamname_abbr] IS NULL))
                AND (t.[team_abbreviation] = s.[team_abbreviation] OR (t.[team_abbreviation] IS NULL AND s.[team_abbreviation] IS NULL))
                AND (t.[slug] = s.[slug] OR (t.[slug] IS NULL AND s.[slug] IS NULL))
                AND (t.[venue] = s.[venue] OR (t.[venue] IS NULL AND s.[venue] IS NULL))
                AND (t.[logo] = s.[logo] OR (t.[logo] IS NULL AND s.[logo] IS NULL))
                AND (t.[conference] = s.[conference] OR (t.[conference] IS NULL AND s.[conference] IS NULL))
                AND (t.[rank] = s.[rank] OR (t.[rank] IS NULL AND s.[rank] IS NULL))
                AND (t.[record] = s.[record] OR (t.[record] IS NULL AND s.[record] IS NULL))
            ) THEN UPDATE SET
                [sport_id] = s.[sport_id],
                [league_id] = s.[league_id],
                [teamname] = s.[teamname],
                [teamname_abbr] = s.[teamname_abbr],
                [team_abbreviation] = s.[team_abbreviation],
                [slug] = s.[slug],
                [venue] = s.[venue],
                [logo] = s.[logo],
                [conference] = s.[conference],
                [rank] = s.[rank],
                [record] = s.[record],
                [last_modified_at] = GETDATE()
            WHEN NOT MATCHED BY TARGET THEN INSERT (
                [parlay_play_team_id], [sport_id], [league_id], [teamname], [teamname_abbr], [team_abbreviation], [slug], [venue], [logo],
                [conference], [rank], [record], [last_modified_at]
            ) VALUES (
                s.[id], s.[sport_id], s.[league_id], s.[teamname], s.[teamname_abbr], s.[team_abbreviation], s.[slug], s.[venue], s.[logo],
                s.[conference], s.[rank], s.[record], GETDATE()
            );
        END
        ELSE
            THROW 50004, 'parlay_play_team is missing expected PK column (id or parlay_play_team_id).', 1;
    END

    -- 4) match (wide) - use CHECKSUM for change detection
    IF OBJECT_ID(N'[dbo].[parlay_play_match_stage]', N'U') IS NOT NULL
       AND OBJECT_ID(N'[dbo].[parlay_play_match]', N'U') IS NOT NULL
    BEGIN
        IF COL_LENGTH(N'dbo.parlay_play_match', N'id') IS NOT NULL
        BEGIN
            ;MERGE [dbo].[parlay_play_match] AS t
            USING [dbo].[parlay_play_match_stage] AS s ON t.[id] = s.[id]
            WHEN MATCHED AND NOT (
                CHECKSUM(
                    ISNULL(CONVERT(nvarchar(20), s.[sport_id]), NCHAR(0)),
                    ISNULL(CONVERT(nvarchar(20), s.[league_id]), NCHAR(0)),
                    ISNULL(CONVERT(nvarchar(20), s.[home_team_id]), NCHAR(0)),
                    ISNULL(CONVERT(nvarchar(20), s.[away_team_id]), NCHAR(0)),
                    ISNULL(s.[slug], NCHAR(0)),
                    ISNULL(CONVERT(nvarchar(50), s.[match_date]), NCHAR(0)),
                    ISNULL(s.[match_type], NCHAR(0)),
                    ISNULL(CONVERT(nvarchar(20), s.[match_status]), NCHAR(0)),
                    ISNULL(s.[match_period], NCHAR(0)),
                    ISNULL(CONVERT(nvarchar(20), s.[score_home]), NCHAR(0)),
                    ISNULL(CONVERT(nvarchar(20), s.[score_away]), NCHAR(0)),
                    ISNULL(s.[time_left], NCHAR(0)),
                    ISNULL(s.[time_to_start], NCHAR(0)),
                    ISNULL(CONVERT(nvarchar(20), s.[time_to_start_min]), NCHAR(0)),
                    ISNULL(s.[home_win_prob], NCHAR(0)),
                    ISNULL(s.[away_win_prob], NCHAR(0)),
                    ISNULL(s.[draw_prob], NCHAR(0))
                )
                =
                CHECKSUM(
                    ISNULL(CONVERT(nvarchar(20), t.[sport_id]), NCHAR(0)),
                    ISNULL(CONVERT(nvarchar(20), t.[league_id]), NCHAR(0)),
                    ISNULL(CONVERT(nvarchar(20), t.[home_team_id]), NCHAR(0)),
                    ISNULL(CONVERT(nvarchar(20), t.[away_team_id]), NCHAR(0)),
                    ISNULL(t.[slug], NCHAR(0)),
                    ISNULL(CONVERT(nvarchar(50), t.[match_date]), NCHAR(0)),
                    ISNULL(t.[match_type], NCHAR(0)),
                    ISNULL(CONVERT(nvarchar(20), t.[match_status]), NCHAR(0)),
                    ISNULL(t.[match_period], NCHAR(0)),
                    ISNULL(CONVERT(nvarchar(20), t.[score_home]), NCHAR(0)),
                    ISNULL(CONVERT(nvarchar(20), t.[score_away]), NCHAR(0)),
                    ISNULL(t.[time_left], NCHAR(0)),
                    ISNULL(t.[time_to_start], NCHAR(0)),
                    ISNULL(CONVERT(nvarchar(20), t.[time_to_start_min]), NCHAR(0)),
                    ISNULL(t.[home_win_prob], NCHAR(0)),
                    ISNULL(t.[away_win_prob], NCHAR(0)),
                    ISNULL(t.[draw_prob], NCHAR(0))
                )
            ) THEN UPDATE SET
                [sport_id] = s.[sport_id],
                [league_id] = s.[league_id],
                [home_team_id] = s.[home_team_id],
                [away_team_id] = s.[away_team_id],
                [slug] = s.[slug],
                [match_date] = s.[match_date],
                [match_type] = s.[match_type],
                [match_status] = s.[match_status],
                [match_period] = s.[match_period],
                [score_home] = s.[score_home],
                [score_away] = s.[score_away],
                [time_left] = s.[time_left],
                [time_to_start] = s.[time_to_start],
                [time_to_start_min] = s.[time_to_start_min],
                [home_win_prob] = s.[home_win_prob],
                [away_win_prob] = s.[away_win_prob],
                [draw_prob] = s.[draw_prob],
                [last_modified_at] = GETDATE()
            WHEN NOT MATCHED BY TARGET THEN INSERT (
                [id], [sport_id], [league_id], [home_team_id], [away_team_id], [slug], [match_date], [match_type], [match_status],
                [match_period], [score_home], [score_away], [time_left], [time_to_start], [time_to_start_min], [home_win_prob], [away_win_prob],
                [draw_prob], [last_modified_at]
            ) VALUES (
                s.[id], s.[sport_id], s.[league_id], s.[home_team_id], s.[away_team_id], s.[slug], s.[match_date], s.[match_type], s.[match_status],
                s.[match_period], s.[score_home], s.[score_away], s.[time_left], s.[time_to_start], s.[time_to_start_min], s.[home_win_prob], s.[away_win_prob],
                s.[draw_prob], GETDATE()
            );
        END
        ELSE IF COL_LENGTH(N'dbo.parlay_play_match', N'parlay_play_match_id') IS NOT NULL
        BEGIN
            ;MERGE [dbo].[parlay_play_match] AS t
            USING [dbo].[parlay_play_match_stage] AS s ON t.[parlay_play_match_id] = s.[id]
            WHEN MATCHED AND NOT (
                CHECKSUM(
                    ISNULL(CONVERT(nvarchar(20), s.[sport_id]), NCHAR(0)),
                    ISNULL(CONVERT(nvarchar(20), s.[league_id]), NCHAR(0)),
                    ISNULL(CONVERT(nvarchar(20), s.[home_team_id]), NCHAR(0)),
                    ISNULL(CONVERT(nvarchar(20), s.[away_team_id]), NCHAR(0)),
                    ISNULL(s.[slug], NCHAR(0)),
                    ISNULL(CONVERT(nvarchar(50), s.[match_date]), NCHAR(0)),
                    ISNULL(s.[match_type], NCHAR(0)),
                    ISNULL(CONVERT(nvarchar(20), s.[match_status]), NCHAR(0)),
                    ISNULL(s.[match_period], NCHAR(0)),
                    ISNULL(CONVERT(nvarchar(20), s.[score_home]), NCHAR(0)),
                    ISNULL(CONVERT(nvarchar(20), s.[score_away]), NCHAR(0)),
                    ISNULL(s.[time_left], NCHAR(0)),
                    ISNULL(s.[time_to_start], NCHAR(0)),
                    ISNULL(CONVERT(nvarchar(20), s.[time_to_start_min]), NCHAR(0)),
                    ISNULL(s.[home_win_prob], NCHAR(0)),
                    ISNULL(s.[away_win_prob], NCHAR(0)),
                    ISNULL(s.[draw_prob], NCHAR(0))
                )
                =
                CHECKSUM(
                    ISNULL(CONVERT(nvarchar(20), t.[sport_id]), NCHAR(0)),
                    ISNULL(CONVERT(nvarchar(20), t.[league_id]), NCHAR(0)),
                    ISNULL(CONVERT(nvarchar(20), t.[home_team_id]), NCHAR(0)),
                    ISNULL(CONVERT(nvarchar(20), t.[away_team_id]), NCHAR(0)),
                    ISNULL(t.[slug], NCHAR(0)),
                    ISNULL(CONVERT(nvarchar(50), t.[match_date]), NCHAR(0)),
                    ISNULL(t.[match_type], NCHAR(0)),
                    ISNULL(CONVERT(nvarchar(20), t.[match_status]), NCHAR(0)),
                    ISNULL(t.[match_period], NCHAR(0)),
                    ISNULL(CONVERT(nvarchar(20), t.[score_home]), NCHAR(0)),
                    ISNULL(CONVERT(nvarchar(20), t.[score_away]), NCHAR(0)),
                    ISNULL(t.[time_left], NCHAR(0)),
                    ISNULL(t.[time_to_start], NCHAR(0)),
                    ISNULL(CONVERT(nvarchar(20), t.[time_to_start_min]), NCHAR(0)),
                    ISNULL(t.[home_win_prob], NCHAR(0)),
                    ISNULL(t.[away_win_prob], NCHAR(0)),
                    ISNULL(t.[draw_prob], NCHAR(0))
                )
            ) THEN UPDATE SET
                [sport_id] = s.[sport_id],
                [league_id] = s.[league_id],
                [home_team_id] = s.[home_team_id],
                [away_team_id] = s.[away_team_id],
                [slug] = s.[slug],
                [match_date] = s.[match_date],
                [match_type] = s.[match_type],
                [match_status] = s.[match_status],
                [match_period] = s.[match_period],
                [score_home] = s.[score_home],
                [score_away] = s.[score_away],
                [time_left] = s.[time_left],
                [time_to_start] = s.[time_to_start],
                [time_to_start_min] = s.[time_to_start_min],
                [home_win_prob] = s.[home_win_prob],
                [away_win_prob] = s.[away_win_prob],
                [draw_prob] = s.[draw_prob],
                [last_modified_at] = GETDATE()
            WHEN NOT MATCHED BY TARGET THEN INSERT (
                [parlay_play_match_id], [sport_id], [league_id], [home_team_id], [away_team_id], [slug], [match_date], [match_type], [match_status],
                [match_period], [score_home], [score_away], [time_left], [time_to_start], [time_to_start_min], [home_win_prob], [away_win_prob],
                [draw_prob], [last_modified_at]
            ) VALUES (
                s.[id], s.[sport_id], s.[league_id], s.[home_team_id], s.[away_team_id], s.[slug], s.[match_date], s.[match_type], s.[match_status],
                s.[match_period], s.[score_home], s.[score_away], s.[time_left], s.[time_to_start], s.[time_to_start_min], s.[home_win_prob], s.[away_win_prob],
                s.[draw_prob], GETDATE()
            );
        END
        ELSE
            THROW 50001, 'parlay_play_match is missing expected PK column (id or parlay_play_match_id).', 1;
    END

    -- 5) player
    IF OBJECT_ID(N'[dbo].[parlay_play_player_stage]', N'U') IS NOT NULL
       AND OBJECT_ID(N'[dbo].[parlay_play_player]', N'U') IS NOT NULL
    BEGIN
        IF COL_LENGTH(N'dbo.parlay_play_player', N'id') IS NOT NULL
        BEGIN
            ;MERGE [dbo].[parlay_play_player] AS t
            USING [dbo].[parlay_play_player_stage] AS s ON t.[id] = s.[id]
            WHEN MATCHED AND NOT (
                    (t.[sport_id] = s.[sport_id] OR (t.[sport_id] IS NULL AND s.[sport_id] IS NULL))
                AND (t.[team_id] = s.[team_id] OR (t.[team_id] IS NULL AND s.[team_id] IS NULL))
                AND (t.[first_name] = s.[first_name] OR (t.[first_name] IS NULL AND s.[first_name] IS NULL))
                AND (t.[last_name] = s.[last_name] OR (t.[last_name] IS NULL AND s.[last_name] IS NULL))
                AND (t.[full_name] = s.[full_name] OR (t.[full_name] IS NULL AND s.[full_name] IS NULL))
                AND (t.[name_initial] = s.[name_initial] OR (t.[name_initial] IS NULL AND s.[name_initial] IS NULL))
                AND (t.[image] = s.[image] OR (t.[image] IS NULL AND s.[image] IS NULL))
                AND (t.[position] = s.[position] OR (t.[position] IS NULL AND s.[position] IS NULL))
                AND (t.[gender] = s.[gender] OR (t.[gender] IS NULL AND s.[gender] IS NULL))
                AND (t.[popularity] = s.[popularity] OR (t.[popularity] IS NULL AND s.[popularity] IS NULL))
                AND (t.[show_alt_lines] = s.[show_alt_lines] OR (t.[show_alt_lines] IS NULL AND s.[show_alt_lines] IS NULL))
            ) THEN UPDATE SET
                [sport_id] = s.[sport_id],
                [team_id] = s.[team_id],
                [first_name] = s.[first_name],
                [last_name] = s.[last_name],
                [full_name] = s.[full_name],
                [name_initial] = s.[name_initial],
                [image] = s.[image],
                [position] = s.[position],
                [gender] = s.[gender],
                [popularity] = s.[popularity],
                [show_alt_lines] = s.[show_alt_lines],
                [last_modified_at] = GETDATE()
            WHEN NOT MATCHED BY TARGET THEN INSERT (
                [id], [sport_id], [team_id], [first_name], [last_name], [full_name], [name_initial], [image], [position],
                [gender], [popularity], [show_alt_lines], [last_modified_at]
            ) VALUES (
                s.[id], s.[sport_id], s.[team_id], s.[first_name], s.[last_name], s.[full_name], s.[name_initial], s.[image], s.[position],
                s.[gender], s.[popularity], s.[show_alt_lines], GETDATE()
            );
        END
        ELSE IF COL_LENGTH(N'dbo.parlay_play_player', N'parlay_play_player_id') IS NOT NULL
        BEGIN
            ;MERGE [dbo].[parlay_play_player] AS t
            USING [dbo].[parlay_play_player_stage] AS s ON t.[parlay_play_player_id] = s.[id]
            WHEN MATCHED AND NOT (
                    (t.[sport_id] = s.[sport_id] OR (t.[sport_id] IS NULL AND s.[sport_id] IS NULL))
                AND (t.[team_id] = s.[team_id] OR (t.[team_id] IS NULL AND s.[team_id] IS NULL))
                AND (t.[first_name] = s.[first_name] OR (t.[first_name] IS NULL AND s.[first_name] IS NULL))
                AND (t.[last_name] = s.[last_name] OR (t.[last_name] IS NULL AND s.[last_name] IS NULL))
                AND (t.[full_name] = s.[full_name] OR (t.[full_name] IS NULL AND s.[full_name] IS NULL))
                AND (t.[name_initial] = s.[name_initial] OR (t.[name_initial] IS NULL AND s.[name_initial] IS NULL))
                AND (t.[image] = s.[image] OR (t.[image] IS NULL AND s.[image] IS NULL))
                AND (t.[position] = s.[position] OR (t.[position] IS NULL AND s.[position] IS NULL))
                AND (t.[gender] = s.[gender] OR (t.[gender] IS NULL AND s.[gender] IS NULL))
                AND (t.[popularity] = s.[popularity] OR (t.[popularity] IS NULL AND s.[popularity] IS NULL))
                AND (t.[show_alt_lines] = s.[show_alt_lines] OR (t.[show_alt_lines] IS NULL AND s.[show_alt_lines] IS NULL))
            ) THEN UPDATE SET
                [sport_id] = s.[sport_id],
                [team_id] = s.[team_id],
                [first_name] = s.[first_name],
                [last_name] = s.[last_name],
                [full_name] = s.[full_name],
                [name_initial] = s.[name_initial],
                [image] = s.[image],
                [position] = s.[position],
                [gender] = s.[gender],
                [popularity] = s.[popularity],
                [show_alt_lines] = s.[show_alt_lines],
                [last_modified_at] = GETDATE()
            WHEN NOT MATCHED BY TARGET THEN INSERT (
                [parlay_play_player_id], [sport_id], [team_id], [first_name], [last_name], [full_name], [name_initial], [image], [position],
                [gender], [popularity], [show_alt_lines], [last_modified_at]
            ) VALUES (
                s.[id], s.[sport_id], s.[team_id], s.[first_name], s.[last_name], s.[full_name], s.[name_initial], s.[image], s.[position],
                s.[gender], s.[popularity], s.[show_alt_lines], GETDATE()
            );
        END
        ELSE
            THROW 50005, 'parlay_play_player is missing expected PK column (id or parlay_play_player_id).', 1;
    END

    -- 6) stat_type
    IF OBJECT_ID(N'[dbo].[parlay_play_stat_type_stage]', N'U') IS NOT NULL
       AND OBJECT_ID(N'[dbo].[parlay_play_stat_type]', N'U') IS NOT NULL
    BEGIN
        ;MERGE [dbo].[parlay_play_stat_type] AS t
        USING [dbo].[parlay_play_stat_type_stage] AS s ON t.[challenge_option] = s.[challenge_option]
        WHEN MATCHED AND NOT (
                (t.[challenge_name] = s.[challenge_name] OR (t.[challenge_name] IS NULL AND s.[challenge_name] IS NULL))
            AND (t.[challenge_units] = s.[challenge_units] OR (t.[challenge_units] IS NULL AND s.[challenge_units] IS NULL))
        ) THEN UPDATE SET
            [challenge_name] = s.[challenge_name],
            [challenge_units] = s.[challenge_units],
            [last_modified_at] = GETDATE()
        WHEN NOT MATCHED BY TARGET THEN INSERT (
            [challenge_option], [challenge_name], [challenge_units], [last_modified_at]
        ) VALUES (
            s.[challenge_option], s.[challenge_name], s.[challenge_units], GETDATE()
        );
    END

    -- 7) projection (wide) - use CHECKSUM for change detection
    IF OBJECT_ID(N'[dbo].[parlay_play_projection_stage]', N'U') IS NOT NULL
       AND OBJECT_ID(N'[dbo].[parlay_play_projection]', N'U') IS NOT NULL
    BEGIN
        ;MERGE [dbo].[parlay_play_projection] AS t
        USING [dbo].[parlay_play_projection_stage] AS s ON t.[projection_id] = s.[projection_id]
        WHEN MATCHED AND NOT (
            CHECKSUM(
                ISNULL(CONVERT(nvarchar(20), s.[match_id]), NCHAR(0)),
                ISNULL(CONVERT(nvarchar(20), s.[player_id]), NCHAR(0)),
                ISNULL(s.[challenge_option], NCHAR(0)),
                ISNULL(CONVERT(nvarchar(50), s.[line_score]), NCHAR(0)),
                ISNULL(CONVERT(tinyint, s.[is_main_line]), 0),
                ISNULL(CONVERT(nvarchar(50), s.[decimal_price_over]), NCHAR(0)),
                ISNULL(CONVERT(nvarchar(50), s.[decimal_price_under]), NCHAR(0)),
                ISNULL(s.[market_name], NCHAR(0)),
                ISNULL(s.[match_period], NCHAR(0)),
                ISNULL(CONVERT(tinyint, s.[show_default]), 0),
                ISNULL(s.[display_name], NCHAR(0)),
                ISNULL(s.[stat_type_name], NCHAR(0)),
                ISNULL(CONVERT(nvarchar(40), s.[start_time]), NCHAR(0)),
                ISNULL(CONVERT(nvarchar(50), s.[promo_deadline]), NCHAR(0)),
                ISNULL(CONVERT(nvarchar(50), s.[promo_max_entry]), NCHAR(0)),
                ISNULL(CONVERT(nvarchar(20), s.[player_promo_id]), NCHAR(0)),
                ISNULL(s.[player_promo_type], NCHAR(0)),
                ISNULL(CONVERT(tinyint, s.[is_boosted_payout]), 0),
                ISNULL(CONVERT(tinyint, s.[is_player_promo]), 0),
                ISNULL(CONVERT(nvarchar(50), s.[default_multiplier]), NCHAR(0)),
                ISNULL(CONVERT(nvarchar(50), s.[promo_multiplier]), NCHAR(0)),
                ISNULL(s.[payout_boost_selection], NCHAR(0)),
                ISNULL(CONVERT(tinyint, s.[is_public]), 0),
                ISNULL(CONVERT(tinyint, s.[is_slashed_line]), 0),
                ISNULL(CONVERT(nvarchar(20), s.[alt_line_count]), NCHAR(0))
            )
            =
            CHECKSUM(
                ISNULL(CONVERT(nvarchar(20), t.[match_id]), NCHAR(0)),
                ISNULL(CONVERT(nvarchar(20), t.[player_id]), NCHAR(0)),
                ISNULL(t.[challenge_option], NCHAR(0)),
                ISNULL(CONVERT(nvarchar(50), t.[line_score]), NCHAR(0)),
                ISNULL(CONVERT(tinyint, t.[is_main_line]), 0),
                ISNULL(CONVERT(nvarchar(50), t.[decimal_price_over]), NCHAR(0)),
                ISNULL(CONVERT(nvarchar(50), t.[decimal_price_under]), NCHAR(0)),
                ISNULL(t.[market_name], NCHAR(0)),
                ISNULL(t.[match_period], NCHAR(0)),
                ISNULL(CONVERT(tinyint, t.[show_default]), 0),
                ISNULL(t.[display_name], NCHAR(0)),
                ISNULL(t.[stat_type_name], NCHAR(0)),
                ISNULL(CONVERT(nvarchar(40), t.[start_time]), NCHAR(0)),
                ISNULL(CONVERT(nvarchar(50), t.[promo_deadline]), NCHAR(0)),
                ISNULL(CONVERT(nvarchar(50), t.[promo_max_entry]), NCHAR(0)),
                ISNULL(CONVERT(nvarchar(20), t.[player_promo_id]), NCHAR(0)),
                ISNULL(t.[player_promo_type], NCHAR(0)),
                ISNULL(CONVERT(tinyint, t.[is_boosted_payout]), 0),
                ISNULL(CONVERT(tinyint, t.[is_player_promo]), 0),
                ISNULL(CONVERT(nvarchar(50), t.[default_multiplier]), NCHAR(0)),
                ISNULL(CONVERT(nvarchar(50), t.[promo_multiplier]), NCHAR(0)),
                ISNULL(t.[payout_boost_selection], NCHAR(0)),
                ISNULL(CONVERT(tinyint, t.[is_public]), 0),
                ISNULL(CONVERT(tinyint, t.[is_slashed_line]), 0),
                ISNULL(CONVERT(nvarchar(20), t.[alt_line_count]), NCHAR(0))
            )
        ) THEN UPDATE SET
            [match_id] = s.[match_id],
            [player_id] = s.[player_id],
            [challenge_option] = s.[challenge_option],
            [line_score] = s.[line_score],
            [is_main_line] = s.[is_main_line],
            [decimal_price_over] = s.[decimal_price_over],
            [decimal_price_under] = s.[decimal_price_under],
            [market_name] = s.[market_name],
            [match_period] = s.[match_period],
            [show_default] = s.[show_default],
            [display_name] = s.[display_name],
            [stat_type_name] = s.[stat_type_name],
            [start_time] = s.[start_time],
            [promo_deadline] = s.[promo_deadline],
            [promo_max_entry] = s.[promo_max_entry],
            [player_promo_id] = s.[player_promo_id],
            [player_promo_type] = s.[player_promo_type],
            [is_boosted_payout] = s.[is_boosted_payout],
            [is_player_promo] = s.[is_player_promo],
            [default_multiplier] = s.[default_multiplier],
            [promo_multiplier] = s.[promo_multiplier],
            [payout_boost_selection] = s.[payout_boost_selection],
            [is_public] = s.[is_public],
            [is_slashed_line] = s.[is_slashed_line],
            [alt_line_count] = s.[alt_line_count],
            [last_modified_at] = GETDATE()
        WHEN NOT MATCHED BY TARGET THEN INSERT (
            [projection_id], [match_id], [player_id], [challenge_option], [line_score], [is_main_line], [decimal_price_over], [decimal_price_under],
            [market_name], [match_period], [show_default], [display_name], [stat_type_name], [start_time], [promo_deadline], [promo_max_entry],
            [player_promo_id], [player_promo_type], [is_boosted_payout], [is_player_promo], [default_multiplier], [promo_multiplier],
            [payout_boost_selection], [is_public], [is_slashed_line], [alt_line_count], [last_modified_at]
        ) VALUES (
            s.[projection_id], s.[match_id], s.[player_id], s.[challenge_option], s.[line_score], s.[is_main_line], s.[decimal_price_over], s.[decimal_price_under],
            s.[market_name], s.[match_period], s.[show_default], s.[display_name], s.[stat_type_name], s.[start_time], s.[promo_deadline], s.[promo_max_entry],
            s.[player_promo_id], s.[player_promo_type], s.[is_boosted_payout], s.[is_player_promo], s.[default_multiplier], s.[promo_multiplier],
            s.[payout_boost_selection], s.[is_public], s.[is_slashed_line], s.[alt_line_count], GETDATE()
        );
    END
END
GO

