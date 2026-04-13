"""Sync sportsbook stage tables into a unified projection table."""

from __future__ import annotations


def _get_db_conn(
    server: str,
    database: str,
    user: str,
    password: str,
    trusted_connection: bool = False,
):
    import pyodbc

    if trusted_connection:
        conn_str = (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={server};DATABASE={database};Trusted_Connection=yes;"
        )
    else:
        conn_str = (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={server};DATABASE={database};UID={user};PWD={password or ''}"
        )
    try:
        return pyodbc.connect(conn_str)
    except pyodbc.Error:
        if trusted_connection:
            conn_str = (
                f"DRIVER={{SQL Server}};"
                f"SERVER={server};DATABASE={database};Trusted_Connection=yes;"
            )
        else:
            conn_str = (
                f"DRIVER={{SQL Server}};"
                f"SERVER={server};DATABASE={database};UID={user};PWD={password or ''}"
            )
        return pyodbc.connect(conn_str)


def ensure_sportsbook_projection_table(conn) -> None:
    cursor = conn.cursor()
    cursor.execute(
        """
        IF NOT EXISTS (
            SELECT 1 FROM sys.tables WHERE name = 'sportsbook_projection' AND schema_id = SCHEMA_ID('dbo')
        )
        BEGIN
            CREATE TABLE [dbo].[sportsbook_projection](
                [sportsbook] [nvarchar](30) NOT NULL,
                [external_projection_id] [bigint] NOT NULL,
                [source_player_id] [nvarchar](64) NULL,
                [source_game_id] [nvarchar](64) NULL,
                [player_name] [nvarchar](255) NOT NULL,
                [stat_type_name] [nvarchar](120) NOT NULL,
                [line_score] [decimal](10, 2) NULL,
                [odds_type] [nvarchar](50) NULL,
                [start_time] [datetime2](3) NULL,
                [league_id] [int] NULL,
                [team] [nvarchar](50) NULL,
                [team_name] [nvarchar](100) NULL,
                [home_abbreviation] [nvarchar](10) NULL,
                [away_abbreviation] [nvarchar](10) NULL,
                [opponent_abbreviation] [nvarchar](10) NULL,
                [home_away] [nvarchar](1) NULL,
                [event_name] [nvarchar](200) NULL,
                [extra_json] [nvarchar](max) NULL,
                [created_at] [datetime2](7) NOT NULL
                    CONSTRAINT [DF_sportsbook_projection_created_at]
                    DEFAULT (CONVERT(datetime2(7), SYSUTCDATETIME() AT TIME ZONE 'UTC' AT TIME ZONE 'Central Standard Time')),
                [last_modified_at] [datetime2](7) NOT NULL
                    CONSTRAINT [DF_sportsbook_projection_last_modified_at]
                    DEFAULT (CONVERT(datetime2(7), SYSUTCDATETIME() AT TIME ZONE 'UTC' AT TIME ZONE 'Central Standard Time')),
                CONSTRAINT [PK_sportsbook_projection] PRIMARY KEY CLUSTERED ([sportsbook] ASC, [external_projection_id] ASC)
            ) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY];
        END;

        IF NOT EXISTS (
            SELECT 1 FROM sys.tables WHERE name = 'sportsbook_projection_history' AND schema_id = SCHEMA_ID('dbo')
        )
        BEGIN
            CREATE TABLE [dbo].[sportsbook_projection_history](
                [sportsbook] [nvarchar](30) NOT NULL,
                [external_projection_id] [bigint] NOT NULL,
                [source_player_id] [nvarchar](64) NULL,
                [source_game_id] [nvarchar](64) NULL,
                [player_name] [nvarchar](255) NOT NULL,
                [stat_type_name] [nvarchar](120) NOT NULL,
                [line_score] [decimal](10, 2) NULL,
                [odds_type] [nvarchar](50) NULL,
                [start_time] [datetime2](3) NULL,
                [league_id] [int] NULL,
                [team] [nvarchar](50) NULL,
                [team_name] [nvarchar](100) NULL,
                [home_abbreviation] [nvarchar](10) NULL,
                [away_abbreviation] [nvarchar](10) NULL,
                [opponent_abbreviation] [nvarchar](10) NULL,
                [home_away] [nvarchar](1) NULL,
                [event_name] [nvarchar](200) NULL,
                [extra_json] [nvarchar](max) NULL,
                [created_at] [datetime2](7) NOT NULL,
                [last_modified_at] [datetime2](7) NOT NULL,
                [archived_at] [datetime2](7) NOT NULL
                    CONSTRAINT [DF_sportsbook_projection_history_archived_at]
                    DEFAULT (CONVERT(datetime2(7), SYSUTCDATETIME() AT TIME ZONE 'UTC' AT TIME ZONE 'Central Standard Time')),
                [archive_reason] [nvarchar](40) NOT NULL
                    CONSTRAINT [DF_sportsbook_projection_history_archive_reason]
                    DEFAULT (N'unknown')
            ) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY];
        END;

        IF NOT EXISTS (
            SELECT 1 FROM sys.indexes
            WHERE name = 'IX_sportsbook_projection_player_time'
              AND object_id = OBJECT_ID('dbo.sportsbook_projection')
        )
        BEGIN
            CREATE NONCLUSTERED INDEX [IX_sportsbook_projection_player_time]
                ON [dbo].[sportsbook_projection]([player_name], [start_time], [stat_type_name])
                INCLUDE ([sportsbook], [line_score], [odds_type], [league_id], [team], [team_name]);
        END;

        IF NOT EXISTS (
            SELECT 1 FROM sys.indexes
            WHERE name = 'IX_sportsbook_projection_book_time'
              AND object_id = OBJECT_ID('dbo.sportsbook_projection')
        )
        BEGIN
            CREATE NONCLUSTERED INDEX [IX_sportsbook_projection_book_time]
                ON [dbo].[sportsbook_projection]([sportsbook], [start_time]);
        END;

        IF NOT EXISTS (
            SELECT 1 FROM sys.indexes
            WHERE name = 'IX_sportsbook_projection_history_book_time'
              AND object_id = OBJECT_ID('dbo.sportsbook_projection_history')
        )
        BEGIN
            CREATE NONCLUSTERED INDEX [IX_sportsbook_projection_history_book_time]
                ON [dbo].[sportsbook_projection_history]([sportsbook], [start_time], [archived_at]);
        END;
        """
    )
    conn.commit()


def _archive_missing_from_stage(conn, sportsbook: str, stage_table: str) -> None:
    """Archive rows not present in the latest sportsbook stage snapshot, then delete them."""
    cursor = conn.cursor()
    cursor.execute(
        f"""
        DECLARE @nowCentral datetime2(7) =
            CONVERT(datetime2(7), SYSUTCDATETIME() AT TIME ZONE 'UTC' AT TIME ZONE 'Central Standard Time');

        IF EXISTS (SELECT 1 FROM [dbo].[{stage_table}])
        BEGIN
            INSERT INTO [dbo].[sportsbook_projection_history](
                sportsbook, external_projection_id, source_player_id, source_game_id,
                player_name, stat_type_name, line_score, odds_type, start_time, league_id,
                team, team_name, home_abbreviation, away_abbreviation, opponent_abbreviation,
                home_away, event_name, extra_json, created_at, last_modified_at,
                archived_at, archive_reason
            )
            SELECT
                p.sportsbook, p.external_projection_id, p.source_player_id, p.source_game_id,
                p.player_name, p.stat_type_name, p.line_score, p.odds_type, p.start_time, p.league_id,
                p.team, p.team_name, p.home_abbreviation, p.away_abbreviation, p.opponent_abbreviation,
                p.home_away, p.event_name, p.extra_json, p.created_at, p.last_modified_at,
                @nowCentral, N'no_longer_active'
            FROM [dbo].[sportsbook_projection] p
            WHERE p.sportsbook = ?
              AND NOT EXISTS (
                  SELECT 1
                  FROM [dbo].[{stage_table}] s
                  WHERE CAST(s.projection_id AS bigint) = p.external_projection_id
              )
              AND NOT EXISTS (
                  SELECT 1
                  FROM [dbo].[sportsbook_projection_history] h
                  WHERE h.sportsbook = p.sportsbook
                    AND h.external_projection_id = p.external_projection_id
                    AND ((h.start_time = p.start_time) OR (h.start_time IS NULL AND p.start_time IS NULL))
                    AND h.archive_reason = N'no_longer_active'
              );

            DELETE p
            FROM [dbo].[sportsbook_projection] p
            WHERE p.sportsbook = ?
              AND NOT EXISTS (
                  SELECT 1
                  FROM [dbo].[{stage_table}] s
                  WHERE CAST(s.projection_id AS bigint) = p.external_projection_id
              );
        END;
        """,
        (sportsbook, sportsbook),
    )


def _sync_prizepicks_from_stage(conn) -> None:
    cursor = conn.cursor()
    cursor.execute(
        """
        ;WITH src AS (
            SELECT
                CAST(s.projection_id AS bigint) AS external_projection_id,
                CAST(s.player_id AS nvarchar(64)) AS source_player_id,
                CAST(s.game_id AS nvarchar(64)) AS source_game_id,
                COALESCE(
                    NULLIF(LTRIM(RTRIM(pp.display_name)), N''),
                    NULLIF(LTRIM(RTRIM(pp.name)), N''),
                    N'Player ' + CAST(s.player_id AS nvarchar(64))
                ) AS player_name,
                LTRIM(RTRIM(COALESCE(s.stat_type_name, N''))) AS stat_type_name,
                s.line_score,
                LTRIM(RTRIM(COALESCE(s.odds_type, N''))) AS odds_type,
                CAST(s.start_time AS datetime2(3)) AS start_time,
                s.league_id,
                NULLIF(LTRIM(RTRIM(pp.team)), N'') AS team,
                NULLIF(LTRIM(RTRIM(pp.team_name)), N'') AS team_name,
                NULLIF(LTRIM(RTRIM(g.home_abbreviation)), N'') AS home_abbreviation,
                NULLIF(LTRIM(RTRIM(g.away_abbreviation)), N'') AS away_abbreviation,
                CASE
                    WHEN pp.team IS NOT NULL AND g.home_abbreviation IS NOT NULL AND UPPER(LTRIM(RTRIM(pp.team))) = UPPER(LTRIM(RTRIM(g.home_abbreviation)))
                        THEN NULLIF(LTRIM(RTRIM(g.away_abbreviation)), N'')
                    WHEN pp.team IS NOT NULL AND g.away_abbreviation IS NOT NULL AND UPPER(LTRIM(RTRIM(pp.team))) = UPPER(LTRIM(RTRIM(g.away_abbreviation)))
                        THEN NULLIF(LTRIM(RTRIM(g.home_abbreviation)), N'')
                    ELSE NULL
                END AS opponent_abbreviation,
                CASE
                    WHEN pp.team IS NOT NULL AND g.home_abbreviation IS NOT NULL AND UPPER(LTRIM(RTRIM(pp.team))) = UPPER(LTRIM(RTRIM(g.home_abbreviation)))
                        THEN N'H'
                    WHEN pp.team IS NOT NULL AND g.away_abbreviation IS NOT NULL AND UPPER(LTRIM(RTRIM(pp.team))) = UPPER(LTRIM(RTRIM(g.away_abbreviation)))
                        THEN N'A'
                    ELSE NULL
                END AS home_away,
                NULL AS event_name,
                NULL AS extra_json
            FROM [dbo].[prizepicks_projection_stage] s
            LEFT JOIN [dbo].[prizepicks_player] pp
                ON pp.player_id = CAST(s.player_id AS nvarchar(20))
            LEFT JOIN [dbo].[prizepicks_game] g
                ON g.game_id = CAST(s.game_id AS nvarchar(20))
            WHERE s.player_id IS NOT NULL
              AND LTRIM(RTRIM(COALESCE(s.stat_type_name, N''))) <> N''
        )
        MERGE [dbo].[sportsbook_projection] AS t
        USING src AS s
            ON t.sportsbook = N'prizepicks'
           AND t.external_projection_id = s.external_projection_id
        WHEN MATCHED THEN
            UPDATE SET
                t.source_player_id = s.source_player_id,
                t.source_game_id = s.source_game_id,
                t.player_name = s.player_name,
                t.stat_type_name = s.stat_type_name,
                t.line_score = s.line_score,
                t.odds_type = s.odds_type,
                t.start_time = s.start_time,
                t.league_id = s.league_id,
                t.team = s.team,
                t.team_name = s.team_name,
                t.home_abbreviation = s.home_abbreviation,
                t.away_abbreviation = s.away_abbreviation,
                t.opponent_abbreviation = s.opponent_abbreviation,
                t.home_away = s.home_away,
                t.event_name = s.event_name,
                t.extra_json = s.extra_json,
                t.last_modified_at = CONVERT(datetime2(7), SYSUTCDATETIME() AT TIME ZONE 'UTC' AT TIME ZONE 'Central Standard Time')
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (
                sportsbook, external_projection_id, source_player_id, source_game_id,
                player_name, stat_type_name, line_score, odds_type, start_time, league_id,
                team, team_name, home_abbreviation, away_abbreviation, opponent_abbreviation,
                home_away, event_name, extra_json, created_at, last_modified_at
            )
            VALUES (
                N'prizepicks', s.external_projection_id, s.source_player_id, s.source_game_id,
                s.player_name, s.stat_type_name, s.line_score, s.odds_type, s.start_time, s.league_id,
                s.team, s.team_name, s.home_abbreviation, s.away_abbreviation, s.opponent_abbreviation,
                s.home_away, s.event_name, s.extra_json,
                CONVERT(datetime2(7), SYSUTCDATETIME() AT TIME ZONE 'UTC' AT TIME ZONE 'Central Standard Time'),
                CONVERT(datetime2(7), SYSUTCDATETIME() AT TIME ZONE 'UTC' AT TIME ZONE 'Central Standard Time')
            )
        -- Missing rows are archived/deleted by _archive_missing_from_stage.
        """
    )


def _sync_underdog_from_stage(conn) -> None:
    cursor = conn.cursor()
    cursor.execute(
        """
        ;WITH ranked_stage AS (
            SELECT
                s.*,
                ROW_NUMBER() OVER (
                    PARTITION BY s.projection_id
                    ORDER BY
                        ISNULL(s.updated_at, N'') DESC,
                        ISNULL(CAST(s.start_time AS datetime2(3)), CAST('1900-01-01' AS datetime2(3))) DESC
                ) AS rn
            FROM [dbo].[underdog_projection_stage] s
        ),
        src AS (
            SELECT
                CAST(s.projection_id AS bigint) AS external_projection_id,
                COALESCE(CAST(ua.player_id AS nvarchar(64)), CAST(s.underdog_player_id AS nvarchar(64))) AS source_player_id,
                CAST(ua.match_id AS nvarchar(64)) AS source_game_id,
                COALESCE(
                    NULLIF(LTRIM(RTRIM(s.display_name)), N''),
                    NULLIF(LTRIM(RTRIM(CONCAT(up.first_name, N' ', up.last_name))), N''),
                    N'Player ' + COALESCE(CAST(ua.player_id AS nvarchar(64)), CAST(s.underdog_player_id AS nvarchar(64)), N'0')
                ) AS player_name,
                LTRIM(RTRIM(COALESCE(s.stat_type_name, N''))) AS stat_type_name,
                s.line_score,
                LTRIM(RTRIM(COALESCE(CAST(s.line_type AS nvarchar(50)), N''))) AS odds_type,
                CAST(s.start_time AS datetime2(3)) AS start_time,
                TRY_CAST(ug.sport_id AS int) AS league_id,
                CAST(ua.team_id AS nvarchar(50)) AS team,
                NULL AS team_name,
                NULL AS home_abbreviation,
                NULL AS away_abbreviation,
                NULL AS opponent_abbreviation,
                NULL AS home_away,
                NULLIF(LTRIM(RTRIM(ug.title)), N'') AS event_name,
                NULL AS extra_json
            FROM ranked_stage s
            LEFT JOIN [dbo].[underdog_appearance] ua
                ON ua.id = s.appearance_id
            LEFT JOIN [dbo].[underdog_player] up
                ON up.id = ua.player_id
            LEFT JOIN [dbo].[underdog_game] ug
                ON ug.id = ua.match_id
            WHERE s.rn = 1
              AND LTRIM(RTRIM(COALESCE(s.stat_type_name, N''))) <> N''
        )
        MERGE [dbo].[sportsbook_projection] AS t
        USING src AS s
            ON t.sportsbook = N'underdog'
           AND t.external_projection_id = s.external_projection_id
        WHEN MATCHED THEN
            UPDATE SET
                t.source_player_id = s.source_player_id,
                t.source_game_id = s.source_game_id,
                t.player_name = s.player_name,
                t.stat_type_name = s.stat_type_name,
                t.line_score = s.line_score,
                t.odds_type = s.odds_type,
                t.start_time = s.start_time,
                t.league_id = s.league_id,
                t.team = s.team,
                t.team_name = s.team_name,
                t.home_abbreviation = s.home_abbreviation,
                t.away_abbreviation = s.away_abbreviation,
                t.opponent_abbreviation = s.opponent_abbreviation,
                t.home_away = s.home_away,
                t.event_name = s.event_name,
                t.extra_json = s.extra_json,
                t.last_modified_at = CONVERT(datetime2(7), SYSUTCDATETIME() AT TIME ZONE 'UTC' AT TIME ZONE 'Central Standard Time')
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (
                sportsbook, external_projection_id, source_player_id, source_game_id,
                player_name, stat_type_name, line_score, odds_type, start_time, league_id,
                team, team_name, home_abbreviation, away_abbreviation, opponent_abbreviation,
                home_away, event_name, extra_json, created_at, last_modified_at
            )
            VALUES (
                N'underdog', s.external_projection_id, s.source_player_id, s.source_game_id,
                s.player_name, s.stat_type_name, s.line_score, s.odds_type, s.start_time, s.league_id,
                s.team, s.team_name, s.home_abbreviation, s.away_abbreviation, s.opponent_abbreviation,
                s.home_away, s.event_name, s.extra_json,
                CONVERT(datetime2(7), SYSUTCDATETIME() AT TIME ZONE 'UTC' AT TIME ZONE 'Central Standard Time'),
                CONVERT(datetime2(7), SYSUTCDATETIME() AT TIME ZONE 'UTC' AT TIME ZONE 'Central Standard Time')
            )
        -- Missing rows are archived/deleted by _archive_missing_from_stage.
        """
    )


def _sync_parlay_play_from_stage(conn) -> None:
    cursor = conn.cursor()
    cursor.execute(
        """
        ;WITH ranked_stage AS (
            SELECT
                s.*,
                ROW_NUMBER() OVER (
                    PARTITION BY s.projection_id
                    ORDER BY
                        CASE WHEN s.is_main_line = 1 THEN 0 ELSE 1 END,
                        ISNULL(s.alt_line_count, 0) DESC,
                        ISNULL(s.line_score, -1) DESC
                ) AS rn
            FROM [dbo].[parlay_play_projection_stage] s
        ),
        src AS (
            SELECT
                CAST(s.projection_id AS bigint) AS external_projection_id,
                CAST(s.player_id AS nvarchar(64)) AS source_player_id,
                CAST(s.match_id AS nvarchar(64)) AS source_game_id,
                COALESCE(
                    NULLIF(LTRIM(RTRIM(s.display_name)), N''),
                    NULLIF(LTRIM(RTRIM(pp.full_name)), N''),
                    N'Player ' + CAST(s.player_id AS nvarchar(64))
                ) AS player_name,
                LTRIM(RTRIM(COALESCE(s.stat_type_name, N''))) AS stat_type_name,
                s.line_score,
                CASE WHEN s.is_main_line = 1 THEN N'main' ELSE N'alt' END AS odds_type,
                CAST(s.start_time AS datetime2(3)) AS start_time,
                m.league_id,
                COALESCE(NULLIF(LTRIM(RTRIM(pt.team_abbreviation)), N''), NULLIF(LTRIM(RTRIM(pt.teamname_abbr)), N'')) AS team,
                NULLIF(LTRIM(RTRIM(pt.teamname)), N'') AS team_name,
                COALESCE(NULLIF(LTRIM(RTRIM(ht.team_abbreviation)), N''), NULLIF(LTRIM(RTRIM(ht.teamname_abbr)), N'')) AS home_abbreviation,
                COALESCE(NULLIF(LTRIM(RTRIM(at.team_abbreviation)), N''), NULLIF(LTRIM(RTRIM(at.teamname_abbr)), N'')) AS away_abbreviation,
                CASE
                    WHEN pp.team_id = m.home_team_id THEN COALESCE(NULLIF(LTRIM(RTRIM(at.team_abbreviation)), N''), NULLIF(LTRIM(RTRIM(at.teamname_abbr)), N''))
                    WHEN pp.team_id = m.away_team_id THEN COALESCE(NULLIF(LTRIM(RTRIM(ht.team_abbreviation)), N''), NULLIF(LTRIM(RTRIM(ht.teamname_abbr)), N''))
                    ELSE NULL
                END AS opponent_abbreviation,
                CASE
                    WHEN pp.team_id = m.home_team_id THEN N'H'
                    WHEN pp.team_id = m.away_team_id THEN N'A'
                    ELSE NULL
                END AS home_away,
                NULLIF(LTRIM(RTRIM(m.slug)), N'') AS event_name,
                NULL AS extra_json
            FROM ranked_stage s
            LEFT JOIN [dbo].[parlay_play_player] pp
                ON pp.id = s.player_id
            LEFT JOIN [dbo].[parlay_play_match] m
                ON m.id = s.match_id
            LEFT JOIN [dbo].[parlay_play_team] pt
                ON pt.id = pp.team_id
            LEFT JOIN [dbo].[parlay_play_team] ht
                ON ht.id = m.home_team_id
            LEFT JOIN [dbo].[parlay_play_team] at
                ON at.id = m.away_team_id
            WHERE s.rn = 1
              AND LTRIM(RTRIM(COALESCE(s.stat_type_name, N''))) <> N''
        )
        MERGE [dbo].[sportsbook_projection] AS t
        USING src AS s
            ON t.sportsbook = N'parlay_play'
           AND t.external_projection_id = s.external_projection_id
        WHEN MATCHED THEN
            UPDATE SET
                t.source_player_id = s.source_player_id,
                t.source_game_id = s.source_game_id,
                t.player_name = s.player_name,
                t.stat_type_name = s.stat_type_name,
                t.line_score = s.line_score,
                t.odds_type = s.odds_type,
                t.start_time = s.start_time,
                t.league_id = s.league_id,
                t.team = s.team,
                t.team_name = s.team_name,
                t.home_abbreviation = s.home_abbreviation,
                t.away_abbreviation = s.away_abbreviation,
                t.opponent_abbreviation = s.opponent_abbreviation,
                t.home_away = s.home_away,
                t.event_name = s.event_name,
                t.extra_json = s.extra_json,
                t.last_modified_at = CONVERT(datetime2(7), SYSUTCDATETIME() AT TIME ZONE 'UTC' AT TIME ZONE 'Central Standard Time')
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (
                sportsbook, external_projection_id, source_player_id, source_game_id,
                player_name, stat_type_name, line_score, odds_type, start_time, league_id,
                team, team_name, home_abbreviation, away_abbreviation, opponent_abbreviation,
                home_away, event_name, extra_json, created_at, last_modified_at
            )
            VALUES (
                N'parlay_play', s.external_projection_id, s.source_player_id, s.source_game_id,
                s.player_name, s.stat_type_name, s.line_score, s.odds_type, s.start_time, s.league_id,
                s.team, s.team_name, s.home_abbreviation, s.away_abbreviation, s.opponent_abbreviation,
                s.home_away, s.event_name, s.extra_json,
                CONVERT(datetime2(7), SYSUTCDATETIME() AT TIME ZONE 'UTC' AT TIME ZONE 'Central Standard Time'),
                CONVERT(datetime2(7), SYSUTCDATETIME() AT TIME ZONE 'UTC' AT TIME ZONE 'Central Standard Time')
            )
        -- Missing rows are archived/deleted by _archive_missing_from_stage.
        """
    )


def _archive_started_projections(conn) -> None:
    cursor = conn.cursor()
    cursor.execute(
        """
        DECLARE @nowCentral datetime2(7) =
            CONVERT(datetime2(7), SYSUTCDATETIME() AT TIME ZONE 'UTC' AT TIME ZONE 'Central Standard Time');

        INSERT INTO [dbo].[sportsbook_projection_history](
            sportsbook, external_projection_id, source_player_id, source_game_id,
            player_name, stat_type_name, line_score, odds_type, start_time, league_id,
            team, team_name, home_abbreviation, away_abbreviation, opponent_abbreviation,
            home_away, event_name, extra_json, created_at, last_modified_at,
            archived_at, archive_reason
        )
        SELECT
            p.sportsbook, p.external_projection_id, p.source_player_id, p.source_game_id,
            p.player_name, p.stat_type_name, p.line_score, p.odds_type, p.start_time, p.league_id,
            p.team, p.team_name, p.home_abbreviation, p.away_abbreviation, p.opponent_abbreviation,
            p.home_away, p.event_name, p.extra_json, p.created_at, p.last_modified_at,
            @nowCentral, N'game_started'
        FROM [dbo].[sportsbook_projection] p
        WHERE p.start_time IS NOT NULL
          AND p.start_time < @nowCentral
          AND NOT EXISTS (
              SELECT 1
              FROM [dbo].[sportsbook_projection_history] h
              WHERE h.sportsbook = p.sportsbook
                AND h.external_projection_id = p.external_projection_id
                AND ((h.start_time = p.start_time) OR (h.start_time IS NULL AND p.start_time IS NULL))
                AND h.archive_reason = N'game_started'
          );

        DELETE p
        FROM [dbo].[sportsbook_projection] p
        WHERE p.start_time IS NOT NULL
          AND p.start_time < @nowCentral;
        """
    )


def sync_sportsbook_projection_snapshot(
    sportsbook: str,
    server: str = "localhost\\SQLEXPRESS",
    database: str = "Props",
    user: str = "dbadmin",
    password: str = "",
    trusted_connection: bool = False,
) -> int:
    """Upsert a sportsbook stage snapshot into dbo.sportsbook_projection."""

    sb = (sportsbook or "").strip().lower()
    if sb not in {"prizepicks", "underdog", "parlay_play"}:
        raise ValueError("sportsbook must be one of: prizepicks, underdog, parlay_play")

    conn = _get_db_conn(server, database, user, password, trusted_connection)
    try:
        ensure_sportsbook_projection_table(conn)
        with conn:
            if sb == "prizepicks":
                _sync_prizepicks_from_stage(conn)
                _archive_missing_from_stage(conn, "prizepicks", "prizepicks_projection_stage")
            elif sb == "underdog":
                _sync_underdog_from_stage(conn)
                _archive_missing_from_stage(conn, "underdog", "underdog_projection_stage")
            else:
                _sync_parlay_play_from_stage(conn)
                _archive_missing_from_stage(conn, "parlay_play", "parlay_play_projection_stage")
            _archive_started_projections(conn)

        cursor = conn.cursor()
        cursor.execute(
            "SELECT COUNT(*) FROM [dbo].[sportsbook_projection] WHERE sportsbook = ?",
            (sb,),
        )
        row = cursor.fetchone()
        return int(row[0]) if row and row[0] is not None else 0
    finally:
        conn.close()
