-- Migrate underdog_projection / underdog_projection_stage / underdog_projection_history
-- from datetimeoffset(3) (UTC) to datetime2(3) (America/Chicago local).
-- Run once on existing DBs. New installs use datetime2(3) in underdog_projection.sql and underdog_projection_stage.sql.
USE [Props]
GO

-- ========== underdog_projection ==========
IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dbo].[underdog_projection]') AND name = N'start_time')
BEGIN
    -- Check if column is still datetimeoffset (skip if already datetime2)
    DECLARE @type sysname;
    SELECT @type = t.name
    FROM sys.columns c
    JOIN sys.types t ON c.user_type_id = t.user_type_id
    WHERE c.object_id = OBJECT_ID(N'[dbo].[underdog_projection]') AND c.name = N'start_time';

    IF @type = 'datetimeoffset'
    BEGIN
        DROP INDEX IF EXISTS [IX_underdog_projection_match] ON [dbo].[underdog_projection];
        DROP INDEX IF EXISTS [IX_underdog_projection_underdog_player_id] ON [dbo].[underdog_projection];

        ALTER TABLE [dbo].[underdog_projection] ADD [start_time_chicago] [datetime2](3) NULL;

        EXEC('UPDATE [dbo].[underdog_projection] SET [start_time_chicago] = CONVERT(datetime2(3), [start_time] AT TIME ZONE ''UTC'' AT TIME ZONE ''Central Standard Time'') WHERE [start_time] IS NOT NULL');

        ALTER TABLE [dbo].[underdog_projection] DROP COLUMN [start_time];
        EXEC sp_rename 'dbo.underdog_projection.start_time_chicago', 'start_time', 'COLUMN';

        CREATE NONCLUSTERED INDEX [IX_underdog_projection_match] ON [dbo].[underdog_projection]
            ([display_name], [stat_type_name], [start_time]) INCLUDE ([line_score]);

        IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dbo].[underdog_projection]') AND name = N'underdog_player_id')
            CREATE NONCLUSTERED INDEX [IX_underdog_projection_underdog_player_id] ON [dbo].[underdog_projection]
                ([underdog_player_id], [stat_type_name], [start_time]) INCLUDE ([line_score])
                WHERE [underdog_player_id] IS NOT NULL;
    END
END
GO

-- ========== underdog_projection_stage ==========
IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dbo].[underdog_projection_stage]') AND name = N'start_time')
BEGIN
    DECLARE @stage_type sysname;
    SELECT @stage_type = t.name
    FROM sys.columns c
    JOIN sys.types t ON c.user_type_id = t.user_type_id
    WHERE c.object_id = OBJECT_ID(N'[dbo].[underdog_projection_stage]') AND c.name = N'start_time';

    IF @stage_type = 'datetimeoffset'
    BEGIN
        ALTER TABLE [dbo].[underdog_projection_stage] ADD [start_time_chicago] [datetime2](3) NULL;

        EXEC('UPDATE [dbo].[underdog_projection_stage] SET [start_time_chicago] = CONVERT(datetime2(3), [start_time] AT TIME ZONE ''UTC'' AT TIME ZONE ''Central Standard Time'') WHERE [start_time] IS NOT NULL');

        ALTER TABLE [dbo].[underdog_projection_stage] DROP COLUMN [start_time];
        EXEC sp_rename 'dbo.underdog_projection_stage.start_time_chicago', 'start_time', 'COLUMN';
    END
END
GO

-- ========== underdog_projection_history (if exists) ==========
IF EXISTS (SELECT 1 FROM sys.tables WHERE name = 'underdog_projection_history')
   AND EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dbo].[underdog_projection_history]') AND name = N'start_time')
BEGIN
    DECLARE @hist_type sysname;
    SELECT @hist_type = t.name
    FROM sys.columns c
    JOIN sys.types t ON c.user_type_id = t.user_type_id
    WHERE c.object_id = OBJECT_ID(N'[dbo].[underdog_projection_history]') AND c.name = N'start_time';

    IF @hist_type = 'datetimeoffset'
    BEGIN
        ALTER TABLE [dbo].[underdog_projection_history] ADD [start_time_chicago] [datetime2](3) NULL;

        EXEC('UPDATE [dbo].[underdog_projection_history] SET [start_time_chicago] = CONVERT(datetime2(3), [start_time] AT TIME ZONE ''UTC'' AT TIME ZONE ''Central Standard Time'') WHERE [start_time] IS NOT NULL');

        ALTER TABLE [dbo].[underdog_projection_history] DROP COLUMN [start_time];
        EXEC sp_rename 'dbo.underdog_projection_history.start_time_chicago', 'start_time', 'COLUMN';
    END
END
GO
