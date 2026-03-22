-- Migrate parlay_play_projection / parlay_play_projection_stage / parlay_play_projection_history
-- from datetimeoffset(3) to datetime2(3) in America/Chicago local time (no offset).
--
-- IMPORTANT: Each step uses GO so SQL Server recompiles batches; otherwise UPDATE sees
-- "Invalid column name 'start_time_chicago'" because ADD isn't visible in the same batch.
--
-- Requires SQL Server 2016+ for AT TIME ZONE.
USE [Props]
GO

-- ========= parlay_play_projection =========
IF EXISTS (
    SELECT 1
    FROM sys.columns c
    INNER JOIN sys.types t ON c.user_type_id = t.user_type_id
    WHERE c.object_id = OBJECT_ID(N'[dbo].[parlay_play_projection]')
      AND c.name = N'start_time'
      AND t.name = N'datetimeoffset'
)
BEGIN
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'IX_parlay_play_projection_match' AND object_id = OBJECT_ID(N'[dbo].[parlay_play_projection]'))
        DROP INDEX [IX_parlay_play_projection_match] ON [dbo].[parlay_play_projection];
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'IX_parlay_play_projection_main' AND object_id = OBJECT_ID(N'[dbo].[parlay_play_projection]'))
        DROP INDEX [IX_parlay_play_projection_main] ON [dbo].[parlay_play_projection];
END
GO

IF EXISTS (
    SELECT 1
    FROM sys.columns c
    INNER JOIN sys.types t ON c.user_type_id = t.user_type_id
    WHERE c.object_id = OBJECT_ID(N'[dbo].[parlay_play_projection]')
      AND c.name = N'start_time'
      AND t.name = N'datetimeoffset'
)
   AND NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dbo].[parlay_play_projection]') AND name = N'start_time_chicago')
    ALTER TABLE [dbo].[parlay_play_projection] ADD [start_time_chicago] [datetime2](3) NULL;
GO

IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dbo].[parlay_play_projection]') AND name = N'start_time_chicago')
   AND EXISTS (
    SELECT 1
    FROM sys.columns c
    INNER JOIN sys.types t ON c.user_type_id = t.user_type_id
    WHERE c.object_id = OBJECT_ID(N'[dbo].[parlay_play_projection]')
      AND c.name = N'start_time'
      AND t.name = N'datetimeoffset'
)
    UPDATE [dbo].[parlay_play_projection]
    SET [start_time_chicago] =
        CASE
            WHEN [start_time] IS NULL THEN NULL
            ELSE CONVERT(datetime2(3), ([start_time] AT TIME ZONE 'Central Standard Time'))
        END;
GO

IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dbo].[parlay_play_projection]') AND name = N'start_time_chicago')
   AND EXISTS (
    SELECT 1
    FROM sys.columns c
    INNER JOIN sys.types t ON c.user_type_id = t.user_type_id
    WHERE c.object_id = OBJECT_ID(N'[dbo].[parlay_play_projection]')
      AND c.name = N'start_time'
      AND t.name = N'datetimeoffset'
)
BEGIN
    ALTER TABLE [dbo].[parlay_play_projection] DROP COLUMN [start_time];
    EXEC sp_rename N'dbo.parlay_play_projection.start_time_chicago', N'start_time', N'COLUMN';
END
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'IX_parlay_play_projection_match' AND object_id = OBJECT_ID(N'[dbo].[parlay_play_projection]'))
CREATE NONCLUSTERED INDEX [IX_parlay_play_projection_match] ON [dbo].[parlay_play_projection]
    ([display_name], [stat_type_name], [start_time]) INCLUDE ([line_score]);
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'IX_parlay_play_projection_main' AND object_id = OBJECT_ID(N'[dbo].[parlay_play_projection]'))
CREATE NONCLUSTERED INDEX [IX_parlay_play_projection_main] ON [dbo].[parlay_play_projection]
    ([match_id], [player_id], [challenge_option], [is_main_line]) INCLUDE ([line_score], [start_time]);
GO

-- ========= parlay_play_projection_stage =========
IF EXISTS (
    SELECT 1
    FROM sys.columns c
    INNER JOIN sys.types t ON c.user_type_id = t.user_type_id
    WHERE c.object_id = OBJECT_ID(N'[dbo].[parlay_play_projection_stage]')
      AND c.name = N'start_time'
      AND t.name = N'datetimeoffset'
)
   AND NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dbo].[parlay_play_projection_stage]') AND name = N'start_time_chicago')
    ALTER TABLE [dbo].[parlay_play_projection_stage] ADD [start_time_chicago] [datetime2](3) NULL;
GO

IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dbo].[parlay_play_projection_stage]') AND name = N'start_time_chicago')
   AND EXISTS (
    SELECT 1
    FROM sys.columns c
    INNER JOIN sys.types t ON c.user_type_id = t.user_type_id
    WHERE c.object_id = OBJECT_ID(N'[dbo].[parlay_play_projection_stage]')
      AND c.name = N'start_time'
      AND t.name = N'datetimeoffset'
)
    UPDATE [dbo].[parlay_play_projection_stage]
    SET [start_time_chicago] =
        CASE
            WHEN [start_time] IS NULL THEN NULL
            ELSE CONVERT(datetime2(3), ([start_time] AT TIME ZONE 'Central Standard Time'))
        END;
GO

IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dbo].[parlay_play_projection_stage]') AND name = N'start_time_chicago')
   AND EXISTS (
    SELECT 1
    FROM sys.columns c
    INNER JOIN sys.types t ON c.user_type_id = t.user_type_id
    WHERE c.object_id = OBJECT_ID(N'[dbo].[parlay_play_projection_stage]')
      AND c.name = N'start_time'
      AND t.name = N'datetimeoffset'
)
BEGIN
    ALTER TABLE [dbo].[parlay_play_projection_stage] DROP COLUMN [start_time];
    EXEC sp_rename N'dbo.parlay_play_projection_stage.start_time_chicago', N'start_time', N'COLUMN';
END
GO

-- ========= parlay_play_projection_history =========
IF EXISTS (
    SELECT 1
    FROM sys.columns c
    INNER JOIN sys.types t ON c.user_type_id = t.user_type_id
    WHERE c.object_id = OBJECT_ID(N'[dbo].[parlay_play_projection_history]')
      AND c.name = N'start_time'
      AND t.name = N'datetimeoffset'
)
BEGIN
    IF EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'IX_parlay_play_projection_history_projection_id' AND object_id = OBJECT_ID(N'[dbo].[parlay_play_projection_history]'))
        DROP INDEX [IX_parlay_play_projection_history_projection_id] ON [dbo].[parlay_play_projection_history];
END
GO

IF EXISTS (
    SELECT 1
    FROM sys.columns c
    INNER JOIN sys.types t ON c.user_type_id = t.user_type_id
    WHERE c.object_id = OBJECT_ID(N'[dbo].[parlay_play_projection_history]')
      AND c.name = N'start_time'
      AND t.name = N'datetimeoffset'
)
   AND NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dbo].[parlay_play_projection_history]') AND name = N'start_time_chicago')
    ALTER TABLE [dbo].[parlay_play_projection_history] ADD [start_time_chicago] [datetime2](3) NULL;
GO

IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dbo].[parlay_play_projection_history]') AND name = N'start_time_chicago')
   AND EXISTS (
    SELECT 1
    FROM sys.columns c
    INNER JOIN sys.types t ON c.user_type_id = t.user_type_id
    WHERE c.object_id = OBJECT_ID(N'[dbo].[parlay_play_projection_history]')
      AND c.name = N'start_time'
      AND t.name = N'datetimeoffset'
)
    UPDATE [dbo].[parlay_play_projection_history]
    SET [start_time_chicago] =
        CASE
            WHEN [start_time] IS NULL THEN NULL
            ELSE CONVERT(datetime2(3), ([start_time] AT TIME ZONE 'Central Standard Time'))
        END;
GO

IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dbo].[parlay_play_projection_history]') AND name = N'start_time_chicago')
   AND EXISTS (
    SELECT 1
    FROM sys.columns c
    INNER JOIN sys.types t ON c.user_type_id = t.user_type_id
    WHERE c.object_id = OBJECT_ID(N'[dbo].[parlay_play_projection_history]')
      AND c.name = N'start_time'
      AND t.name = N'datetimeoffset'
)
BEGIN
    ALTER TABLE [dbo].[parlay_play_projection_history] DROP COLUMN [start_time];
    EXEC sp_rename N'dbo.parlay_play_projection_history.start_time_chicago', N'start_time', N'COLUMN';
END
GO

IF OBJECT_ID(N'[dbo].[parlay_play_projection_history]', N'U') IS NOT NULL
   AND NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'IX_parlay_play_projection_history_projection_id' AND object_id = OBJECT_ID(N'[dbo].[parlay_play_projection_history]'))
CREATE NONCLUSTERED INDEX [IX_parlay_play_projection_history_projection_id] ON [dbo].[parlay_play_projection_history]
    ([projection_id], [start_time]) INCLUDE ([line_score], [decimal_price_over], [decimal_price_under]);
GO
