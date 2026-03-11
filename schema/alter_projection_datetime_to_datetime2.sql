-- Optional: alter prizepicks projection tables to store datetimes without UTC offset.
-- Run once if your tables use datetimeoffset and you want display like 2026-03-07 18:10:00.
-- New installs get datetime2(0) from the scraper's ensure_projection_stage_table.

USE [Props]
GO

-- prizepicks_projection_stage
IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dbo].[prizepicks_projection_stage]') AND name = N'board_time')
    ALTER TABLE [dbo].[prizepicks_projection_stage] ALTER COLUMN [board_time] [datetime2](0) NULL;
IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dbo].[prizepicks_projection_stage]') AND name = N'end_time')
    ALTER TABLE [dbo].[prizepicks_projection_stage] ALTER COLUMN [end_time] [datetime2](0) NULL;
IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dbo].[prizepicks_projection_stage]') AND name = N'start_time')
    ALTER TABLE [dbo].[prizepicks_projection_stage] ALTER COLUMN [start_time] [datetime2](0) NULL;
IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dbo].[prizepicks_projection_stage]') AND name = N'updated_at')
    ALTER TABLE [dbo].[prizepicks_projection_stage] ALTER COLUMN [updated_at] [datetime2](0) NULL;

-- prizepicks_projection
IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dbo].[prizepicks_projection]') AND name = N'board_time')
    ALTER TABLE [dbo].[prizepicks_projection] ALTER COLUMN [board_time] [datetime2](0) NULL;
IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dbo].[prizepicks_projection]') AND name = N'end_time')
    ALTER TABLE [dbo].[prizepicks_projection] ALTER COLUMN [end_time] [datetime2](0) NULL;
IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dbo].[prizepicks_projection]') AND name = N'start_time')
    ALTER TABLE [dbo].[prizepicks_projection] ALTER COLUMN [start_time] [datetime2](0) NULL;
IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dbo].[prizepicks_projection]') AND name = N'updated_at')
    ALTER TABLE [dbo].[prizepicks_projection] ALTER COLUMN [updated_at] [datetime2](0) NULL;

-- prizepicks_projection_history
IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dbo].[prizepicks_projection_history]') AND name = N'board_time')
    ALTER TABLE [dbo].[prizepicks_projection_history] ALTER COLUMN [board_time] [datetime2](0) NULL;
IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dbo].[prizepicks_projection_history]') AND name = N'end_time')
    ALTER TABLE [dbo].[prizepicks_projection_history] ALTER COLUMN [end_time] [datetime2](0) NULL;
IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dbo].[prizepicks_projection_history]') AND name = N'start_time')
    ALTER TABLE [dbo].[prizepicks_projection_history] ALTER COLUMN [start_time] [datetime2](0) NULL;
IF EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dbo].[prizepicks_projection_history]') AND name = N'updated_at')
    ALTER TABLE [dbo].[prizepicks_projection_history] ALTER COLUMN [updated_at] [datetime2](0) NULL;

GO
