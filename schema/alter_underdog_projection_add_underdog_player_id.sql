-- Add underdog_player_id to underdog projection tables for joining via [player].
-- Run once on existing DBs. New installs can add the column to CREATE TABLE in underdog_projection.sql / underdog_projection_stage.sql.
USE [Props]
GO

-- underdog_projection: add column (and index in next batch so column exists when index is created)
IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dbo].[underdog_projection]') AND name = N'underdog_player_id')
    ALTER TABLE [dbo].[underdog_projection]
    ADD [underdog_player_id] [bigint] NULL;
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'IX_underdog_projection_underdog_player_id' AND object_id = OBJECT_ID(N'[dbo].[underdog_projection]'))
    CREATE NONCLUSTERED INDEX [IX_underdog_projection_underdog_player_id]
    ON [dbo].[underdog_projection] ([underdog_player_id], [stat_type_name], [start_time]) INCLUDE ([line_score])
    WHERE [underdog_player_id] IS NOT NULL;
GO

-- underdog_projection_stage
IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dbo].[underdog_projection_stage]') AND name = N'underdog_player_id')
    ALTER TABLE [dbo].[underdog_projection_stage]
    ADD [underdog_player_id] [bigint] NULL;
GO

-- underdog_projection_history (if table exists)
IF EXISTS (SELECT 1 FROM sys.tables WHERE name = 'underdog_projection_history')
   AND NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dbo].[underdog_projection_history]') AND name = N'underdog_player_id')
    ALTER TABLE [dbo].[underdog_projection_history]
    ADD [underdog_player_id] [bigint] NULL;
GO
