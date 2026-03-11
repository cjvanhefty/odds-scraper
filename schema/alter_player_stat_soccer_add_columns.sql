-- Add dribbles, touches, blocked_shots, corners, free_kicks, passes, passes_attempted to existing soccer stat tables.
-- Run once if you created soccer_player_stat / soccer_player_stat_stage before these columns existed.

USE [Props]
GO

-- soccer_player_stat_stage
IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dbo].[soccer_player_stat_stage]') AND name = N'dribbles')
    ALTER TABLE [dbo].[soccer_player_stat_stage] ADD [dribbles] [smallint] NULL;
IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dbo].[soccer_player_stat_stage]') AND name = N'touches')
    ALTER TABLE [dbo].[soccer_player_stat_stage] ADD [touches] [smallint] NULL;
IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dbo].[soccer_player_stat_stage]') AND name = N'blocked_shots')
    ALTER TABLE [dbo].[soccer_player_stat_stage] ADD [blocked_shots] [smallint] NULL;
IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dbo].[soccer_player_stat_stage]') AND name = N'corners')
    ALTER TABLE [dbo].[soccer_player_stat_stage] ADD [corners] [smallint] NULL;
IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dbo].[soccer_player_stat_stage]') AND name = N'free_kicks')
    ALTER TABLE [dbo].[soccer_player_stat_stage] ADD [free_kicks] [smallint] NULL;
IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dbo].[soccer_player_stat_stage]') AND name = N'passes')
    ALTER TABLE [dbo].[soccer_player_stat_stage] ADD [passes] [smallint] NULL;
IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dbo].[soccer_player_stat_stage]') AND name = N'passes_attempted')
    ALTER TABLE [dbo].[soccer_player_stat_stage] ADD [passes_attempted] [smallint] NULL;

-- soccer_player_stat
IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dbo].[soccer_player_stat]') AND name = N'dribbles')
    ALTER TABLE [dbo].[soccer_player_stat] ADD [dribbles] [smallint] NULL;
IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dbo].[soccer_player_stat]') AND name = N'touches')
    ALTER TABLE [dbo].[soccer_player_stat] ADD [touches] [smallint] NULL;
IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dbo].[soccer_player_stat]') AND name = N'blocked_shots')
    ALTER TABLE [dbo].[soccer_player_stat] ADD [blocked_shots] [smallint] NULL;
IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dbo].[soccer_player_stat]') AND name = N'corners')
    ALTER TABLE [dbo].[soccer_player_stat] ADD [corners] [smallint] NULL;
IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dbo].[soccer_player_stat]') AND name = N'free_kicks')
    ALTER TABLE [dbo].[soccer_player_stat] ADD [free_kicks] [smallint] NULL;
IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dbo].[soccer_player_stat]') AND name = N'passes')
    ALTER TABLE [dbo].[soccer_player_stat] ADD [passes] [smallint] NULL;
IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dbo].[soccer_player_stat]') AND name = N'passes_attempted')
    ALTER TABLE [dbo].[soccer_player_stat] ADD [passes_attempted] [smallint] NULL;

GO
