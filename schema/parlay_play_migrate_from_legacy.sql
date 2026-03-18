-- Run once if you have the old 5-column parlay_play_projection / parlay_play_projection_stage.
-- Drops the old tables so the new normalized parlay_play_projection (with match_id, player_id, alt lines) can be created.
-- Backup data first if you need to keep it.
USE [Props]
GO

-- Drop old tables only if they have the legacy structure (no match_id column).
IF EXISTS (SELECT * FROM sys.tables WHERE name = 'parlay_play_projection')
   AND NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dbo].[parlay_play_projection]') AND name = N'match_id')
    DROP TABLE [dbo].[parlay_play_projection];
GO

IF EXISTS (SELECT * FROM sys.tables WHERE name = 'parlay_play_projection_stage')
   AND NOT EXISTS (SELECT 1 FROM sys.columns WHERE object_id = OBJECT_ID(N'[dbo].[parlay_play_projection_stage]') AND name = N'match_id')
    DROP TABLE [dbo].[parlay_play_projection_stage];
GO

-- Then run parlay_play_projection.sql and parlay_play_projection_stage.sql to create the new tables.
