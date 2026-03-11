-- soccer_player_stat_stage: staging table for soccer player per-game stats (FBref/soccerdata) before upsert to soccer_player_stat
USE [Props]
GO

IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'soccer_player_stat_stage')
BEGIN
CREATE TABLE [dbo].[soccer_player_stat_stage](
	[league] [varchar](80) NOT NULL,
	[season] [varchar](20) NOT NULL,
	[player_id] [varchar](100) NOT NULL,
	[player_name] [nvarchar](255) NOT NULL,
	[game_id] [varchar](80) NOT NULL,
	[game_date] [date] NOT NULL,
	[team] [nvarchar](100) NOT NULL,
	[opponent] [nvarchar](100) NOT NULL,
	[minutes] [smallint] NULL,
	[goals] [smallint] NOT NULL,
	[assists] [smallint] NOT NULL,
	[shots] [smallint] NULL,
	[shots_on_target] [smallint] NULL,
	[penalty_goals] [smallint] NULL,
	[penalty_attempted] [smallint] NULL,
	[cards_yellow] [smallint] NULL,
	[cards_red] [smallint] NULL,
	[dribbles] [smallint] NULL,
	[touches] [smallint] NULL,
	[blocked_shots] [smallint] NULL,
	[corners] [smallint] NULL,
	[free_kicks] [smallint] NULL,
	[passes] [smallint] NULL,
	[passes_attempted] [smallint] NULL,
 CONSTRAINT [UQ_soccer_player_stat_stage_player_game] UNIQUE NONCLUSTERED ([player_id] ASC, [game_id] ASC)
) ON [PRIMARY]
END
GO

-- soccer_player_stat: main table (same columns; game_date as date)
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'soccer_player_stat')
BEGIN
CREATE TABLE [dbo].[soccer_player_stat](
	[league] [varchar](80) NOT NULL,
	[season] [varchar](20) NOT NULL,
	[player_id] [varchar](100) NOT NULL,
	[player_name] [nvarchar](255) NOT NULL,
	[game_id] [varchar](80) NOT NULL,
	[game_date] [date] NOT NULL,
	[team] [nvarchar](100) NOT NULL,
	[opponent] [nvarchar](100) NOT NULL,
	[minutes] [smallint] NULL,
	[goals] [smallint] NOT NULL,
	[assists] [smallint] NOT NULL,
	[shots] [smallint] NULL,
	[shots_on_target] [smallint] NULL,
	[penalty_goals] [smallint] NULL,
	[penalty_attempted] [smallint] NULL,
	[cards_yellow] [smallint] NULL,
	[cards_red] [smallint] NULL,
	[dribbles] [smallint] NULL,
	[touches] [smallint] NULL,
	[blocked_shots] [smallint] NULL,
	[corners] [smallint] NULL,
	[free_kicks] [smallint] NULL,
	[passes] [smallint] NULL,
	[passes_attempted] [smallint] NULL,
 CONSTRAINT [UQ_soccer_player_stat_player_game] UNIQUE NONCLUSTERED ([player_id] ASC, [game_id] ASC)
) ON [PRIMARY]
END
GO
