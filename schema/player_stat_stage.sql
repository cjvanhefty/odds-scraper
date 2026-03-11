-- player_stat_stage: staging table for NBA gamelog data before upsert to player_stat
USE [Props]
GO

IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'player_stat_stage')
BEGIN
CREATE TABLE [dbo].[player_stat_stage](
	[season_id] [varchar](10) NOT NULL,
	[player_id] [bigint] NOT NULL,
	[game_id] [varchar](20) NOT NULL,
	[game_date] [varchar](20) NOT NULL,
	[matchup] [varchar](20) NOT NULL,
	[wl] [char](1) NOT NULL,
	[min] [smallint] NOT NULL,
	[fgm] [smallint] NOT NULL,
	[fga] [smallint] NOT NULL,
	[fg_pct] [decimal](5, 3) NULL,
	[fg3m] [smallint] NOT NULL,
	[fg3a] [smallint] NOT NULL,
	[fg3_pct] [decimal](5, 3) NULL,
	[ftm] [smallint] NOT NULL,
	[fta] [smallint] NOT NULL,
	[ft_pct] [decimal](5, 3) NULL,
	[oreb] [smallint] NOT NULL,
	[dreb] [smallint] NOT NULL,
	[reb] [smallint] NOT NULL,
	[ast] [smallint] NOT NULL,
	[stl] [smallint] NOT NULL,
	[blk] [smallint] NOT NULL,
	[tov] [smallint] NOT NULL,
	[pf] [smallint] NOT NULL,
	[pts] [smallint] NOT NULL,
	[plus_minus] [smallint] NOT NULL,
	[video_available] [smallint] NOT NULL,
 CONSTRAINT [UQ_player_stat_stage_player_game] UNIQUE NONCLUSTERED 
(
	[player_id] ASC,
	[game_id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]
END
GO
