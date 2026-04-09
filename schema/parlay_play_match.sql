-- Parlay Play match (game). From body.players[].match. Links sport, league, home_team, away_team.
USE [Props]
GO

IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'parlay_play_match')
BEGIN
CREATE TABLE [dbo].[parlay_play_match](
	[id] [int] NOT NULL,
	[sport_id] [int] NULL,
	[league_id] [int] NULL,
	[home_team_id] [int] NULL,
	[away_team_id] [int] NULL,
	[slug] [nvarchar](150) NULL,
	[match_date] [datetimeoffset](3) NULL,
	[match_type] [nvarchar](50) NULL,
	[match_status] [int] NULL,
	[match_period] [nvarchar](20) NULL,
	[score_home] [int] NULL,
	[score_away] [int] NULL,
	[time_left] [nvarchar](20) NULL,
	[time_to_start] [nvarchar](20) NULL,
	[time_to_start_min] [int] NULL,
	[home_win_prob] [nvarchar](20) NULL,
	[away_win_prob] [nvarchar](20) NULL,
	[draw_prob] [nvarchar](20) NULL,
	[last_modified_at] [datetime2](7) NOT NULL DEFAULT (CONVERT(datetime2(7), SYSUTCDATETIME() AT TIME ZONE 'UTC' AT TIME ZONE 'Central Standard Time')),
	CONSTRAINT [PK_parlay_play_match] PRIMARY KEY CLUSTERED ([id] ASC),
	CONSTRAINT [FK_parlay_play_match_sport] FOREIGN KEY ([sport_id]) REFERENCES [dbo].[parlay_play_sport] ([id]),
	CONSTRAINT [FK_parlay_play_match_league] FOREIGN KEY ([league_id]) REFERENCES [dbo].[parlay_play_league] ([id]),
	CONSTRAINT [FK_parlay_play_match_home_team] FOREIGN KEY ([home_team_id]) REFERENCES [dbo].[parlay_play_team] ([id]),
	CONSTRAINT [FK_parlay_play_match_away_team] FOREIGN KEY ([away_team_id]) REFERENCES [dbo].[parlay_play_team] ([id])
) ON [PRIMARY]
END
GO

IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'parlay_play_match_stage')
BEGIN
CREATE TABLE [dbo].[parlay_play_match_stage](
	[id] [int] NOT NULL,
	[sport_id] [int] NULL,
	[league_id] [int] NULL,
	[home_team_id] [int] NULL,
	[away_team_id] [int] NULL,
	[slug] [nvarchar](150) NULL,
	[match_date] [datetimeoffset](3) NULL,
	[match_type] [nvarchar](50) NULL,
	[match_status] [int] NULL,
	[match_period] [nvarchar](20) NULL,
	[score_home] [int] NULL,
	[score_away] [int] NULL,
	[time_left] [nvarchar](20) NULL,
	[time_to_start] [nvarchar](20) NULL,
	[time_to_start_min] [int] NULL,
	[home_win_prob] [nvarchar](20) NULL,
	[away_win_prob] [nvarchar](20) NULL,
	[draw_prob] [nvarchar](20) NULL
) ON [PRIMARY]
END
GO
