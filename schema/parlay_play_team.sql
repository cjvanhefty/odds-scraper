-- Parlay Play team (from match.homeTeam/awayTeam, player.team). Links to sport and league.
USE [Props]
GO

IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'parlay_play_team')
BEGIN
CREATE TABLE [dbo].[parlay_play_team](
	[id] [int] NOT NULL,
	[sport_id] [int] NULL,
	[league_id] [int] NULL,
	[teamname] [nvarchar](100) NULL,
	[teamname_abbr] [nvarchar](50) NULL,
	[team_abbreviation] [nvarchar](20) NULL,
	[slug] [nvarchar](100) NULL,
	[venue] [nvarchar](200) NULL,
	[logo] [nvarchar](500) NULL,
	[conference] [nvarchar](100) NULL,
	[rank] [int] NULL,
	[record] [nvarchar](20) NULL,
	[last_modified_at] [datetime2](7) NOT NULL DEFAULT (CONVERT(datetime2(7), SYSUTCDATETIME() AT TIME ZONE 'UTC' AT TIME ZONE 'Central Standard Time')),
	CONSTRAINT [PK_parlay_play_team] PRIMARY KEY CLUSTERED ([id] ASC),
	CONSTRAINT [FK_parlay_play_team_sport] FOREIGN KEY ([sport_id]) REFERENCES [dbo].[parlay_play_sport] ([id]),
	CONSTRAINT [FK_parlay_play_team_league] FOREIGN KEY ([league_id]) REFERENCES [dbo].[parlay_play_league] ([id])
) ON [PRIMARY]
END
GO

IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'parlay_play_team_stage')
BEGIN
CREATE TABLE [dbo].[parlay_play_team_stage](
	[id] [int] NOT NULL,
	[sport_id] [int] NULL,
	[league_id] [int] NULL,
	[teamname] [nvarchar](100) NULL,
	[teamname_abbr] [nvarchar](50) NULL,
	[team_abbreviation] [nvarchar](20) NULL,
	[slug] [nvarchar](100) NULL,
	[venue] [nvarchar](200) NULL,
	[logo] [nvarchar](500) NULL,
	[conference] [nvarchar](100) NULL,
	[rank] [int] NULL,
	[record] [nvarchar](20) NULL
) ON [PRIMARY]
END
GO
