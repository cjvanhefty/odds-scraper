-- Parlay Play league (from match.league). Links to sport.
USE [Props]
GO

IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'parlay_play_league')
BEGIN
CREATE TABLE [dbo].[parlay_play_league](
	[id] [int] NOT NULL,
	[sport_id] [int] NULL,
	[league_name] [nvarchar](100) NULL,
	[league_name_short] [nvarchar](20) NULL,
	[slug] [nvarchar](50) NULL,
	[popularity] [nvarchar](20) NULL,
	[allowed_players_per_match] [int] NULL,
	[last_modified_at] [datetime2](7) NOT NULL DEFAULT GETUTCDATE(),
	CONSTRAINT [PK_parlay_play_league] PRIMARY KEY CLUSTERED ([id] ASC),
	CONSTRAINT [FK_parlay_play_league_sport] FOREIGN KEY ([sport_id]) REFERENCES [dbo].[parlay_play_sport] ([id])
) ON [PRIMARY]
END
GO

IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'parlay_play_league_stage')
BEGIN
CREATE TABLE [dbo].[parlay_play_league_stage](
	[id] [int] NOT NULL,
	[sport_id] [int] NULL,
	[league_name] [nvarchar](100) NULL,
	[league_name_short] [nvarchar](20) NULL,
	[slug] [nvarchar](50) NULL,
	[popularity] [nvarchar](20) NULL,
	[allowed_players_per_match] [int] NULL
) ON [PRIMARY]
END
GO
