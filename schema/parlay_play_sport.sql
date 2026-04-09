-- Parlay Play sport (from match.sport / player.sport). Links league, team, match, player.
USE [Props]
GO

IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'parlay_play_sport')
BEGIN
CREATE TABLE [dbo].[parlay_play_sport](
	[id] [int] NOT NULL,
	[sport_name] [nvarchar](100) NULL,
	[slug] [nvarchar](50) NULL,
	[symbol] [nvarchar](500) NULL,
	[illustration] [nvarchar](500) NULL,
	[popularity] [nvarchar](20) NULL,
	[last_modified_at] [datetime2](7) NOT NULL DEFAULT (CONVERT(datetime2(7), SYSUTCDATETIME() AT TIME ZONE 'UTC' AT TIME ZONE 'Central Standard Time')),
	CONSTRAINT [PK_parlay_play_sport] PRIMARY KEY CLUSTERED ([id] ASC)
) ON [PRIMARY]
END
GO

IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'parlay_play_sport_stage')
BEGIN
CREATE TABLE [dbo].[parlay_play_sport_stage](
	[id] [int] NOT NULL,
	[sport_name] [nvarchar](100) NULL,
	[slug] [nvarchar](50) NULL,
	[symbol] [nvarchar](500) NULL,
	[illustration] [nvarchar](500) NULL,
	[popularity] [nvarchar](20) NULL
) ON [PRIMARY]
END
GO
