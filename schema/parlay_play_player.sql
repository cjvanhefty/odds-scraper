-- Parlay Play player. From body.players[].player. Links to sport and team. Link to [player] via parlay_play_player_id.
USE [Props]
GO

IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'parlay_play_player')
BEGIN
CREATE TABLE [dbo].[parlay_play_player](
	[id] [int] NOT NULL,
	[sport_id] [int] NULL,
	[team_id] [int] NULL,
	[first_name] [nvarchar](100) NULL,
	[last_name] [nvarchar](100) NULL,
	[full_name] [nvarchar](150) NOT NULL,
	[name_initial] [nvarchar](50) NULL,
	[image] [nvarchar](500) NULL,
	[position] [nvarchar](20) NULL,
	[gender] [nvarchar](10) NULL,
	[popularity] [nvarchar](20) NULL,
	[show_alt_lines] [bit] NULL,
	[last_modified_at] [datetime2](7) NOT NULL DEFAULT (CONVERT(datetime2(7), SYSUTCDATETIME() AT TIME ZONE 'UTC' AT TIME ZONE 'Central Standard Time')),
	CONSTRAINT [PK_parlay_play_player] PRIMARY KEY CLUSTERED ([id] ASC),
	CONSTRAINT [FK_parlay_play_player_sport] FOREIGN KEY ([sport_id]) REFERENCES [dbo].[parlay_play_sport] ([id]),
	CONSTRAINT [FK_parlay_play_player_team] FOREIGN KEY ([team_id]) REFERENCES [dbo].[parlay_play_team] ([id])
) ON [PRIMARY]
END
GO

IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'parlay_play_player_stage')
BEGIN
CREATE TABLE [dbo].[parlay_play_player_stage](
	[id] [int] NOT NULL,
	[sport_id] [int] NULL,
	[team_id] [int] NULL,
	[first_name] [nvarchar](100) NULL,
	[last_name] [nvarchar](100) NULL,
	[full_name] [nvarchar](150) NOT NULL,
	[name_initial] [nvarchar](50) NULL,
	[image] [nvarchar](500) NULL,
	[position] [nvarchar](20) NULL,
	[gender] [nvarchar](10) NULL,
	[popularity] [nvarchar](20) NULL,
	[show_alt_lines] [bit] NULL
) ON [PRIMARY]
END
GO
