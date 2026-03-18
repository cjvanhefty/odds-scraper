-- Parlay Play stat/challenge type (from stats[].challengeName, challengeOption). One row per unique challenge_option.
USE [Props]
GO

IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'parlay_play_stat_type')
BEGIN
CREATE TABLE [dbo].[parlay_play_stat_type](
	[challenge_option] [nvarchar](50) NOT NULL,
	[challenge_name] [nvarchar](100) NULL,
	[challenge_units] [nvarchar](20) NULL,
	[last_modified_at] [datetime2](7) NOT NULL DEFAULT GETUTCDATE(),
	CONSTRAINT [PK_parlay_play_stat_type] PRIMARY KEY CLUSTERED ([challenge_option] ASC)
) ON [PRIMARY]
END
GO

IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'parlay_play_stat_type_stage')
BEGIN
CREATE TABLE [dbo].[parlay_play_stat_type_stage](
	[challenge_option] [nvarchar](50) NOT NULL,
	[challenge_name] [nvarchar](100) NULL,
	[challenge_units] [nvarchar](20) NULL
) ON [PRIMARY]
END
GO
