-- Parlay Play projections: one row per line (main + each alt line). Links match, player, stat_type.
-- Use display_name + stat_type_name + start_time for cross-site matching (PrizePicks/Underdog).
USE [Props]
GO

IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'parlay_play_projection')
BEGIN
CREATE TABLE [dbo].[parlay_play_projection](
	[projection_id] [bigint] NOT NULL,
	[match_id] [int] NOT NULL,
	[player_id] [int] NOT NULL,
	[challenge_option] [nvarchar](50) NOT NULL,
	[line_score] [decimal](10, 2) NULL,
	[is_main_line] [bit] NOT NULL,
	[decimal_price_over] [decimal](10, 4) NULL,
	[decimal_price_under] [decimal](10, 4) NULL,
	[market_name] [nvarchar](150) NULL,
	[match_period] [nvarchar](20) NULL,
	[show_default] [bit] NULL,
	[display_name] [nvarchar](100) NOT NULL,
	[stat_type_name] [nvarchar](100) NOT NULL,
	[start_time] [datetime2](3) NULL,
	[promo_deadline] [datetimeoffset](3) NULL,
	[promo_max_entry] [decimal](10, 2) NULL,
	[player_promo_id] [int] NULL,
	[player_promo_type] [nvarchar](50) NULL,
	[is_boosted_payout] [bit] NULL,
	[is_player_promo] [bit] NULL,
	[default_multiplier] [decimal](10, 4) NULL,
	[promo_multiplier] [decimal](10, 4) NULL,
	[payout_boost_selection] [nvarchar](20) NULL,
	[is_public] [bit] NULL,
	[is_slashed_line] [bit] NULL,
	[alt_line_count] [int] NULL,
	[last_modified_at] [datetime2](7) NOT NULL DEFAULT GETUTCDATE(),
	CONSTRAINT [PK_parlay_play_projection] PRIMARY KEY CLUSTERED ([projection_id] ASC),
	CONSTRAINT [FK_parlay_play_projection_match] FOREIGN KEY ([match_id]) REFERENCES [dbo].[parlay_play_match] ([id]),
	CONSTRAINT [FK_parlay_play_projection_player] FOREIGN KEY ([player_id]) REFERENCES [dbo].[parlay_play_player] ([id]),
	CONSTRAINT [FK_parlay_play_projection_stat_type] FOREIGN KEY ([challenge_option]) REFERENCES [dbo].[parlay_play_stat_type] ([challenge_option])
) ON [PRIMARY]
END
GO

IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_parlay_play_projection_match' AND object_id = OBJECT_ID('dbo.parlay_play_projection'))
CREATE NONCLUSTERED INDEX [IX_parlay_play_projection_match] ON [dbo].[parlay_play_projection]
	([display_name], [stat_type_name], [start_time]) INCLUDE ([line_score]);
GO

IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_parlay_play_projection_main' AND object_id = OBJECT_ID('dbo.parlay_play_projection'))
CREATE NONCLUSTERED INDEX [IX_parlay_play_projection_main] ON [dbo].[parlay_play_projection]
	([match_id], [player_id], [challenge_option], [is_main_line]) INCLUDE ([line_score], [start_time]);
GO
