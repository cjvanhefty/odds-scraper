-- Parlay Play projection staging (one row per line: main + alt). MERGE into parlay_play_projection.
USE [Props]
GO

IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'parlay_play_projection_stage')
BEGIN
CREATE TABLE [dbo].[parlay_play_projection_stage](
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
	[alt_line_count] [int] NULL
) ON [PRIMARY]
END
GO
