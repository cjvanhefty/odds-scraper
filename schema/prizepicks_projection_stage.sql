-- prizepicks_projection_stage: staging table for API data before upsert to prizepicks_projection
USE [Props]
GO

IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'prizepicks_projection_stage')
BEGIN
CREATE TABLE [dbo].[prizepicks_projection_stage](
	[projection_id] [bigint] NOT NULL,
	[projection_type] [varchar](50) NOT NULL,
	[adjusted_odds] [bit] NULL,
	[board_time] [datetimeoffset](3) NULL,
	[custom_image] [varchar](500) NULL,
	[description] [varchar](100) NULL,
	[end_time] [datetimeoffset](3) NULL,
	[event_type] [varchar](50) NULL,
	[flash_sale_line_score] [decimal](10, 2) NULL,
	[game_id] [varchar](100) NULL,
	[group_key] [varchar](150) NULL,
	[hr_20] [bit] NULL,
	[in_game] [bit] NULL,
	[is_live] [bit] NULL,
	[is_live_scored] [bit] NULL,
	[is_promo] [bit] NULL,
	[line_score] [decimal](10, 2) NULL,
	[odds_type] [varchar](50) NULL,
	[projection_display_type] [varchar](100) NULL,
	[rank] [int] NULL,
	[refundable] [bit] NULL,
	[start_time] [datetimeoffset](3) NULL,
	[stat_display_name] [varchar](100) NULL,
	[stat_type_name] [varchar](100) NULL,
	[status] [varchar](50) NULL,
	[today] [bit] NULL,
	[tv_channel] [varchar](50) NULL,
	[updated_at] [datetimeoffset](3) NULL,
	[duration_id] [int] NULL,
	[game_rel_id] [int] NULL,
	[league_id] [int] NULL,
	[player_id] [int] NULL,
	[projection_type_id] [int] NULL,
	[score_id] [int] NULL,
	[stat_type_id] [int] NULL
) ON [PRIMARY]
END
GO
