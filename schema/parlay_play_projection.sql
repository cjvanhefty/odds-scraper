-- Parlay Play projections (one row per player + stat + start_time for matching to PrizePicks).
USE [Props]
GO

IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'parlay_play_projection')
BEGIN
CREATE TABLE [dbo].[parlay_play_projection](
	[projection_id] [bigint] NOT NULL,
	[display_name] [nvarchar](100) NOT NULL,
	[stat_type_name] [nvarchar](100) NOT NULL,
	[line_score] [decimal](10, 2) NULL,
	[start_time] [datetimeoffset](3) NULL,
	[last_modified_at] [datetime2](7) NOT NULL DEFAULT GETUTCDATE(),
	CONSTRAINT [PK_parlay_play_projection] PRIMARY KEY CLUSTERED ([projection_id] ASC)
) ON [PRIMARY]
END
GO

IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_parlay_play_projection_match' AND object_id = OBJECT_ID('dbo.parlay_play_projection'))
CREATE NONCLUSTERED INDEX [IX_parlay_play_projection_match] ON [dbo].[parlay_play_projection]
	([display_name], [stat_type_name], [start_time]) INCLUDE ([line_score]);
GO
