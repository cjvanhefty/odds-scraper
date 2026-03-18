-- Underdog Fantasy projections (one row per player + stat + start_time for matching to PrizePicks).
-- start_time is stored as America/Chicago local time (datetime2, no timezone).
USE [Props]
GO

IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'underdog_projection')
BEGIN
CREATE TABLE [dbo].[underdog_projection](
	[projection_id] [bigint] NOT NULL,
	[display_name] [nvarchar](100) NOT NULL,
	[stat_type_name] [nvarchar](100) NOT NULL,
	[line_score] [decimal](10, 2) NULL,
	[start_time] [datetime2](3) NULL,
	[last_modified_at] [datetime2](7) NOT NULL DEFAULT GETUTCDATE(),
	CONSTRAINT [PK_underdog_projection] PRIMARY KEY CLUSTERED ([projection_id] ASC)
) ON [PRIMARY]
END
GO

-- Index for matching: (display_name, stat_type_name, start_time date)
IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_underdog_projection_match' AND object_id = OBJECT_ID('dbo.underdog_projection'))
CREATE NONCLUSTERED INDEX [IX_underdog_projection_match] ON [dbo].[underdog_projection]
	([display_name], [stat_type_name], [start_time]) INCLUDE ([line_score]);
GO
