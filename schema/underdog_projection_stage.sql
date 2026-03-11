-- Underdog Fantasy projection staging table (mirrors underdog_projection for MERGE).
USE [Props]
GO

IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'underdog_projection_stage')
BEGIN
CREATE TABLE [dbo].[underdog_projection_stage](
	[projection_id] [bigint] NOT NULL,
	[display_name] [nvarchar](100) NOT NULL,
	[stat_type_name] [nvarchar](100) NOT NULL,
	[line_score] [decimal](10, 2) NULL,
	[start_time] [datetimeoffset](3) NULL
) ON [PRIMARY]
END
GO
