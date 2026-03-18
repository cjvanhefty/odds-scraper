-- Store full Parlay Play API response JSON for replay and debugging.
-- Insert one row per scrape; normalized tables are populated from this or from in-memory parse.
USE [Props]
GO

IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'parlay_play_raw')
BEGIN
CREATE TABLE [dbo].[parlay_play_raw](
	[id] [bigint] IDENTITY(1,1) NOT NULL,
	[fetched_at] [datetime2](7) NOT NULL DEFAULT SYSUTCDATETIME(),
	[response_json] [nvarchar](max) NULL,
	[url] [nvarchar](500) NULL,
	CONSTRAINT [PK_parlay_play_raw] PRIMARY KEY CLUSTERED ([id] ASC)
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
END
GO
