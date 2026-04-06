-- Underdog stat type dimension: surrogate PK + Underdog external id (pickem_stat_id).
-- MERGE matches on pickem_stat_id; underdog_stat_type_id is IDENTITY(1,1).
-- Load underdog_stat_type_stage (truncate + insert), then dbo.MergeUnderdogReferenceFromStage.
-- Existing DBs on the old stat_type_key schema: run alter_underdog_stat_type_identity_pk.sql once.
USE [Props]
GO

IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'underdog_stat_type' AND schema_id = SCHEMA_ID('dbo'))
BEGIN
CREATE TABLE [dbo].[underdog_stat_type](
	[underdog_stat_type_id] [int] IDENTITY(1,1) NOT NULL,
	[pickem_stat_id] [nvarchar](36) NOT NULL,
	[stat_type_name] [nvarchar](100) NOT NULL,
	[display_stat] [nvarchar](200) NULL,
	[stat] [nvarchar](200) NULL,
	[last_modified_at] [datetime2](7) NOT NULL CONSTRAINT [DF_underdog_stat_type_last_modified_at] DEFAULT (GETUTCDATE()),
	CONSTRAINT [PK_underdog_stat_type] PRIMARY KEY CLUSTERED ([underdog_stat_type_id] ASC),
	CONSTRAINT [UQ_underdog_stat_type_pickem_stat_id] UNIQUE ([pickem_stat_id] ASC)
) ON [PRIMARY]
END
GO

IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'underdog_stat_type_stage' AND schema_id = SCHEMA_ID('dbo'))
BEGIN
CREATE TABLE [dbo].[underdog_stat_type_stage](
	[pickem_stat_id] [nvarchar](36) NOT NULL,
	[stat_type_name] [nvarchar](100) NOT NULL,
	[display_stat] [nvarchar](200) NULL,
	[stat] [nvarchar](200) NULL
) ON [PRIMARY]
END
GO
