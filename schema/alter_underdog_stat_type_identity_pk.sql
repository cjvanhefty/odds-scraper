-- One-time migration: replace PK stat_type_key with underdog_stat_type_id IDENTITY + unique pickem_stat_id.
-- Run after backup. Rebuilds underdog_stat_type_stage for the new shape.
-- Skip if underdog_stat_type already has underdog_stat_type_id.
USE [Props]
GO

IF OBJECT_ID(N'[dbo].[underdog_stat_type]', N'U') IS NULL
BEGIN
    RAISERROR ('underdog_stat_type does not exist; run underdog_stat_type.sql instead.', 16, 1);
END
ELSE IF COL_LENGTH('dbo.underdog_stat_type', 'underdog_stat_type_id') IS NOT NULL
BEGIN
    PRINT 'underdog_stat_type already uses underdog_stat_type_id; nothing to do.';
END
ELSE IF COL_LENGTH('dbo.underdog_stat_type', 'stat_type_key') IS NULL
BEGIN
    RAISERROR ('underdog_stat_type has unexpected shape; fix manually.', 16, 1);
END
ELSE
BEGIN
    IF OBJECT_ID(N'[dbo].[underdog_stat_type_stage]', N'U') IS NOT NULL
        DROP TABLE [dbo].[underdog_stat_type_stage];

    SELECT *
    INTO [dbo].[_underdog_stat_type_legacy]
    FROM [dbo].[underdog_stat_type];

    DROP TABLE [dbo].[underdog_stat_type];

    CREATE TABLE [dbo].[underdog_stat_type](
        [underdog_stat_type_id] [int] IDENTITY(1,1) NOT NULL,
        [pickem_stat_id] [nvarchar](36) NOT NULL,
        [stat_type_name] [nvarchar](100) NOT NULL,
        [display_stat] [nvarchar](200) NULL,
        [stat] [nvarchar](200) NULL,
        [last_modified_at] [datetime2](7) NOT NULL CONSTRAINT [DF_underdog_stat_type_last_modified_at] DEFAULT (CONVERT(datetime2(7), SYSUTCDATETIME() AT TIME ZONE 'UTC' AT TIME ZONE 'Central Standard Time')),
        CONSTRAINT [PK_underdog_stat_type] PRIMARY KEY CLUSTERED ([underdog_stat_type_id] ASC),
        CONSTRAINT [UQ_underdog_stat_type_pickem_stat_id] UNIQUE ([pickem_stat_id] ASC)
    ) ON [PRIMARY];

    ;WITH [raw] AS (
        SELECT
            l.[stat_type_name],
            l.[display_stat],
            l.[stat],
            l.[last_modified_at],
            LEFT(LTRIM(RTRIM(COALESCE(
                NULLIF(LTRIM(RTRIM(l.[pickem_stat_id])), N''),
                CASE
                    WHEN LEN(LTRIM(RTRIM(l.[stat_type_key]))) = 36
                         AND l.[stat_type_key] LIKE N'[0-9A-Fa-f][0-9A-Fa-f][0-9A-Fa-f][0-9A-Fa-f][0-9A-Fa-f][0-9A-Fa-f][0-9A-Fa-f][0-9A-Fa-f]-%'
                    THEN LTRIM(RTRIM(l.[stat_type_key]))
                END
            ))), 36) AS [ext_pickem]
        FROM [dbo].[_underdog_stat_type_legacy] AS l
    ),
    [dedup] AS (
        SELECT
            r.*,
            ROW_NUMBER() OVER (PARTITION BY r.[ext_pickem] ORDER BY r.[last_modified_at] DESC) AS [rn]
        FROM [raw] AS r
        WHERE r.[ext_pickem] IS NOT NULL
          AND LEN(r.[ext_pickem]) > 0
    )
    INSERT INTO [dbo].[underdog_stat_type] (
        [pickem_stat_id], [stat_type_name], [display_stat], [stat], [last_modified_at]
    )
    SELECT
        d.[ext_pickem],
        d.[stat_type_name],
        d.[display_stat],
        d.[stat],
        d.[last_modified_at]
    FROM [dedup] AS d
    WHERE d.[rn] = 1
    ORDER BY d.[ext_pickem];

    DROP TABLE [dbo].[_underdog_stat_type_legacy];

    CREATE TABLE [dbo].[underdog_stat_type_stage](
        [pickem_stat_id] [nvarchar](36) NOT NULL,
        [stat_type_name] [nvarchar](100) NOT NULL,
        [display_stat] [nvarchar](200) NULL,
        [stat] [nvarchar](200) NULL
    ) ON [PRIMARY];

    PRINT 'Migrated underdog_stat_type to underdog_stat_type_id + pickem_stat_id; recreated underdog_stat_type_stage.';
END
GO

-- Redeploy MergeUnderdogReferenceFromStage from underdog_reference_merge.sql after this script.
