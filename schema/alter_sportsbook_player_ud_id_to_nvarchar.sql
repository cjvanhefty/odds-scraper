-- Migration: sportsbook_player.underdog_player_id bigint -> nvarchar(64) for deployments where underdog_player.id is not numeric.
USE [Props]
GO

IF OBJECT_ID(N'[dbo].[sportsbook_player]', N'U') IS NOT NULL
BEGIN
    DECLARE @t sysname = N'dbo.sportsbook_player';

    IF EXISTS (
        SELECT 1
        FROM sys.columns c
        INNER JOIN sys.types ty ON ty.user_type_id = c.user_type_id
        WHERE c.object_id = OBJECT_ID(@t)
          AND c.name = N'underdog_player_id'
          AND ty.name IN (N'bigint', N'int', N'smallint')
    )
    BEGIN
        IF EXISTS (
            SELECT 1 FROM sys.indexes
            WHERE name = N'UQ_sportsbook_player_ud'
              AND object_id = OBJECT_ID(@t)
        )
            DROP INDEX [UQ_sportsbook_player_ud] ON [dbo].[sportsbook_player];

        ALTER TABLE [dbo].[sportsbook_player]
            ALTER COLUMN [underdog_player_id] [nvarchar](64) NULL;

        CREATE UNIQUE NONCLUSTERED INDEX [UQ_sportsbook_player_ud]
            ON [dbo].[sportsbook_player]([underdog_player_id])
            WHERE [underdog_player_id] IS NOT NULL;
    END
END
GO

