-- FK relationships for unified sportsbook dimensions.
-- Safe to run repeatedly; guards against missing tables/constraints.
USE [Props]
GO

-- sportsbook_league -> sportsbook_sport
IF OBJECT_ID(N'[dbo].[sportsbook_league]', N'U') IS NOT NULL
   AND OBJECT_ID(N'[dbo].[sportsbook_sport]', N'U') IS NOT NULL
   AND COL_LENGTH(N'dbo.sportsbook_league', N'sportsbook_sport_id') IS NOT NULL
BEGIN
    IF NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = N'FK_sportsbook_league_sportsbook_sport')
    BEGIN
        ALTER TABLE [dbo].[sportsbook_league] WITH NOCHECK
            ADD CONSTRAINT [FK_sportsbook_league_sportsbook_sport]
            FOREIGN KEY ([sportsbook_sport_id]) REFERENCES [dbo].[sportsbook_sport] ([sportsbook_sport_id]);
    END
END
GO

-- sportsbook_team -> sportsbook_league
IF OBJECT_ID(N'[dbo].[sportsbook_team]', N'U') IS NOT NULL
   AND OBJECT_ID(N'[dbo].[sportsbook_league]', N'U') IS NOT NULL
   AND COL_LENGTH(N'dbo.sportsbook_team', N'sportsbook_league_id') IS NOT NULL
BEGIN
    IF NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = N'FK_sportsbook_team_sportsbook_league')
    BEGIN
        ALTER TABLE [dbo].[sportsbook_team] WITH NOCHECK
            ADD CONSTRAINT [FK_sportsbook_team_sportsbook_league]
            FOREIGN KEY ([sportsbook_league_id]) REFERENCES [dbo].[sportsbook_league] ([sportsbook_league_id]);
    END
END
GO

-- sportsbook_player -> team/league/sport
IF OBJECT_ID(N'[dbo].[sportsbook_player]', N'U') IS NOT NULL
   AND OBJECT_ID(N'[dbo].[sportsbook_team]', N'U') IS NOT NULL
   AND COL_LENGTH(N'dbo.sportsbook_player', N'sportsbook_team_id') IS NOT NULL
BEGIN
    IF NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = N'FK_sportsbook_player_sportsbook_team')
    BEGIN
        ALTER TABLE [dbo].[sportsbook_player] WITH NOCHECK
            ADD CONSTRAINT [FK_sportsbook_player_sportsbook_team]
            FOREIGN KEY ([sportsbook_team_id]) REFERENCES [dbo].[sportsbook_team] ([sportsbook_team_id]);
    END
END
GO

IF OBJECT_ID(N'[dbo].[sportsbook_player]', N'U') IS NOT NULL
   AND OBJECT_ID(N'[dbo].[sportsbook_league]', N'U') IS NOT NULL
   AND COL_LENGTH(N'dbo.sportsbook_player', N'sportsbook_league_id') IS NOT NULL
BEGIN
    IF NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = N'FK_sportsbook_player_sportsbook_league')
    BEGIN
        ALTER TABLE [dbo].[sportsbook_player] WITH NOCHECK
            ADD CONSTRAINT [FK_sportsbook_player_sportsbook_league]
            FOREIGN KEY ([sportsbook_league_id]) REFERENCES [dbo].[sportsbook_league] ([sportsbook_league_id]);
    END
END
GO

IF OBJECT_ID(N'[dbo].[sportsbook_player]', N'U') IS NOT NULL
   AND OBJECT_ID(N'[dbo].[sportsbook_sport]', N'U') IS NOT NULL
   AND COL_LENGTH(N'dbo.sportsbook_player', N'sportsbook_sport_id') IS NOT NULL
BEGIN
    IF NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = N'FK_sportsbook_player_sportsbook_sport')
    BEGIN
        ALTER TABLE [dbo].[sportsbook_player] WITH NOCHECK
            ADD CONSTRAINT [FK_sportsbook_player_sportsbook_sport]
            FOREIGN KEY ([sportsbook_sport_id]) REFERENCES [dbo].[sportsbook_sport] ([sportsbook_sport_id]);
    END
END
GO

-- sportsbook_game -> home/away team
IF OBJECT_ID(N'[dbo].[sportsbook_game]', N'U') IS NOT NULL
   AND OBJECT_ID(N'[dbo].[sportsbook_team]', N'U') IS NOT NULL
BEGIN
    IF COL_LENGTH(N'dbo.sportsbook_game', N'home_sportsbook_team_id') IS NOT NULL
       AND NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = N'FK_sportsbook_game_home_team')
    BEGIN
        ALTER TABLE [dbo].[sportsbook_game] WITH NOCHECK
            ADD CONSTRAINT [FK_sportsbook_game_home_team]
            FOREIGN KEY ([home_sportsbook_team_id]) REFERENCES [dbo].[sportsbook_team] ([sportsbook_team_id]);
    END

    IF COL_LENGTH(N'dbo.sportsbook_game', N'away_sportsbook_team_id') IS NOT NULL
       AND NOT EXISTS (SELECT 1 FROM sys.foreign_keys WHERE name = N'FK_sportsbook_game_away_team')
    BEGIN
        ALTER TABLE [dbo].[sportsbook_game] WITH NOCHECK
            ADD CONSTRAINT [FK_sportsbook_game_away_team]
            FOREIGN KEY ([away_sportsbook_team_id]) REFERENCES [dbo].[sportsbook_team] ([sportsbook_team_id]);
    END
END
GO
