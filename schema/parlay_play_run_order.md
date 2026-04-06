# Parlay Play schema run order

Run these scripts in order (dependencies: raw has none; sport before league/team; league/team before match/player; match/player/stat_type before projection). **[player]** is optional; create it if you use cross-site linking.

**One-time (if you have legacy projection tables):**
- **parlay_play_migrate_from_legacy.sql** – drops old 5-column parlay_play_projection / parlay_play_projection_stage so the new normalized tables can be created. Run before step 8.

**Create tables:**
1. **parlay_play_raw.sql** – full JSON storage (no FKs)
2. **parlay_play_sport.sql** – sport + sport_stage
3. **parlay_play_league.sql** – league + league_stage (FK sport)
4. **parlay_play_team.sql** – team + team_stage (FK sport, league)
5. **parlay_play_match.sql** – match + match_stage (FK sport, league, home_team, away_team)
6. **parlay_play_player.sql** – player + player_stage (FK sport, team)
7. **parlay_play_stat_type.sql** – stat_type + stat_type_stage (no FK)
8. **parlay_play_projection.sql** – projection (FK match, player, stat_type)
9. **parlay_play_projection_stage.sql** – projection_stage (same columns as projection, no FKs)
10. **parlay_play_projection_history.sql** – history snapshots for projections removed/changed from active
11. **link_parlay_play_player_to_player.sql** – add parlay_play_player_id to [player] (requires [player] and parlay_play_player)

**After loading stage tables from scraper/ETL:**
- **parlay_play_reference_merge.sql** – creates `dbo.MergeParlayPlayFromStage` (PrizePicks-like change detection; updates only when changed).
- Then run: `EXEC [dbo].[MergeParlayPlayFromStage]`.
- (Legacy) **parlay_play_merge_stage.sql** – older one-off MERGE batches that update matched rows unconditionally.
