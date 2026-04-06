# Underdog schema run order

**Reference merge (change-detection, like ParlayPlay):**

1. Ensure base tables exist: run [underdog_stat_type.sql](underdog_stat_type.sql) once (creates `underdog_stat_type` with `underdog_stat_type_id` IDENTITY + unique `pickem_stat_id`, and `underdog_stat_type_stage`). If you still have the old `stat_type_key` primary key, run [alter_underdog_stat_type_identity_pk.sql](alter_underdog_stat_type_identity_pk.sql) once instead of creating from scratch.
2. Ensure stage tables exist and are loaded (`underdog_stat_type_stage`, `underdog_game_stage`, `underdog_player_stage`, `underdog_solo_game_stage`, `underdog_appearance_stage`) — typically truncate + insert per scrape from `underdog_scraper.py`.
3. Run **`underdog_reference_merge.sql`** to create `dbo.MergeUnderdogReferenceFromStage` (merges stat types first, then game / player / solo_game / appearance).
4. Execute: `EXEC [dbo].[MergeUnderdogReferenceFromStage];`

**Projection + history:** remains in Python: `insert_underdog_stage` + `upsert_underdog_from_stage` (history, missing rows, dedupe).

**Join semantics:** `underdog_projection.appearance_id` = `underdog_appearance.id`; `underdog_appearance.player_id` = `underdog_player.id`.

See also: [underdog_projection.sql](underdog_projection.sql), [underdog_projection_stage.sql](underdog_projection_stage.sql), [underdog_stat_type.sql](underdog_stat_type.sql).
