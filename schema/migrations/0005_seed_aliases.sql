-- 0005_seed_aliases.sql
--
-- Plan step 1.3 follow-up — seed ref.person_alias from the operator-
-- reviewed output of scripts/seed_aliases.py (run 2026-04-17).
--
-- Background:
--   seed_aliases.py proposed 23 person alias rows harvested from
--   current sportsbook_player duplicates. The operator ran the
--   proposals file directly against the live DB by accident, so all
--   23 rows are already present in ref.person_alias. On review, row 17
--   of the proposal was rejected because the two underlying
--   sportsbook_player rows are two different real soccer players:
--
--     sportsbook_player_id=18585: 'Diego Gonzalez'  -- Houston Dynamo  #39 (Midfielder)
--     sportsbook_player_id=9301:  'Diego González'  -- Atlas (Liga MX) #11 (Attacker)
--
--   Both happen to normalize to 'diego gonzalez' and collide under the
--   person-name alias key (canonical_league_id=82, source='prizepicks',
--   alias_normalized='diego gonzalez'). Merging them would mis-attribute
--   props from one player to the other.
--
--   This migration:
--     1. DELETEs the stale Diego González row that was inserted
--        directly (so the live DB matches the committed seed).
--     2. Re-applies all 22 approved rows idempotently. Rows already
--        present from the direct apply match the PK and are skipped;
--        a fresh DB gets the full 22 rows from this MERGE alone.
--
-- Re-running this migration is a no-op once applied. ref.person_alias
-- PK is (canonical_league_id, source, alias_normalized).

SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
GO

-- 1. Remove the rejected Diego González / Diego Gonzalez row.
DELETE FROM [ref].[person_alias]
WHERE [canonical_league_id] = 82
  AND [source] = N'prizepicks'
  AND [alias_normalized] = N'diego gonzalez';
GO

-- 2. Apply the 22 approved rows.
MERGE [ref].[person_alias] AS t USING (VALUES
    (82,  N'prizepicks', N'vitinha',           N'Vitinha',                N'Vítinha',                  N'player row 9168 raw=''Vítinha'' -> ''Vitinha''  (group size 2, variant count 1)'),
    (265, N'prizepicks', N'dexter',            N'dexter',                 N'--dexter---',              N'player row 8795 raw=''--dexter---'' -> ''dexter''  (group size 2, variant count 1)'),
    (265, N'prizepicks', N'alistair',          N'aliStair',               N'alistair',                 N'player row 8794 raw=''alistair'' -> ''aliStair''  (group size 2, variant count 1)'),
    (1,   N'prizepicks', N'robert macintyre',  N'Robert MacIntyre',       N'Robert Macintyre',         N'player row 3541 raw=''Robert Macintyre'' -> ''Robert MacIntyre''  (group size 2, variant count 1)'),
    (121, N'prizepicks', N'papiteero',         N'Papiteero',              N'PapiTeero',                N'player row 18208 raw=''PapiTeero'' -> ''Papiteero''  (group size 2, variant count 1)'),
    (265, N'prizepicks', N'lucky',             N'Lucky',                  N'lucky',                    N'player row 16513 raw=''lucky'' -> ''Lucky''  (group size 2, variant count 1)'),
    (151, N'prizepicks', N'dzanan musa',       N'Dzanan Musa',            N'Džanan Musa',              N'player row 15101 raw=''Džanan Musa'' -> ''Dzanan Musa''  (group size 2, variant count 1)'),
    (265, N'prizepicks', N'shadiy',            N'shadiy',                 N'Shadiy',                   N'player row 8768 raw=''Shadiy'' -> ''shadiy''  (group size 2, variant count 1)'),
    (265, N'prizepicks', N'hazr',              N'hazr',                   N'Hazr',                     N'player row 8770 raw=''Hazr'' -> ''hazr''  (group size 2, variant count 1)'),
    (284, N'prizepicks', N'renars uscins',     N'Renārs Uščins',          N'Renars Uscins',            N'player row 15568 raw=''Renars Uscins'' -> ''Renārs Uščins''  (group size 2, variant count 1)'),
    (284, N'prizepicks', N'julian koster',     N'Julian Koster',          N'Julian Köster',            N'player row 16908 raw=''Julian Köster'' -> ''Julian Koster''  (group size 2, variant count 1)'),
    (151, N'prizepicks', N'theo maledon',      N'Theo Maledon',           N'Théo Maledon',             N'player row 12795 raw=''Théo Maledon'' -> ''Theo Maledon''  (group size 2, variant count 1)'),
    (265, N'prizepicks', N'910',               N'910',                    N'910-',                     N'player row 6877 raw=''910-'' -> ''910''  (group size 2, variant count 1)'),
    (265, N'prizepicks', N'dem0n',             N'dem0n',                  N'Dem0N',                    N'player row 16971 raw=''Dem0N'' -> ''dem0n''  (group size 2, variant count 1)'),
    (265, N'prizepicks', N'krabeni',           N'Krabeni',                N'krabeni',                  N'player row 16970 raw=''krabeni'' -> ''Krabeni''  (group size 2, variant count 1)'),
    (265, N'prizepicks', N'elige',             N'EliGe',                  N'EliGE',                    N'player row 13802 raw=''EliGE'' -> ''EliGe''  (group size 2, variant count 1)'),
    (82,  N'prizepicks', N'santiago munoz',    N'Santiago Muñóz',         N'Santiago Múñoz',           N'player row 16772 raw=''Santiago Múñoz'' -> ''Santiago Muñóz''  (group size 2, variant count 1)'),
    (265, N'prizepicks', N'mo0n',              N'mo0N',                   N'mo0n',                     N'player row 16388 raw=''mo0n'' -> ''mo0N''  (group size 2, variant count 1)'),
    (151, N'prizepicks', N'azuolas tubelis',   N'Ąžuolas Tubelis',        N'Azuolas Tubelis',          N'player row 15380 raw=''Azuolas Tubelis'' -> ''Ąžuolas Tubelis''  (group size 2, variant count 1)'),
    (265, N'prizepicks', N'heavygod',          N'HeavyGod',               N'HeavyGoD',                 N'player row 18960 raw=''HeavyGoD'' -> ''HeavyGod''  (group size 2, variant count 1)'),
    (82,  N'_any',       N'dje davilla',       N'Dje D''Avilla',          N'Djé D''Avilla',            N'player row 114048 raw="Djé D''Avilla" -> "Dje D''Avilla"  (group size 2, variant count 1)'),
    (285, N'prizepicks', N'kral j + kral o',   N'Kral J + Kral O',        N'Král J + Král O',          N'player row 18276 raw=''Král J + Král O'' -> ''Kral J + Kral O''  (group size 2, variant count 1)')
) AS s(canonical_league_id, source, alias_normalized, canonical_display_name, alias_raw, notes)
    ON t.canonical_league_id = s.canonical_league_id
   AND t.source = s.source
   AND t.alias_normalized = s.alias_normalized
WHEN NOT MATCHED BY TARGET THEN
    INSERT (canonical_league_id, source, alias_normalized, canonical_display_name, alias_raw, notes)
    VALUES (s.canonical_league_id, s.source, s.alias_normalized, s.canonical_display_name, s.alias_raw, s.notes);
GO
