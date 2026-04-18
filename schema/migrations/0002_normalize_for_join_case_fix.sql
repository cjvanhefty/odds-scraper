-- 0002_normalize_for_join_case_fix.sql
--
-- Plan step 1.2 follow-up — fix case-insensitive-collation bug in
-- dbo.fn_normalize_for_join.
--
-- Bug background:
--   scripts/check_sql_udfs.py reported one disagreement against the shared
--   fixtures after migration 0001 was applied on SQL Server 2016:
--
--     dbo.fn_normalize_for_join('Shots on Target')
--       -> 'Shots on Target'   (expected 'Shots On Target')
--
--   Trace:
--     1. fn_apply_join_aliases('Shots on Target') correctly RETURNs
--        N'Shots On Target' via the IN-list on line 77 of 0001.
--     2. Back in fn_normalize_for_join, the guard
--            IF @pre <> @trimmed RETURN @pre;
--        compares the two strings under the database's default
--        collation (case-insensitive in this deployment), so
--        N'Shots On Target' <> N'Shots on Target' evaluates to FALSE.
--     3. Control falls through, fn_canonical_stat_by_alnum returns NULL
--        for the alnum key 'shotsontarget' (no row with that exact key),
--        and the function returns the trimmed input unchanged -- losing
--        the alias-normalized capitalization.
--
-- Fix: force a binary collation on the single control-flow comparison
-- that distinguishes "alias applied" from "alias didn't apply". Any
-- COLLATE clause that is both case- and accent-sensitive works;
-- Latin1_General_BIN2 is the standard byte-order choice and is available
-- on every supported SQL Server.
--
-- Only fn_normalize_for_join is affected. The IN-list comparisons inside
-- fn_apply_join_aliases intentionally use the default (CI) collation so
-- inputs like 'SHOTS ON TARGET' or 'shots ON target' also bucket into the
-- alias; those are not changed.
--
-- Offline parity tests (tests/test_sql_parity.py) did not catch this
-- because their Python simulator uses ==/!= which are always case-
-- sensitive, so the SIM matched Python (correct) while the real engine
-- under CI collation did not. scripts/check_sql_udfs.py -- which runs
-- the UDFs on a live DB -- did catch it. The operator playbook after
-- this migration is the same.

SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
GO

-- ALTER (not DROP/CREATE) because fn_normalize_for_join is WITH
-- SCHEMABINDING and dependents may already reference it by the time this
-- migration runs. ALTER preserves the schemabound dependency chain.
ALTER FUNCTION dbo.fn_normalize_for_join(@s nvarchar(120))
RETURNS nvarchar(120)
WITH SCHEMABINDING
AS
BEGIN
    IF @s IS NULL RETURN N'';
    DECLARE @trimmed nvarchar(120) = LTRIM(RTRIM(@s));
    IF LEN(@trimmed) = 0 RETURN N'';
    DECLARE @pre nvarchar(120) = dbo.fn_apply_join_aliases(@trimmed);
    -- Force binary collation so a case-only alias ('Shots on Target' ->
    -- 'Shots On Target') still trips the "alias applied" branch.
    IF @pre <> @trimmed COLLATE Latin1_General_BIN2 RETURN @pre;
    DECLARE @mapped nvarchar(120) = dbo.fn_canonical_stat_by_alnum(dbo.fn_alnum_key(@trimmed));
    IF @mapped IS NULL RETURN @trimmed;
    RETURN dbo.fn_apply_join_aliases(@mapped);
END
GO
