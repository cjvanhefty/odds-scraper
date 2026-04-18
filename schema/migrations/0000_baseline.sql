-- 0000_baseline.sql
--
-- Sentinel baseline migration.
--
-- Purpose: mark every pre-existing (pre-runner) object in the `Props`
-- database as version 0000. This script intentionally does NOT recreate the
-- dozens of idempotent DDL scripts under schema/*.sql. Those remain the
-- documented bootstrap path for a brand-new database until a follow-up PR
-- consolidates them into real versioned migrations (0001+).
--
-- On an existing database (what every deployment is today), run:
--     python scripts/migrate.py --mark-applied 0000
-- which records this version without executing anything.
--
-- On a brand-new database, this file is a no-op; the operator bootstraps the
-- schema via the existing schema/*.sql run orders, then runs --mark-applied
-- 0000 before applying later migrations. A future migration (see plan step
-- 1.1b) will replace this sentinel with a real baseline that bootstraps a
-- fresh DB from zero.
--
-- Editing this file after it has been recorded will be detected by
-- `scripts/migrate.py --check` as checksum drift; don't do that. Author new
-- migrations on top instead.

PRINT N'0000_baseline: sentinel; no-op. See comment block for context.';
GO
