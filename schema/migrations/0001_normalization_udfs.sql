-- 0001_normalization_udfs.sql
--
-- Plan step 1.2 — cross-book normalization UDFs.
--
-- Creates the deterministic, SCHEMABINDING-safe scalar UDFs used by
-- cross-book dedup (the 'five LeBrons' fix in plan section 3.0) and by the
-- unified sportsbook_* dimension indexes that arrive in plan step 1.5.
--
-- Every UDF mirrors a Python helper in cross_book_stat_normalize.py. Fixture
-- parity is asserted by:
--   * tests/test_normalization.py (Python side, CI).
--   * scripts/check_sql_udfs.py   (live DB side; operator runs after migrate).
--
-- The CANONICAL_STAT_BY_ALNUM mapping is inlined here as a VALUES table.
-- Plan step 1.3 introduces ref.stat_alias and this function will be replaced
-- (via a new migration) with a read from that table.
--
-- This migration file is auto-generated from cross_book_stat_normalize.py by
-- scripts/_gen_0001_migration.py. To regenerate:
--     python3 scripts/_gen_0001_migration.py > schema/migrations/0001_normalization_udfs.sql
-- Do NOT edit a migration file after it has been applied; add a new
-- migration on top instead (see schema/migrations/README.md).

SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
GO

-- dbo.fn_alnum_key(@s)
-- Mirror of Python _alnum_key: lowercase @s then strip every character that
-- is not [a-z0-9]. Used by the stat canonicalization lookup.
IF OBJECT_ID(N'dbo.fn_alnum_key', N'FN') IS NOT NULL DROP FUNCTION dbo.fn_alnum_key;
GO
CREATE FUNCTION dbo.fn_alnum_key(@s nvarchar(400))
RETURNS nvarchar(400)
WITH SCHEMABINDING
AS
BEGIN
    IF @s IS NULL RETURN N'';
    DECLARE @r  nvarchar(400) = LOWER(@s);
    DECLARE @n  int = LEN(@r);
    DECLARE @i  int = 1;
    DECLARE @c  nchar(1);
    DECLARE @out nvarchar(400) = N'';
    WHILE @i <= @n
    BEGIN
        SET @c = SUBSTRING(@r, @i, 1);
        IF (@c >= N'a' AND @c <= N'z') OR (@c >= N'0' AND @c <= N'9')
            SET @out = @out + @c;
        SET @i += 1;
    END
    RETURN @out;
END
GO

-- dbo.fn_apply_join_aliases(@s)
-- Mirror of Python apply_join_aliases: label-level bucketing for cross-book
-- joins. Returns the trimmed input when no alias applies (matches Python's
-- "return t" branch).
IF OBJECT_ID(N'dbo.fn_apply_join_aliases', N'FN') IS NOT NULL DROP FUNCTION dbo.fn_apply_join_aliases;
GO
CREATE FUNCTION dbo.fn_apply_join_aliases(@s nvarchar(120))
RETURNS nvarchar(120)
WITH SCHEMABINDING
AS
BEGIN
    IF @s IS NULL RETURN N'';
    DECLARE @t nvarchar(120) = LTRIM(RTRIM(@s));
    IF LEN(@t) = 0 RETURN N'';
    IF @t IN (N'Blocks', N'Blocked Shots') RETURN N'Blocks__Blocked_Shots';
    IF @t IN (N'Double Doubles', N'Double-Doubles', N'Double-Double', N'Double Double') RETURN N'Double_Doubles';
    IF @t IN (N'Triple Doubles', N'Triple-Doubles', N'Triple-Double') RETURN N'Triple_Doubles';
    IF @t IN (N'Blocks + Steals', N'Blocks+Steals') RETURN N'Blks_Stls';
    IF @t LIKE N'Blks+Stls%' RETURN N'Blks_Stls';
    IF @t IN (N'3-PT Attempted', N'3 Pointers Attempted', N'3s Attempted') RETURN N'FG3A';
    IF @t IN (N'3 Pointers', N'3 Pointers Made', N'3-PT Made', N'3-Pointers Made') RETURN N'FG3M';
    IF @t IN (N'Hits+Runs+RBIs', N'Hits + Runs + RBIs') RETURN N'Hits+Runs+RBIs';
    IF @t IN (N'Shots On Target', N'Shots on Target') RETURN N'Shots On Target';
    IF @t IN (N'Goal + Assist', N'Goals + Assists') RETURN N'Goal + Assist';
    IF @t IN (N'Passes Attempted', N'Passes') RETURN N'Passes Attempted';
    RETURN @t;
END
GO

-- dbo.fn_canonical_stat_by_alnum(@k)
-- Lookup of alnum key -> PrizePicks-style canonical label. Returns NULL on
-- miss so callers can distinguish "no mapping" from "maps to empty".
-- Contents mirror CANONICAL_STAT_BY_ALNUM in cross_book_stat_normalize.py;
-- plan step 1.3 will move this to a ref.stat_alias table lookup.
IF OBJECT_ID(N'dbo.fn_canonical_stat_by_alnum', N'FN') IS NOT NULL DROP FUNCTION dbo.fn_canonical_stat_by_alnum;
GO
CREATE FUNCTION dbo.fn_canonical_stat_by_alnum(@k nvarchar(120))
RETURNS nvarchar(120)
WITH SCHEMABINDING
AS
BEGIN
    IF @k IS NULL OR LEN(@k) = 0 RETURN NULL;
    DECLARE @v nvarchar(120);
    SELECT @v = m.v
    FROM (VALUES
        (N'pts', N'Points'),
        (N'points', N'Points'),
        (N'point', N'Points'),
        (N'reb', N'Rebounds'),
        (N'rebounds', N'Rebounds'),
        (N'ast', N'Assists'),
        (N'assists', N'Assists'),
        (N'stl', N'Steals'),
        (N'steals', N'Steals'),
        (N'blk', N'Blocks'),
        (N'blocks', N'Blocks'),
        (N'blockedshots', N'Blocked Shots'),
        (N'tov', N'Turnovers'),
        (N'to', N'Turnovers'),
        (N'turnovers', N'Turnovers'),
        (N'turnover', N'Turnovers'),
        (N'blksstls', N'Blks+Stls'),
        (N'blockssteals', N'Blks+Stls'),
        (N'stocks', N'Blks+Stls'),
        (N'fantasypoints', N'Fantasy Score'),
        (N'fantasyscore', N'Fantasy Score'),
        (N'ptsreb', N'Pts+Rebs'),
        (N'ptsrebs', N'Pts+Rebs'),
        (N'ptsast', N'Pts+Asts'),
        (N'ptsasts', N'Pts+Asts'),
        (N'rebast', N'Rebs+Asts'),
        (N'rebsasts', N'Rebs+Asts'),
        (N'ptsrebast', N'Pts+Rebs+Asts'),
        (N'ptsrebsasts', N'Pts+Rebs+Asts'),
        (N'ptsrebsast', N'Pts+Rebs+Asts'),
        (N'pointsreboundsassists', N'Pts+Rebs+Asts'),
        (N'pointsrebounds', N'Pts+Rebs'),
        (N'pointsassists', N'Pts+Asts'),
        (N'reboundsassists', N'Rebs+Asts'),
        (N'doubledoubles', N'Double-Double'),
        (N'doubledouble', N'Double-Double'),
        (N'tripledoubles', N'Triple-Double'),
        (N'oreb', N'Offensive Rebounds'),
        (N'dreb', N'Defensive Rebounds'),
        (N'3pm', N'3-PT Made'),
        (N'3ptm', N'3-PT Made'),
        (N'3ptmade', N'3-PT Made'),
        (N'3pmade', N'3-PT Made'),
        (N'3pointersmade', N'3 Pointers Made'),
        (N'threepointsmade', N'3 Pointers Made'),
        (N'threes', N'3 Pointers Made'),
        (N'3pt', N'3 Pointers'),
        (N'3sattempted', N'3-PT Attempted'),
        (N'threepointsatt', N'3-PT Attempted'),
        (N'threepointersattempted', N'3 Pointers Attempted'),
        (N'attemptedthrees', N'3-PT Attempted'),
        (N'fgattempted', N'FG Attempted'),
        (N'fieldgoalsatt', N'FG Attempted'),
        (N'fgmade', N'FG Made'),
        (N'ftmade', N'Free Throws Made'),
        (N'freethrowsmade', N'Free Throws Made'),
        (N'freethrowsattempted', N'Free Throws Attempted'),
        (N'twopointersmade', N'Two Pointers Made'),
        (N'twopointersattempted', N'Two Pointers Attempted'),
        (N'bbpoints', N'Points'),
        (N'bbrebounds', N'Rebounds'),
        (N'bbassists', N'Assists'),
        (N'bbsteals', N'Steals'),
        (N'bbblocks', N'Blocks'),
        (N'bbturnovers', N'Turnovers'),
        (N'bbpersonal', N'Personal Fouls'),
        (N'bbdreb', N'Defensive Rebounds'),
        (N'bboreb', N'Offensive Rebounds'),
        (N'bbfgmade', N'FG Made'),
        (N'bbfgattempted', N'FG Attempted'),
        (N'bbtwopointersmade', N'Two Pointers Made'),
        (N'bbtwopointersattempted', N'Two Pointers Attempted'),
        (N'bbfreethrowsmade', N'Free Throws Made'),
        (N'bbfreethrowsattempted', N'Free Throws Attempted'),
        (N'bbptsreb', N'Pts+Rebs'),
        (N'bbptsast', N'Pts+Asts'),
        (N'bbptsrebast', N'Pts+Rebs+Asts'),
        (N'bbrebast', N'Rebs+Asts'),
        (N'bbdd', N'Double-Double'),
        (N'bbtd', N'Triple-Double'),
        (N'bbparlaypoints', N'Fantasy Score'),
        (N'bbfirstbasket', N'First Point Scorer'),
        (N'bbthreepointersmade', N'3 Pointers Made'),
        (N'bbthreepointersattempted', N'3-PT Attempted'),
        (N'bbthreepointfieldgoalsattempted', N'3-PT Attempted'),
        (N'bbfg3a', N'3-PT Attempted'),
        (N'bb3ptattempted', N'3-PT Attempted'),
        (N'ptsrebsasts1h', N'1H Pts + Rebs + Asts'),
        (N'period1points', N'1Q Points'),
        (N'period12points', N'1H Points'),
        (N'period1rebounds', N'1Q Rebounds'),
        (N'period12rebounds', N'1H Rebounds'),
        (N'period1assists', N'1Q Assists'),
        (N'period12assists', N'1H Assists'),
        (N'period1threepointsmade', N'1Q 3-Pointers Made'),
        (N'period12threepointsmade', N'1H 3-Pointers Made'),
        (N'period1ptsrebsasts', N'1Q Pts + Rebs + Asts'),
        (N'period12ptsrebsasts', N'1H Pts + Rebs + Asts'),
        (N'babhrr', N'Hits+Runs+RBIs'),
        (N'hitsrunsrbis', N'Hits+Runs+RBIs'),
        (N'hrr', N'Hits+Runs+RBIs'),
        (N'babtotalbases', N'Total Bases'),
        (N'babhits', N'Hits'),
        (N'babrbi', N'RBIs'),
        (N'babruns', N'Runs'),
        (N'babsingles', N'Singles'),
        (N'babdoubles', N'Doubles'),
        (N'babtriples', N'Triples'),
        (N'babhomeruns', N'Home Runs'),
        (N'babstolenbases', N'Stolen Bases'),
        (N'babwalks', N'Walks'),
        (N'babpitchingstrikeouts', N'Pitcher Strikeouts'),
        (N'babstrikeouts', N'Hitter Strikeouts'),
        (N'babpitchingouts', N'Pitching Outs'),
        (N'babpitchesthrown', N'Pitches Thrown'),
        (N'babhitsallowed', N'Hits Allowed'),
        (N'babwalksallowed', N'Walks Allowed'),
        (N'babparlaypoints', N'Hitter Fantasy Score'),
        (N'earnedrunsallowed', N'Earned Runs Allowed'),
        (N'firstinningrunsallowed', N'1st Inning Runs Allowed'),
        (N'socshots', N'Shots'),
        (N'socshotsontarget', N'Shots On Target'),
        (N'socshotsongoal', N'Shots On Target'),
        (N'soctackles', N'Tackles'),
        (N'socgoals', N'Goals'),
        (N'socassists', N'Assists'),
        (N'socfouls', N'Fouls'),
        (N'socfouled', N'Fouled'),
        (N'soccards', N'Cards'),
        (N'socgoalassist', N'Goal + Assist'),
        (N'socdribblesattempted', N'Attempted Dribbles'),
        (N'soccrosses', N'Crosses'),
        (N'socgksaves', N'Goalie Saves'),
        (N'socoffsides', N'Offsides'),
        (N'passyards', N'Pass Yards'),
        (N'passingyards', N'Pass Yards'),
        (N'passtds', N'Pass TDs'),
        (N'passingtds', N'Pass TDs'),
        (N'rushyards', N'Rush Yards'),
        (N'rushingyards', N'Rush Yards'),
        (N'rushtds', N'Rush TDs'),
        (N'rushingtds', N'Rush TDs'),
        (N'recyards', N'Receiving Yards'),
        (N'receivingyards', N'Receiving Yards'),
        (N'rectds', N'Rec TDs'),
        (N'receivingtds', N'Rec TDs'),
        (N'rushtrectds', N'Rush + Rec TDs'),
        (N'rushrectds', N'Rush + Rec TDs'),
        (N'significantstrikes', N'Significant Strikes'),
        (N'takedowns', N'Takedowns'),
        (N'tackledowns', N'Takedowns'),
        (N'totalrounds', N'Total Rounds'),
        (N'fighttimemins', N'Fight Time (Mins)'),
        (N'tentotalgames', N'Total Games'),
        (N'tengameswon', N'Total Games Won'),
        (N'tensetswon', N'Total Sets'),
        (N'tenaces', N'Aces'),
        (N'escsgokills', N'MAPS 1-2 Kills'),
        (N'escsgoheadshots', N'MAPS 1-2 Headshots'),
        (N'eslolkills', N'MAPS 1-2 Kills'),
        (N'esvalkills', N'MAPS 1-2 Kills'),
        (N'esdota2kills', N'MAPS 1-3 Kills'),
        (N'crickruns', N'Runs'),
        (N'crickfours', N'Fours'),
        (N'cricksixes', N'Sixes')
    ) AS m(k, v)
    WHERE m.k = @k;
    RETURN @v;
END
GO

-- dbo.fn_normalize_stat_basic(@s)
-- Mirror of Python normalize_stat_basic: canonicalize a provider stat label
-- to the PrizePicks-style display label, falling back to the trimmed input.
IF OBJECT_ID(N'dbo.fn_normalize_stat_basic', N'FN') IS NOT NULL DROP FUNCTION dbo.fn_normalize_stat_basic;
GO
CREATE FUNCTION dbo.fn_normalize_stat_basic(@s nvarchar(120))
RETURNS nvarchar(120)
WITH SCHEMABINDING
AS
BEGIN
    IF @s IS NULL RETURN N'';
    DECLARE @trimmed nvarchar(120) = LTRIM(RTRIM(@s));
    IF LEN(@trimmed) = 0 RETURN N'';
    DECLARE @mapped nvarchar(120) = dbo.fn_canonical_stat_by_alnum(dbo.fn_alnum_key(@trimmed));
    IF @mapped IS NULL RETURN @trimmed;
    RETURN @mapped;
END
GO

-- dbo.fn_normalize_for_join(@s)
-- Mirror of Python normalize_for_join: bucket aliases win first; otherwise
-- canonicalize via the alnum map and re-bucket the canonical form.
IF OBJECT_ID(N'dbo.fn_normalize_for_join', N'FN') IS NOT NULL DROP FUNCTION dbo.fn_normalize_for_join;
GO
CREATE FUNCTION dbo.fn_normalize_for_join(@s nvarchar(120))
RETURNS nvarchar(120)
WITH SCHEMABINDING
AS
BEGIN
    IF @s IS NULL RETURN N'';
    DECLARE @trimmed nvarchar(120) = LTRIM(RTRIM(@s));
    IF LEN(@trimmed) = 0 RETURN N'';
    DECLARE @pre nvarchar(120) = dbo.fn_apply_join_aliases(@trimmed);
    IF @pre <> @trimmed RETURN @pre;
    DECLARE @mapped nvarchar(120) = dbo.fn_canonical_stat_by_alnum(dbo.fn_alnum_key(@trimmed));
    IF @mapped IS NULL RETURN @trimmed;
    RETURN dbo.fn_apply_join_aliases(@mapped);
END
GO

-- dbo.fn_normalize_person_name(@n)
-- Mirror of Python normalize_person_name. Steps, in order:
--   1. Lowercase.
--   2. Strip diacritics on common Latin-1/Extended-A accented characters
--      via stacked REPLACE(@s, NCHAR(cp), 'ascii'). TRANSLATE would be
--      cleaner but is SQL Server 2017+; this works on 2016 and earlier.
--      The (code point, ascii) pairs are auto-generated from
--      unicodedata.normalize('NFKD', ch) for cp in [0x00C0..0x017F] where
--      the stripped form is exactly one ASCII character. Characters whose
--      NFKD strip is a no-op (O-slash, stroked L, dotless i, long s)
--      are intentionally omitted so SQL leaves them alone, matching
--      Python's NFKD behavior.
--   3. Remove ., , ' " - and treat tab/CR/LF as spaces.
--   4. Collapse consecutive spaces (stacked REPLACE handles up to 16 in a run).
--   5. Trim.
--   6. Drop a single trailing generational suffix from {jr, sr, ii, iii, iv, v}.
-- NULL input maps to N'' to match the Python helper.
IF OBJECT_ID(N'dbo.fn_normalize_person_name', N'FN') IS NOT NULL DROP FUNCTION dbo.fn_normalize_person_name;
GO
CREATE FUNCTION dbo.fn_normalize_person_name(@n nvarchar(255))
RETURNS nvarchar(255)
WITH SCHEMABINDING
AS
BEGIN
    IF @n IS NULL RETURN N'';
    DECLARE @s nvarchar(300) = @n;
    -- 1. Lowercase first so we only need one REPLACE per lowercase code point.
    SET @s = LOWER(@s);
    -- 2. Diacritic strip (81 pairs, auto-generated from unicodedata).
    SET @s = REPLACE(@s, NCHAR(224), N'a');  -- à -> a
    SET @s = REPLACE(@s, NCHAR(225), N'a');  -- á -> a
    SET @s = REPLACE(@s, NCHAR(226), N'a');  -- â -> a
    SET @s = REPLACE(@s, NCHAR(227), N'a');  -- ã -> a
    SET @s = REPLACE(@s, NCHAR(228), N'a');  -- ä -> a
    SET @s = REPLACE(@s, NCHAR(229), N'a');  -- å -> a
    SET @s = REPLACE(@s, NCHAR(231), N'c');  -- ç -> c
    SET @s = REPLACE(@s, NCHAR(232), N'e');  -- è -> e
    SET @s = REPLACE(@s, NCHAR(233), N'e');  -- é -> e
    SET @s = REPLACE(@s, NCHAR(234), N'e');  -- ê -> e
    SET @s = REPLACE(@s, NCHAR(235), N'e');  -- ë -> e
    SET @s = REPLACE(@s, NCHAR(236), N'i');  -- ì -> i
    SET @s = REPLACE(@s, NCHAR(237), N'i');  -- í -> i
    SET @s = REPLACE(@s, NCHAR(238), N'i');  -- î -> i
    SET @s = REPLACE(@s, NCHAR(239), N'i');  -- ï -> i
    SET @s = REPLACE(@s, NCHAR(241), N'n');  -- ñ -> n
    SET @s = REPLACE(@s, NCHAR(242), N'o');  -- ò -> o
    SET @s = REPLACE(@s, NCHAR(243), N'o');  -- ó -> o
    SET @s = REPLACE(@s, NCHAR(244), N'o');  -- ô -> o
    SET @s = REPLACE(@s, NCHAR(245), N'o');  -- õ -> o
    SET @s = REPLACE(@s, NCHAR(246), N'o');  -- ö -> o
    SET @s = REPLACE(@s, NCHAR(249), N'u');  -- ù -> u
    SET @s = REPLACE(@s, NCHAR(250), N'u');  -- ú -> u
    SET @s = REPLACE(@s, NCHAR(251), N'u');  -- û -> u
    SET @s = REPLACE(@s, NCHAR(252), N'u');  -- ü -> u
    SET @s = REPLACE(@s, NCHAR(253), N'y');  -- ý -> y
    SET @s = REPLACE(@s, NCHAR(255), N'y');  -- ÿ -> y
    SET @s = REPLACE(@s, NCHAR(257), N'a');  -- ā -> a
    SET @s = REPLACE(@s, NCHAR(259), N'a');  -- ă -> a
    SET @s = REPLACE(@s, NCHAR(261), N'a');  -- ą -> a
    SET @s = REPLACE(@s, NCHAR(263), N'c');  -- ć -> c
    SET @s = REPLACE(@s, NCHAR(265), N'c');  -- ĉ -> c
    SET @s = REPLACE(@s, NCHAR(267), N'c');  -- ċ -> c
    SET @s = REPLACE(@s, NCHAR(269), N'c');  -- č -> c
    SET @s = REPLACE(@s, NCHAR(271), N'd');  -- ď -> d
    SET @s = REPLACE(@s, NCHAR(275), N'e');  -- ē -> e
    SET @s = REPLACE(@s, NCHAR(277), N'e');  -- ĕ -> e
    SET @s = REPLACE(@s, NCHAR(279), N'e');  -- ė -> e
    SET @s = REPLACE(@s, NCHAR(281), N'e');  -- ę -> e
    SET @s = REPLACE(@s, NCHAR(283), N'e');  -- ě -> e
    SET @s = REPLACE(@s, NCHAR(285), N'g');  -- ĝ -> g
    SET @s = REPLACE(@s, NCHAR(287), N'g');  -- ğ -> g
    SET @s = REPLACE(@s, NCHAR(289), N'g');  -- ġ -> g
    SET @s = REPLACE(@s, NCHAR(291), N'g');  -- ģ -> g
    SET @s = REPLACE(@s, NCHAR(293), N'h');  -- ĥ -> h
    SET @s = REPLACE(@s, NCHAR(297), N'i');  -- ĩ -> i
    SET @s = REPLACE(@s, NCHAR(299), N'i');  -- ī -> i
    SET @s = REPLACE(@s, NCHAR(301), N'i');  -- ĭ -> i
    SET @s = REPLACE(@s, NCHAR(303), N'i');  -- į -> i
    SET @s = REPLACE(@s, NCHAR(309), N'j');  -- ĵ -> j
    SET @s = REPLACE(@s, NCHAR(311), N'k');  -- ķ -> k
    SET @s = REPLACE(@s, NCHAR(314), N'l');  -- ĺ -> l
    SET @s = REPLACE(@s, NCHAR(316), N'l');  -- ļ -> l
    SET @s = REPLACE(@s, NCHAR(318), N'l');  -- ľ -> l
    SET @s = REPLACE(@s, NCHAR(324), N'n');  -- ń -> n
    SET @s = REPLACE(@s, NCHAR(326), N'n');  -- ņ -> n
    SET @s = REPLACE(@s, NCHAR(328), N'n');  -- ň -> n
    SET @s = REPLACE(@s, NCHAR(333), N'o');  -- ō -> o
    SET @s = REPLACE(@s, NCHAR(335), N'o');  -- ŏ -> o
    SET @s = REPLACE(@s, NCHAR(337), N'o');  -- ő -> o
    SET @s = REPLACE(@s, NCHAR(341), N'r');  -- ŕ -> r
    SET @s = REPLACE(@s, NCHAR(343), N'r');  -- ŗ -> r
    SET @s = REPLACE(@s, NCHAR(345), N'r');  -- ř -> r
    SET @s = REPLACE(@s, NCHAR(347), N's');  -- ś -> s
    SET @s = REPLACE(@s, NCHAR(349), N's');  -- ŝ -> s
    SET @s = REPLACE(@s, NCHAR(351), N's');  -- ş -> s
    SET @s = REPLACE(@s, NCHAR(353), N's');  -- š -> s
    SET @s = REPLACE(@s, NCHAR(355), N't');  -- ţ -> t
    SET @s = REPLACE(@s, NCHAR(357), N't');  -- ť -> t
    SET @s = REPLACE(@s, NCHAR(361), N'u');  -- ũ -> u
    SET @s = REPLACE(@s, NCHAR(363), N'u');  -- ū -> u
    SET @s = REPLACE(@s, NCHAR(365), N'u');  -- ŭ -> u
    SET @s = REPLACE(@s, NCHAR(367), N'u');  -- ů -> u
    SET @s = REPLACE(@s, NCHAR(369), N'u');  -- ű -> u
    SET @s = REPLACE(@s, NCHAR(371), N'u');  -- ų -> u
    SET @s = REPLACE(@s, NCHAR(373), N'w');  -- ŵ -> w
    SET @s = REPLACE(@s, NCHAR(375), N'y');  -- ŷ -> y
    SET @s = REPLACE(@s, NCHAR(378), N'z');  -- ź -> z
    SET @s = REPLACE(@s, NCHAR(380), N'z');  -- ż -> z
    SET @s = REPLACE(@s, NCHAR(382), N'z');  -- ž -> z
    SET @s = REPLACE(@s, NCHAR(383), N's');  -- ſ -> s
    -- 3. Remove . , ' " - and normalize whitespace chars.
    SET @s = REPLACE(@s, N'.', N'');
    SET @s = REPLACE(@s, N',', N'');
    SET @s = REPLACE(@s, N'''', N'');
    SET @s = REPLACE(@s, N'"', N'');
    SET @s = REPLACE(@s, N'-', N'');
    SET @s = REPLACE(@s, NCHAR(9), N' ');
    SET @s = REPLACE(@s, NCHAR(13), N' ');
    SET @s = REPLACE(@s, NCHAR(10), N' ');
    -- 4. Collapse runs of spaces (stacked REPLACE; handles up to 16 consecutive spaces).
    SET @s = REPLACE(REPLACE(REPLACE(REPLACE(@s, N'  ', N' '), N'  ', N' '), N'  ', N' '), N'  ', N' ');
    -- 5. Trim.
    SET @s = LTRIM(RTRIM(@s));
    IF LEN(@s) = 0 RETURN N'';
    -- 6. Drop trailing generational suffix (only when there is a preceding token).
    IF CHARINDEX(N' ', @s) > 0
    BEGIN
        DECLARE @spc int = LEN(@s) - CHARINDEX(N' ', REVERSE(@s)) + 1;
        DECLARE @last nvarchar(10) = SUBSTRING(@s, @spc + 1, LEN(@s) - @spc);
        IF @last IN (N'jr', N'sr', N'ii', N'iii', N'iv', N'v')
            SET @s = LTRIM(RTRIM(SUBSTRING(@s, 1, @spc - 1)));
    END
    RETURN @s;
END
GO

-- dbo.fn_normalize_team_abbrev(@abbrev, @canonical_league_id)
-- Mirror of Python normalize_team_abbrev: uppercase + trim only at step 1.2.
-- The @canonical_league_id parameter is accepted now so callers don't need
-- to change when plan step 1.3 layers a ref.team_alias lookup on top.
IF OBJECT_ID(N'dbo.fn_normalize_team_abbrev', N'FN') IS NOT NULL DROP FUNCTION dbo.fn_normalize_team_abbrev;
GO
CREATE FUNCTION dbo.fn_normalize_team_abbrev(@abbrev nvarchar(20), @canonical_league_id int)
RETURNS nvarchar(20)
WITH SCHEMABINDING
AS
BEGIN
    IF @abbrev IS NULL RETURN N'';
    DECLARE @t nvarchar(20) = LTRIM(RTRIM(@abbrev));
    IF LEN(@t) = 0 RETURN N'';
    RETURN UPPER(@t);
END
GO

-- dbo.fn_game_natural_key(@league_id, @home_team_id, @away_team_id, @start_date)
-- Mirror of Python game_natural_key. Always returns a 4-part pipe-delimited
-- string; missing parts appear as empty between pipes.
IF OBJECT_ID(N'dbo.fn_game_natural_key', N'FN') IS NOT NULL DROP FUNCTION dbo.fn_game_natural_key;
GO
CREATE FUNCTION dbo.fn_game_natural_key(
    @league_id     int,
    @home_team_id  bigint,
    @away_team_id  bigint,
    @start_date    date
)
RETURNS nvarchar(80)
WITH SCHEMABINDING
AS
BEGIN
    DECLARE @lid nvarchar(20) = ISNULL(CAST(@league_id AS nvarchar(20)), N'');
    DECLARE @dt  nvarchar(10) =
        CASE WHEN @start_date IS NULL THEN N''
             ELSE
                 CAST(YEAR(@start_date) AS nvarchar(4)) + N'-' +
                 RIGHT(N'00' + CAST(MONTH(@start_date) AS nvarchar(2)), 2) + N'-' +
                 RIGHT(N'00' + CAST(DAY(@start_date) AS nvarchar(2)), 2)
        END;
    DECLARE @h   nvarchar(20) = ISNULL(CAST(@home_team_id AS nvarchar(20)), N'');
    DECLARE @a   nvarchar(20) = ISNULL(CAST(@away_team_id AS nvarchar(20)), N'');
    RETURN @lid + N'|' + @dt + N'|' + @h + N'|' + @a;
END
GO
