"""Sync sportsbook reference tables into unified dimension tables.

This mirrors the `sportsbook_projection_sync.py` approach, but for:
  - sportsbook_sport
  - sportsbook_league
  - sportsbook_team
  - sportsbook_stat_type
  - sportsbook_player
  - sportsbook_game

DDL lives in schema/*.sql; ensure_sportsbook_dimension_tables executes those scripts (splitting on GO).
"""

from __future__ import annotations

import re
from pathlib import Path


_GO_SPLIT_RE = re.compile(r"^\s*GO\s*$", flags=re.IGNORECASE | re.MULTILINE)

SPORTSBOOK_KEYS = ("prizepicks", "underdog", "parlay_play")


def _get_db_conn(
    server: str,
    database: str,
    user: str,
    password: str,
    trusted_connection: bool = False,
):
    import pyodbc

    if trusted_connection:
        conn_str = (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={server};DATABASE={database};Trusted_Connection=yes;"
        )
    else:
        conn_str = (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={server};DATABASE={database};UID={user};PWD={password or ''}"
        )
    try:
        return pyodbc.connect(conn_str)
    except pyodbc.Error:
        # Fallback for older driver names.
        if trusted_connection:
            conn_str = (
                f"DRIVER={{SQL Server}};"
                f"SERVER={server};DATABASE={database};Trusted_Connection=yes;"
            )
        else:
            conn_str = (
                f"DRIVER={{SQL Server}};"
                f"SERVER={server};DATABASE={database};UID={user};PWD={password or ''}"
            )
        return pyodbc.connect(conn_str)


def _sql_batches(sql: str) -> list[str]:
    # `GO` is a client-side batch separator; split and execute per batch.
    parts = [p.strip() for p in _GO_SPLIT_RE.split(sql) if p and p.strip()]
    return parts


def _execute_sql_script(conn, sql: str) -> None:
    cursor = conn.cursor()
    try:
        for batch in _sql_batches(sql):
            cursor.execute(batch)
        conn.commit()
    finally:
        cursor.close()


def ensure_sportsbook_dimension_tables(conn) -> None:
    """Create/alter unified sportsbook dimension tables (idempotent)."""

    root = Path(__file__).resolve().parent
    schema = root / "schema"
    scripts = [
        schema / "sportsbook_sport.sql",
        schema / "sportsbook_league.sql",
        schema / "sportsbook_team.sql",
        schema / "sportsbook_stat_type.sql",
        schema / "sportsbook_player.sql",
        schema / "alter_sportsbook_player_add_missing_columns.sql",
        schema / "sportsbook_player_xref.sql",
        schema / "alter_sportsbook_player_ud_id_to_nvarchar.sql",
        schema / "sportsbook_game.sql",
        schema / "sportsbook_dimension_relations.sql",
    ]
    for p in scripts:
        _execute_sql_script(conn, p.read_text(encoding="utf-8"))


def ensure_sportsbook_dimensions(
    server: str = "localhost\\SQLEXPRESS",
    database: str = "Props",
    user: str = "dbadmin",
    password: str = "",
    trusted_connection: bool = False,
) -> None:
    """Convenience wrapper to ensure tables exist."""
    conn = _get_db_conn(server, database, user, password, trusted_connection)
    try:
        ensure_sportsbook_dimension_tables(conn)
    finally:
        conn.close()


def _table_exists(conn, schema: str, name: str) -> bool:
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            SELECT 1
            FROM sys.tables t
            INNER JOIN sys.schemas s ON s.schema_id = t.schema_id
            WHERE s.name = ? AND t.name = ?
            """,
            (schema, name),
        )
        return cursor.fetchone() is not None
    finally:
        cursor.close()


def _dbo_resolved_object_id(conn, table_name: str) -> int | None:
    """Object id for dbo.{table_name}: user table, view, or synonym target (matches query binding)."""
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            SELECT COALESCE(
                OBJECT_ID(QUOTENAME(N'dbo') + N'.' + QUOTENAME(?), N'U'),
                OBJECT_ID(QUOTENAME(N'dbo') + N'.' + QUOTENAME(?), N'V')
            )
            """,
            (table_name, table_name),
        )
        row = cursor.fetchone()
        oid = int(row[0]) if row and row[0] is not None else None
        if oid:
            return oid
        cursor.execute(
            """
            SELECT s.base_object_id
            FROM sys.synonyms s
            INNER JOIN sys.schemas sch ON sch.schema_id = s.schema_id
            WHERE sch.name = N'dbo' AND s.name = ?
            """,
            (table_name,),
        )
        row2 = cursor.fetchone()
        return int(row2[0]) if row2 and row2[0] is not None else None
    finally:
        cursor.close()


def _dbo_column_lookup(
    conn, table_name: str, *, object_id: int | None = None
) -> dict[str, str]:
    """Map lower-case column name -> actual name from sys.columns (authoritative for the resolved object)."""
    oid = object_id if object_id is not None else _dbo_resolved_object_id(conn, table_name)
    if not oid:
        return {}
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            SELECT c.name
            FROM sys.columns c
            WHERE c.object_id = ?
            ORDER BY c.column_id
            """,
            (oid,),
        )
        return {str(r[0]).lower(): str(r[0]) for r in cursor.fetchall()}
    finally:
        cursor.close()


def _resolve_pk_column(
    conn, table_name: str, *, cols: dict[str, str] | None = None
) -> str:
    """
    Resolve the primary key column name for Parlay Play reference tables.

    Most tables use `id`, but some deployments use `{table_name}_id`.
    """
    c = cols if cols is not None else _dbo_column_lookup(conn, table_name)
    fallback = f"{table_name}_id"
    if "id" in c:
        return c["id"]
    fk = fallback.lower()
    if fk in c:
        return c[fk]
    raise RuntimeError(
        f"Cannot resolve PK column for dbo.{table_name}. Expected 'id' or '{fallback}'."
    )


def _reapply_sportsbook_player_column_alter(conn) -> None:
    """Re-run idempotent ADD COLUMN script on the current database connection."""
    p = Path(__file__).resolve().parent / "schema" / "alter_sportsbook_player_add_missing_columns.sql"
    if p.is_file():
        _execute_sql_script(conn, p.read_text(encoding="utf-8"))


def _parlay_strip_nv(val) -> str | None:
    if val is None:
        return None
    s = str(val).strip()
    return s if s else None


def _parlay_create_stage_temp_table(conn) -> None:
    _merge_exec(
        conn,
        """
            IF OBJECT_ID('tempdb..#pl') IS NOT NULL DROP TABLE #pl;
            CREATE TABLE #pl(
                [parlay_play_player_id] int NOT NULL PRIMARY KEY,
                [display_name] nvarchar(255) NULL,
                [first_name] nvarchar(100) NULL,
                [last_name] nvarchar(100) NULL,
                [position] nvarchar(50) NULL,
                [team_abbrev] nvarchar(20) NULL
            );
            """,
    )


def _parlay_populate_stage_temp_python(conn) -> None:
    """Fill #pl using SELECT * + pyodbc row layout (avoids T-SQL bind quirks on this table)."""
    team_abbr: dict[int, str | None] = {}
    if _table_exists(conn, "dbo", "parlay_play_team"):
        tm_oid = _dbo_resolved_object_id(conn, "parlay_play_team")
        tm = _dbo_column_lookup(conn, "parlay_play_team", object_id=tm_oid)
        team_pk = _resolve_pk_column(conn, "parlay_play_team", cols=tm)
        cur = conn.cursor()
        try:
            cur.execute("SELECT * FROM [dbo].[parlay_play_team]")
            tcols = [str(d[0]) for d in (cur.description or ())]
            tidx = {c.lower(): i for i, c in enumerate(tcols)}
            pk_lc = team_pk.lower()
            if pk_lc not in tidx:
                raise RuntimeError(
                    f"PK column {team_pk!r} not in ODBC result for dbo.parlay_play_team; columns={tcols!r}"
                )
            pk_i = tidx[pk_lc]
            abbr_i = None
            for key in ("team_abbreviation", "teamname_abbr"):
                if key in tidx:
                    abbr_i = tidx[key]
                    break
            for row in cur.fetchall():
                tid = row[pk_i]
                if tid is None:
                    continue
                ab = _parlay_strip_nv(row[abbr_i]) if abbr_i is not None else None
                team_abbr[int(tid)] = ab
        finally:
            cur.close()

    pl = _dbo_column_lookup(conn, "parlay_play_player", object_id=_dbo_resolved_object_id(conn, "parlay_play_player"))
    player_pk = _resolve_pk_column(conn, "parlay_play_player", cols=pl)
    pk_lc = player_pk.lower()

    cur = conn.cursor()
    try:
        cur.execute("SELECT * FROM [dbo].[parlay_play_player]")
        pcols = [str(d[0]) for d in (cur.description or ())]
        pci = {c.lower(): i for i, c in enumerate(pcols)}
        if pk_lc not in pci:
            raise RuntimeError(
                f"PK column {player_pk!r} not in ODBC result for dbo.parlay_play_player; columns={pcols!r}"
            )

        def pick(row: tuple, *names: str):
            for n in names:
                i = pci.get(n.lower())
                if i is not None:
                    return row[i]
            return None

        by_pid: dict[int, tuple[int, str, str | None, str | None, str | None, str | None]] = {}
        for row in cur.fetchall():
            pid_v = row[pci[pk_lc]]
            if pid_v is None:
                continue
            pid = int(pid_v)
            fn = _parlay_strip_nv(pick(row, "first_name"))
            ln = _parlay_strip_nv(pick(row, "last_name"))
            full = _parlay_strip_nv(pick(row, "full_name", "name"))
            pos = _parlay_strip_nv(pick(row, "position"))
            team_id_v = pick(row, "team_id")
            ta = None
            if team_id_v is not None:
                try:
                    ta = team_abbr.get(int(team_id_v))
                except (TypeError, ValueError):
                    ta = None
            display = full
            if not display and fn and ln:
                display = (f"{fn} {ln}").strip() or None
            if not display and fn:
                display = fn
            if not display and ln:
                display = ln
            if not display:
                display = f"Player {pid}"
            by_pid[pid] = (pid, display, fn, ln, pos, ta)
        out = list(by_pid.values())
    finally:
        cur.close()

    if not out:
        return
    ins = conn.cursor()
    try:
        ins.fast_executemany = True
        ins.executemany(
            """
            INSERT INTO #pl (
                [parlay_play_player_id], [display_name], [first_name], [last_name], [position], [team_abbrev]
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            out,
        )
        conn.commit()
    finally:
        ins.close()


def _parlay_play_player_sql_update_batches(conn, *, now_sql: str) -> list[str]:
    """Batches that join #pl to dbo.sportsbook_player (after #pl is filled)."""
    b_update_keyed = f"""
            UPDATE t
            SET
                t.[display_name] = COALESCE(s.[display_name], t.[display_name]),
                t.[first_name] = COALESCE(s.[first_name], t.[first_name]),
                t.[last_name] = COALESCE(s.[last_name], t.[last_name]),
                t.[position] = COALESCE(s.[position], t.[position]),
                t.[team_abbrev] = COALESCE(t.[team_abbrev], s.[team_abbrev]),
                t.[last_modified_at] = {now_sql}
            FROM [dbo].[sportsbook_player] AS t
            INNER JOIN #pl AS s ON t.[parlay_play_player_id] = s.[parlay_play_player_id];
            """

    b_update_cand = f"""
            ;WITH cand AS (
                SELECT
                    t.[sportsbook_player_id],
                    s.[parlay_play_player_id],
                    COUNT(*) OVER (PARTITION BY s.[parlay_play_player_id]) AS c1
                FROM #pl AS s
                INNER JOIN [dbo].[sportsbook_player] AS t
                    ON t.[parlay_play_player_id] IS NULL
                   AND t.[display_name] = COALESCE(s.[display_name], N'Player ' + CAST(s.[parlay_play_player_id] AS nvarchar(20)))
                   AND (
                        t.[team_abbrev] = s.[team_abbrev]
                     OR s.[team_abbrev] IS NULL
                     OR t.[team_abbrev] IS NULL
                   )
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM [dbo].[sportsbook_player] AS t2
                    WHERE t2.[parlay_play_player_id] = s.[parlay_play_player_id]
                )
            )
            UPDATE t
            SET
                t.[parlay_play_player_id] = c.[parlay_play_player_id],
                t.[first_name] = COALESCE(t.[first_name], pl.[first_name]),
                t.[last_name] = COALESCE(t.[last_name], pl.[last_name]),
                t.[position] = COALESCE(t.[position], pl.[position]),
                t.[team_abbrev] = COALESCE(t.[team_abbrev], pl.[team_abbrev]),
                t.[last_modified_at] = {now_sql}
            FROM [dbo].[sportsbook_player] AS t
            INNER JOIN cand AS c ON c.[sportsbook_player_id] = t.[sportsbook_player_id]
            INNER JOIN #pl AS pl ON pl.[parlay_play_player_id] = c.[parlay_play_player_id]
            WHERE c.c1 = 1;
            """

    b_insert = f"""
            INSERT INTO [dbo].[sportsbook_player](
                [display_name], [first_name], [last_name], [position], [team_abbrev], [parlay_play_player_id], [created_at], [last_modified_at]
            )
            SELECT
                COALESCE(s.[display_name], N'Player ' + CAST(s.[parlay_play_player_id] AS nvarchar(20))),
                s.[first_name],
                s.[last_name],
                s.[position],
                s.[team_abbrev],
                s.[parlay_play_player_id],
                {now_sql}, {now_sql}
            FROM #pl AS s
            WHERE NOT EXISTS (
                SELECT 1 FROM [dbo].[sportsbook_player] AS t WHERE t.[parlay_play_player_id] = s.[parlay_play_player_id]
            );
            """

    return [b_update_keyed, b_update_cand, b_insert]


def _now_central_sql() -> str:
    return "CONVERT(datetime2(7), SYSUTCDATETIME() AT TIME ZONE 'UTC' AT TIME ZONE 'Central Standard Time')"


def _merge_exec(conn, sql: str) -> int:
    """Execute a MERGE/UPDATE/INSERT statement and return cursor.rowcount."""
    cursor = conn.cursor()
    try:
        cursor.execute(sql)
        rc = cursor.rowcount
        conn.commit()
        return int(rc) if rc is not None else 0
    finally:
        cursor.close()


def _stage_rows(conn, temp_table_sql: str, insert_sql: str, rows: list[tuple]) -> None:
    cursor = conn.cursor()
    try:
        cursor.execute(temp_table_sql)
        if rows:
            cursor.fast_executemany = True
            cursor.executemany(insert_sql, rows)
        conn.commit()
    finally:
        cursor.close()


def _consolidate_sportsbook_player_dupes(conn) -> None:
    """Merge rows that share normalized display_name + team_abbrev + jersey_number into one survivor row.

    Rows with NULL or blank display_name use a per-row surrogate key (private-use char + id) so they are
    never grouped together on name alone.
    """
    if not _table_exists(conn, "dbo", "sportsbook_player"):
        return
    now = _now_central_sql()
    _merge_exec(
        conn,
        f"""
        IF OBJECT_ID('tempdb..#dgrp') IS NOT NULL DROP TABLE #dgrp;
        ;WITH keyed AS (
            SELECT
                p.sportsbook_player_id,
                COALESCE(
                    NULLIF(LOWER(LTRIM(RTRIM(p.display_name))), N''),
                    NCHAR(0xE000) + CAST(p.sportsbook_player_id AS nvarchar(30))
                ) AS nk,
                LTRIM(RTRIM(COALESCE(p.team_abbrev, N''))) AS ta,
                LTRIM(RTRIM(COALESCE(p.jersey_number, N''))) AS jn
            FROM [dbo].[sportsbook_player] p
        ),
        agg AS (
            SELECT k.nk, k.ta, k.jn, MIN(k.sportsbook_player_id) AS survivor_id, COUNT(*) AS cnt
            FROM keyed k
            GROUP BY k.nk, k.ta, k.jn
            HAVING COUNT(*) > 1
        )
        SELECT k.sportsbook_player_id, a.survivor_id
        INTO #dgrp
        FROM keyed k
        INNER JOIN agg a
            ON a.nk = k.nk AND a.ta = k.ta AND a.jn = k.jn
        WHERE k.sportsbook_player_id <> a.survivor_id;

        IF EXISTS (SELECT 1 FROM #dgrp)
        BEGIN
            IF OBJECT_ID(N'dbo.sportsbook_player_xref', N'U') IS NOT NULL
            BEGIN
                UPDATE x
                SET x.sportsbook_player_id = d.survivor_id,
                    x.last_modified_at = {now}
                FROM [dbo].[sportsbook_player_xref] x
                INNER JOIN #dgrp d ON d.sportsbook_player_id = x.sportsbook_player_id;
            END

            ;WITH fam AS (
                SELECT m.survivor_id, p.*
                FROM [dbo].[sportsbook_player] p
                INNER JOIN (
                    SELECT survivor_id, sportsbook_player_id FROM #dgrp
                    UNION ALL
                    SELECT d.survivor_id, d.survivor_id FROM #dgrp d
                ) m ON p.sportsbook_player_id = m.sportsbook_player_id
            ),
            agg2 AS (
                SELECT
                    survivor_id,
                    MAX(canonical_league_id) AS canonical_league_id,
                    MAX(sportsbook_sport_id) AS sportsbook_sport_id,
                    MAX(sportsbook_league_id) AS sportsbook_league_id,
                    MAX(sportsbook_team_id) AS sportsbook_team_id,
                    MAX(first_name) AS first_name,
                    MAX(last_name) AS last_name,
                    MAX(position) AS position,
                    MAX(team_name) AS team_name,
                    MIN(prizepicks_player_id) AS prizepicks_player_id,
                    MIN(underdog_player_id) AS underdog_player_id,
                    MIN(parlay_play_player_id) AS parlay_play_player_id
                FROM fam
                GROUP BY survivor_id
            )
            UPDATE s
            SET
                s.canonical_league_id = COALESCE(s.canonical_league_id, a.canonical_league_id),
                s.sportsbook_sport_id = COALESCE(s.sportsbook_sport_id, a.sportsbook_sport_id),
                s.sportsbook_league_id = COALESCE(s.sportsbook_league_id, a.sportsbook_league_id),
                s.sportsbook_team_id = COALESCE(s.sportsbook_team_id, a.sportsbook_team_id),
                s.first_name = COALESCE(s.first_name, a.first_name),
                s.last_name = COALESCE(s.last_name, a.last_name),
                s.position = COALESCE(s.position, a.position),
                s.team_name = COALESCE(s.team_name, a.team_name),
                s.prizepicks_player_id = COALESCE(s.prizepicks_player_id, a.prizepicks_player_id),
                s.underdog_player_id = COALESCE(s.underdog_player_id, a.underdog_player_id),
                s.parlay_play_player_id = COALESCE(s.parlay_play_player_id, a.parlay_play_player_id),
                s.last_modified_at = {now}
            FROM [dbo].[sportsbook_player] s
            INNER JOIN agg2 a ON a.survivor_id = s.sportsbook_player_id;

            IF OBJECT_ID(N'dbo.sportsbook_player_xref', N'U') IS NOT NULL
            BEGIN
                INSERT INTO [dbo].[sportsbook_player_xref](sportsbook, external_player_id, sportsbook_player_id, created_at, last_modified_at)
                SELECT
                    N'prizepicks',
                    N'player_id:' + p.prizepicks_player_id,
                    d.survivor_id,
                    {now}, {now}
                FROM [dbo].[sportsbook_player] p
                INNER JOIN #dgrp d ON d.sportsbook_player_id = p.sportsbook_player_id
                WHERE p.prizepicks_player_id IS NOT NULL
                  AND NOT EXISTS (
                      SELECT 1
                      FROM [dbo].[sportsbook_player] s0
                      WHERE s0.sportsbook_player_id = d.survivor_id
                        AND s0.prizepicks_player_id = p.prizepicks_player_id
                  )
                  AND NOT EXISTS (
                      SELECT 1
                      FROM [dbo].[sportsbook_player_xref] x
                      WHERE x.sportsbook = N'prizepicks'
                        AND x.external_player_id = N'player_id:' + p.prizepicks_player_id
                        AND x.sportsbook_player_id = d.survivor_id
                  );
            END

            DELETE p
            FROM [dbo].[sportsbook_player] p
            INNER JOIN #dgrp d ON d.sportsbook_player_id = p.sportsbook_player_id;
        END
        """,
    )


def _sync_sport(conn) -> None:
    now = _now_central_sql()

    if _table_exists(conn, "dbo", "parlay_play_sport"):
        _merge_exec(
            conn,
            f"""
            MERGE [dbo].[sportsbook_sport] AS t
            USING (
                SELECT
                    CAST(s.[id] AS int) AS parlay_play_sport_id,
                    NULLIF(LTRIM(RTRIM(s.[sport_name])), N'') AS display_name
                FROM [dbo].[parlay_play_sport] s
            ) AS s
                ON t.parlay_play_sport_id = s.parlay_play_sport_id
            WHEN MATCHED THEN
                UPDATE SET
                    t.display_name = COALESCE(s.display_name, t.display_name),
                    t.last_modified_at = {now}
            WHEN NOT MATCHED BY TARGET THEN
                INSERT (parlay_play_sport_id, display_name, created_at, last_modified_at)
                VALUES (s.parlay_play_sport_id, s.display_name, {now}, {now});
            """,
        )


def _sync_league(conn) -> None:
    now = _now_central_sql()

    if _table_exists(conn, "dbo", "prizepicks_league"):
        _merge_exec(
            conn,
            f"""
            MERGE [dbo].[sportsbook_league] AS t
            USING (
                SELECT
                    TRY_CAST(l.[league_id] AS int) AS canonical_league_id,
                    CAST(l.[league_id] AS nvarchar(20)) AS prizepicks_league_id,
                    NULLIF(LTRIM(RTRIM(l.[name])), N'') AS display_name
                FROM [dbo].[prizepicks_league] l
            ) AS s
                ON t.prizepicks_league_id = s.prizepicks_league_id
                OR (
                    t.prizepicks_league_id IS NULL
                    AND s.canonical_league_id IS NOT NULL
                    AND t.canonical_league_id = s.canonical_league_id
                )
            WHEN MATCHED THEN
                UPDATE SET
                    t.canonical_league_id = COALESCE(s.canonical_league_id, t.canonical_league_id),
                    t.display_name = COALESCE(s.display_name, t.display_name),
                    t.prizepicks_league_id = COALESCE(t.prizepicks_league_id, s.prizepicks_league_id),
                    t.last_modified_at = {now}
            WHEN NOT MATCHED BY TARGET THEN
                INSERT (canonical_league_id, prizepicks_league_id, display_name, created_at, last_modified_at)
                VALUES (s.canonical_league_id, s.prizepicks_league_id, s.display_name, {now}, {now});
            """,
        )

    if _table_exists(conn, "dbo", "parlay_play_league"):
        # Parlay leagues are linked by explicit map/CASE to canonical_league_id during sync.
        from cross_book_stat_normalize import PARLAY_MATCH_LEAGUE_ID_TO_PRIZEPICKS

        rows: list[tuple] = []
        cur = conn.cursor()
        try:
            cur.execute(
                """
                SELECT id, sport_id, league_name, league_name_short
                FROM [dbo].[parlay_play_league]
                """
            )
            for pid, sport_id, ln, lns in cur.fetchall():
                canonical = PARLAY_MATCH_LEAGUE_ID_TO_PRIZEPICKS.get(int(pid))
                display = (ln or lns or "").strip() or None
                rows.append((int(pid), canonical, int(sport_id) if sport_id is not None else None, display))
        finally:
            cur.close()

        temp_sql = """
        IF OBJECT_ID('tempdb..#pl') IS NOT NULL DROP TABLE #pl;
        CREATE TABLE #pl(
            parlay_play_league_id int NOT NULL,
            canonical_league_id int NULL,
            parlay_play_sport_id int NULL,
            display_name nvarchar(150) NULL
        );
        """
        _stage_rows(
            conn,
            temp_sql,
            "INSERT INTO #pl (parlay_play_league_id, canonical_league_id, parlay_play_sport_id, display_name) VALUES (?,?,?,?)",
            rows,
        )
        _merge_exec(
            conn,
            f"""
            MERGE [dbo].[sportsbook_league] AS t
            USING #pl AS s
                ON t.parlay_play_league_id = s.parlay_play_league_id
                OR (
                    t.parlay_play_league_id IS NULL
                    AND s.canonical_league_id IS NOT NULL
                    AND t.canonical_league_id = s.canonical_league_id
                )
            WHEN MATCHED THEN
                UPDATE SET
                    t.canonical_league_id = COALESCE(s.canonical_league_id, t.canonical_league_id),
                    t.display_name = COALESCE(s.display_name, t.display_name),
                    t.parlay_play_league_id = COALESCE(t.parlay_play_league_id, s.parlay_play_league_id),
                    t.last_modified_at = {now}
            WHEN NOT MATCHED BY TARGET THEN
                INSERT (canonical_league_id, parlay_play_league_id, display_name, created_at, last_modified_at)
                VALUES (s.canonical_league_id, s.parlay_play_league_id, s.display_name, {now}, {now});
            """,
        )

        # Link sportsbook_league.sportsbook_sport_id from parlay sport when possible (no name heuristic).
        _merge_exec(
            conn,
            f"""
            UPDATE sl
            SET sl.sportsbook_sport_id = ss.sportsbook_sport_id,
                sl.last_modified_at = {now}
            FROM [dbo].[sportsbook_league] sl
            INNER JOIN [dbo].[parlay_play_league] pl
                ON pl.id = sl.parlay_play_league_id
            INNER JOIN [dbo].[sportsbook_sport] ss
                ON ss.parlay_play_sport_id = pl.sport_id
            WHERE sl.sportsbook_sport_id IS NULL
              AND sl.parlay_play_league_id IS NOT NULL
              AND ss.sportsbook_sport_id IS NOT NULL;
            """,
        )

    if _table_exists(conn, "dbo", "underdog_game"):
        # Underdog treats sport_id as a league axis in current projection pipeline.
        _merge_exec(
            conn,
            f"""
            MERGE [dbo].[sportsbook_league] AS t
            USING (
                SELECT DISTINCT
                    TRY_CAST(g.[sport_id] AS int) AS underdog_sport_id
                FROM [dbo].[underdog_game] g
                WHERE g.[sport_id] IS NOT NULL
            ) AS s
                ON t.underdog_sport_id = s.underdog_sport_id
            WHEN MATCHED THEN
                UPDATE SET t.last_modified_at = {now}
            WHEN NOT MATCHED BY TARGET THEN
                INSERT (underdog_sport_id, created_at, last_modified_at)
                VALUES (s.underdog_sport_id, {now}, {now});
            """,
        )


def _sync_team(conn) -> None:
    now = _now_central_sql()

    if _table_exists(conn, "dbo", "prizepicks_team"):
        _merge_exec(
            conn,
            f"""
            MERGE [dbo].[sportsbook_team] AS t
            USING (
                SELECT
                    CAST(tm.[team_id] AS nvarchar(20)) AS prizepicks_team_id,
                    NULLIF(LTRIM(RTRIM(tm.[abbreviation])), N'') AS abbreviation,
                    NULLIF(LTRIM(RTRIM(CONCAT(tm.[market], CASE WHEN tm.[market] IS NOT NULL AND tm.[name] IS NOT NULL THEN N' ' ELSE N'' END, tm.[name]))), N'') AS full_name
                FROM [dbo].[prizepicks_team] tm
            ) AS s
                ON t.prizepicks_team_id = s.prizepicks_team_id
            WHEN MATCHED THEN
                UPDATE SET
                    t.abbreviation = COALESCE(s.abbreviation, t.abbreviation),
                    t.full_name = COALESCE(s.full_name, t.full_name),
                    t.last_modified_at = {now}
            WHEN NOT MATCHED BY TARGET THEN
                INSERT (prizepicks_team_id, abbreviation, full_name, created_at, last_modified_at)
                VALUES (s.prizepicks_team_id, s.abbreviation, s.full_name, {now}, {now});
            """,
        )

    if _table_exists(conn, "dbo", "parlay_play_team"):
        _merge_exec(
            conn,
            f"""
            MERGE [dbo].[sportsbook_team] AS t
            USING (
                SELECT
                    CAST(pt.[id] AS int) AS parlay_play_team_id,
                    NULLIF(LTRIM(RTRIM(COALESCE(pt.[team_abbreviation], pt.[teamname_abbr]))), N'') AS abbreviation,
                    NULLIF(LTRIM(RTRIM(pt.[teamname])), N'') AS full_name
                FROM [dbo].[parlay_play_team] pt
            ) AS s
                ON t.parlay_play_team_id = s.parlay_play_team_id
            WHEN MATCHED THEN
                UPDATE SET
                    t.abbreviation = COALESCE(s.abbreviation, t.abbreviation),
                    t.full_name = COALESCE(s.full_name, t.full_name),
                    t.last_modified_at = {now}
            WHEN NOT MATCHED BY TARGET THEN
                INSERT (parlay_play_team_id, abbreviation, full_name, created_at, last_modified_at)
                VALUES (s.parlay_play_team_id, s.abbreviation, s.full_name, {now}, {now});
            """,
        )

    if _table_exists(conn, "dbo", "underdog_appearance"):
        # Underdog does not have a stable team dimension table in this repo; keep the ID for joins.
        _merge_exec(
            conn,
            f"""
            MERGE [dbo].[sportsbook_team] AS t
            USING (
                SELECT DISTINCT
                    CAST(a.[team_id] AS nvarchar(50)) AS underdog_team_id
                FROM [dbo].[underdog_appearance] a
                WHERE a.[team_id] IS NOT NULL
            ) AS s
                ON t.underdog_team_id = s.underdog_team_id
            WHEN MATCHED THEN
                UPDATE SET t.last_modified_at = {now}
            WHEN NOT MATCHED BY TARGET THEN
                INSERT (underdog_team_id, created_at, last_modified_at)
                VALUES (s.underdog_team_id, {now}, {now});
            """,
        )


def _sync_stat_type(conn) -> None:
    now = _now_central_sql()
    from cross_book_stat_normalize import normalize_for_join

    rows: list[tuple] = []

    if _table_exists(conn, "dbo", "prizepicks_stat_type"):
        cur = conn.cursor()
        try:
            cur.execute("SELECT stat_type_id, name FROM [dbo].[prizepicks_stat_type]")
            for sid, name in cur.fetchall():
                disp = (name or "").strip() or None
                nkey = normalize_for_join(disp)
                rows.append((None, nkey or None, disp, str(sid), None, None))
        finally:
            cur.close()

    if _table_exists(conn, "dbo", "underdog_stat_type"):
        cur = conn.cursor()
        try:
            cur.execute("SELECT pickem_stat_id, stat_type_name, display_stat FROM [dbo].[underdog_stat_type]")
            for pid, stn, ds in cur.fetchall():
                disp = (ds or stn or "").strip() or None
                nkey = normalize_for_join(disp)
                rows.append((None, nkey or None, disp, None, str(pid), None))
        finally:
            cur.close()

    if _table_exists(conn, "dbo", "parlay_play_stat_type"):
        cur = conn.cursor()
        try:
            cur.execute(
                "SELECT challenge_option, challenge_name FROM [dbo].[parlay_play_stat_type]"
            )
            for opt, nm in cur.fetchall():
                disp = (nm or opt or "").strip() or None
                nkey = normalize_for_join(disp or opt)
                rows.append((None, nkey or None, disp, None, None, str(opt)))
        finally:
            cur.close()

    temp_sql = """
    IF OBJECT_ID('tempdb..#st') IS NOT NULL DROP TABLE #st;
    CREATE TABLE #st(
        canonical_league_id int NULL,
        normalized_stat_key nvarchar(120) NULL,
        stat_display_name nvarchar(200) NULL,
        prizepicks_stat_type_id nvarchar(20) NULL,
        underdog_pickem_stat_id nvarchar(36) NULL,
        parlay_play_challenge_option nvarchar(50) NULL
    );
    """
    _stage_rows(
        conn,
        temp_sql,
        "INSERT INTO #st (canonical_league_id, normalized_stat_key, stat_display_name, prizepicks_stat_type_id, underdog_pickem_stat_id, parlay_play_challenge_option) VALUES (?,?,?,?,?,?)",
        rows,
    )

    # First pass: update then insert by native ids (exact), avoiding MERGE edge-cases.
    _merge_exec(
        conn,
        f"""
        ;WITH s AS (
            SELECT
                prizepicks_stat_type_id,
                MAX(canonical_league_id) AS canonical_league_id,
                MAX(normalized_stat_key) AS normalized_stat_key,
                MAX(stat_display_name) AS stat_display_name
            FROM #st
            WHERE prizepicks_stat_type_id IS NOT NULL
            GROUP BY prizepicks_stat_type_id
        )
        UPDATE t
        SET t.normalized_stat_key = COALESCE(s.normalized_stat_key, t.normalized_stat_key),
            t.stat_display_name = COALESCE(s.stat_display_name, t.stat_display_name),
            t.last_modified_at = {now}
        FROM [dbo].[sportsbook_stat_type] t
        INNER JOIN s ON s.prizepicks_stat_type_id = t.prizepicks_stat_type_id;

        ;WITH s AS (
            SELECT
                prizepicks_stat_type_id,
                MAX(canonical_league_id) AS canonical_league_id,
                MAX(normalized_stat_key) AS normalized_stat_key,
                MAX(stat_display_name) AS stat_display_name
            FROM #st
            WHERE prizepicks_stat_type_id IS NOT NULL
            GROUP BY prizepicks_stat_type_id
        )
        INSERT INTO [dbo].[sportsbook_stat_type](
            canonical_league_id, normalized_stat_key, stat_display_name, prizepicks_stat_type_id,
            created_at, last_modified_at
        )
        SELECT
            s.canonical_league_id, s.normalized_stat_key, s.stat_display_name, s.prizepicks_stat_type_id,
            {now}, {now}
        FROM s
        WHERE NOT EXISTS (
            SELECT 1 FROM [dbo].[sportsbook_stat_type] t
            WHERE t.prizepicks_stat_type_id = s.prizepicks_stat_type_id
        );
        """,
    )

    _merge_exec(
        conn,
        f"""
        ;WITH s AS (
            SELECT
                underdog_pickem_stat_id,
                MAX(canonical_league_id) AS canonical_league_id,
                MAX(normalized_stat_key) AS normalized_stat_key,
                MAX(stat_display_name) AS stat_display_name
            FROM #st
            WHERE underdog_pickem_stat_id IS NOT NULL
            GROUP BY underdog_pickem_stat_id
        )
        UPDATE t
        SET t.normalized_stat_key = COALESCE(s.normalized_stat_key, t.normalized_stat_key),
            t.stat_display_name = COALESCE(s.stat_display_name, t.stat_display_name),
            t.last_modified_at = {now}
        FROM [dbo].[sportsbook_stat_type] t
        INNER JOIN s ON s.underdog_pickem_stat_id = t.underdog_pickem_stat_id;

        ;WITH s AS (
            SELECT
                underdog_pickem_stat_id,
                MAX(canonical_league_id) AS canonical_league_id,
                MAX(normalized_stat_key) AS normalized_stat_key,
                MAX(stat_display_name) AS stat_display_name
            FROM #st
            WHERE underdog_pickem_stat_id IS NOT NULL
            GROUP BY underdog_pickem_stat_id
        )
        INSERT INTO [dbo].[sportsbook_stat_type](
            canonical_league_id, normalized_stat_key, stat_display_name, underdog_pickem_stat_id,
            created_at, last_modified_at
        )
        SELECT
            s.canonical_league_id, s.normalized_stat_key, s.stat_display_name, s.underdog_pickem_stat_id,
            {now}, {now}
        FROM s
        WHERE NOT EXISTS (
            SELECT 1 FROM [dbo].[sportsbook_stat_type] t
            WHERE t.underdog_pickem_stat_id = s.underdog_pickem_stat_id
        );
        """,
    )

    _merge_exec(
        conn,
        f"""
        ;WITH s AS (
            SELECT
                parlay_play_challenge_option,
                MAX(canonical_league_id) AS canonical_league_id,
                MAX(normalized_stat_key) AS normalized_stat_key,
                MAX(stat_display_name) AS stat_display_name
            FROM #st
            WHERE parlay_play_challenge_option IS NOT NULL
            GROUP BY parlay_play_challenge_option
        )
        UPDATE t
        SET t.normalized_stat_key = COALESCE(s.normalized_stat_key, t.normalized_stat_key),
            t.stat_display_name = COALESCE(s.stat_display_name, t.stat_display_name),
            t.last_modified_at = {now}
        FROM [dbo].[sportsbook_stat_type] t
        INNER JOIN s ON s.parlay_play_challenge_option = t.parlay_play_challenge_option;

        ;WITH s AS (
            SELECT
                parlay_play_challenge_option,
                MAX(canonical_league_id) AS canonical_league_id,
                MAX(normalized_stat_key) AS normalized_stat_key,
                MAX(stat_display_name) AS stat_display_name
            FROM #st
            WHERE parlay_play_challenge_option IS NOT NULL
            GROUP BY parlay_play_challenge_option
        )
        INSERT INTO [dbo].[sportsbook_stat_type](
            canonical_league_id, normalized_stat_key, stat_display_name, parlay_play_challenge_option,
            created_at, last_modified_at
        )
        SELECT
            s.canonical_league_id, s.normalized_stat_key, s.stat_display_name, s.parlay_play_challenge_option,
            {now}, {now}
        FROM s
        WHERE NOT EXISTS (
            SELECT 1 FROM [dbo].[sportsbook_stat_type] t
            WHERE t.parlay_play_challenge_option = s.parlay_play_challenge_option
        );
        """,
    )

    # Second pass: attach missing native ids by normalized_stat_key when unambiguous.
    _merge_exec(
        conn,
        f"""
        ;WITH cand AS (
            SELECT
                t.sportsbook_stat_type_id,
                s.prizepicks_stat_type_id,
                COUNT(*) OVER (PARTITION BY s.prizepicks_stat_type_id) AS c1
            FROM #st s
            INNER JOIN [dbo].[sportsbook_stat_type] t
                ON t.normalized_stat_key IS NOT NULL
               AND s.normalized_stat_key IS NOT NULL
               AND t.normalized_stat_key = s.normalized_stat_key
            WHERE s.prizepicks_stat_type_id IS NOT NULL
              AND t.prizepicks_stat_type_id IS NULL
              AND NOT EXISTS (
                  SELECT 1
                  FROM [dbo].[sportsbook_stat_type] t2
                  WHERE t2.prizepicks_stat_type_id = s.prizepicks_stat_type_id
              )
        )
        UPDATE t
        SET t.prizepicks_stat_type_id = c.prizepicks_stat_type_id,
            t.last_modified_at = {now}
        FROM [dbo].[sportsbook_stat_type] t
        INNER JOIN cand c
            ON c.sportsbook_stat_type_id = t.sportsbook_stat_type_id
        WHERE c.c1 = 1;
        """,
    )


def _sync_player(conn) -> None:
    now = _now_central_sql()

    _consolidate_sportsbook_player_dupes(conn)

    if _table_exists(conn, "dbo", "prizepicks_player"):
        # Stage PP players and map to a canonical sportsbook_player row by (display_name, team_abbrev) when possible.
        # Then store the PP id in sportsbook_player_xref to avoid multiplying canonical rows.
        _merge_exec(
            conn,
            f"""
            IF OBJECT_ID('tempdb..#pp') IS NOT NULL DROP TABLE #pp;
            CREATE TABLE #pp(
                prizepicks_player_id nvarchar(20) NOT NULL,
                prizepicks_ppid nvarchar(200) NULL,
                display_name nvarchar(255) NULL,
                jersey_number nvarchar(20) NULL,
                position nvarchar(50) NULL,
                team_abbrev nvarchar(20) NULL,
                team_name nvarchar(150) NULL,
                canonical_league_id int NULL
            );

            INSERT INTO #pp(prizepicks_player_id, prizepicks_ppid, display_name, jersey_number, position, team_abbrev, team_name, canonical_league_id)
            SELECT
                CAST(pp.[player_id] AS nvarchar(20)) AS prizepicks_player_id,
                NULLIF(LTRIM(RTRIM(pp.[ppid])), N'') AS prizepicks_ppid,
                NULLIF(LTRIM(RTRIM(pp.[display_name])), N'') AS display_name,
                NULLIF(LTRIM(RTRIM(pp.[jersey_number])), N'') AS jersey_number,
                NULLIF(LTRIM(RTRIM(pp.[position])), N'') AS position,
                NULLIF(LTRIM(RTRIM(pp.[team])), N'') AS team_abbrev,
                NULLIF(LTRIM(RTRIM(pp.[team_name])), N'') AS team_name,
                TRY_CAST(pp.[league_id] AS int) AS canonical_league_id
            FROM [dbo].[prizepicks_player] pp
            WHERE pp.[combo] = 0;

            -- Backfill xref for existing deployments that previously stored unprefixed external ids
            -- or populated sportsbook_player.prizepicks_player_id directly.
            IF OBJECT_ID(N'dbo.sportsbook_player_xref', N'U') IS NOT NULL
            BEGIN
                -- 1) Ensure every existing sportsbook_player.prizepicks_player_id has an xref row.
                INSERT INTO [dbo].[sportsbook_player_xref](sportsbook, external_player_id, sportsbook_player_id, created_at, last_modified_at)
                SELECT
                    N'prizepicks',
                    N'player_id:' + CAST(p.prizepicks_player_id AS nvarchar(64)),
                    p.sportsbook_player_id,
                    {now}, {now}
                FROM [dbo].[sportsbook_player] p
                WHERE p.prizepicks_player_id IS NOT NULL
                  AND NOT EXISTS (
                      SELECT 1 FROM [dbo].[sportsbook_player_xref] x
                      WHERE x.sportsbook = N'prizepicks'
                        AND (
                             x.external_player_id = N'player_id:' + CAST(p.prizepicks_player_id AS nvarchar(64))
                          OR x.external_player_id = CAST(p.prizepicks_player_id AS nvarchar(64))
                        )
                  );

                -- 2) Convert any legacy unprefixed xref rows to prefixed form by inserting prefixed alongside.
                INSERT INTO [dbo].[sportsbook_player_xref](sportsbook, external_player_id, sportsbook_player_id, created_at, last_modified_at)
                SELECT
                    x.sportsbook,
                    N'player_id:' + x.external_player_id,
                    x.sportsbook_player_id,
                    {now}, {now}
                FROM [dbo].[sportsbook_player_xref] x
                WHERE x.sportsbook = N'prizepicks'
                  AND x.external_player_id NOT LIKE N'%:%'
                  AND NOT EXISTS (
                      SELECT 1 FROM [dbo].[sportsbook_player_xref] x2
                      WHERE x2.sportsbook = x.sportsbook
                        AND x2.external_player_id = N'player_id:' + x.external_player_id
                  );

                -- 3) Backfill ppid-based xref using current prizepicks_player table.
                INSERT INTO [dbo].[sportsbook_player_xref](sportsbook, external_player_id, sportsbook_player_id, created_at, last_modified_at)
                SELECT
                    N'prizepicks',
                    N'ppid:' + psrc.prizepicks_ppid,
                    x.sportsbook_player_id,
                    {now}, {now}
                FROM #pp psrc
                INNER JOIN [dbo].[sportsbook_player_xref] x
                    ON x.sportsbook = N'prizepicks'
                   AND (
                        x.external_player_id = N'player_id:' + psrc.prizepicks_player_id
                     OR x.external_player_id = psrc.prizepicks_player_id
                   )
                WHERE psrc.prizepicks_ppid IS NOT NULL
                  AND NOT EXISTS (
                      SELECT 1 FROM [dbo].[sportsbook_player_xref] x2
                      WHERE x2.sportsbook = N'prizepicks'
                        AND x2.external_player_id = N'ppid:' + psrc.prizepicks_ppid
                  );
            END

            -- 1) Create canonical players that don't exist.
            -- Prefer stable PrizePicks ppid to avoid duplicates for quarter/half league variants.
            -- Fallback to (display_name, team_abbrev) when ppid is missing.
            INSERT INTO [dbo].[sportsbook_player](
                canonical_league_id, display_name, jersey_number, position, team_abbrev, team_name,
                created_at, last_modified_at
            )
            SELECT
                p.canonical_league_id,
                COALESCE(p.display_name, N'Player ' + p.prizepicks_player_id),
                p.jersey_number,
                p.position,
                p.team_abbrev,
                p.team_name,
                {now}, {now}
            FROM #pp p
            WHERE NOT EXISTS (
                SELECT 1
                FROM [dbo].[sportsbook_player] t
                WHERE t.display_name = COALESCE(p.display_name, N'Player ' + p.prizepicks_player_id)
                  AND (
                        (t.team_abbrev = p.team_abbrev)
                     OR (t.team_abbrev IS NULL AND p.team_abbrev IS NULL)
                  )
                  AND (
                        t.jersey_number IS NULL
                     OR p.jersey_number IS NULL
                     OR t.jersey_number = p.jersey_number
                  )
            );

            -- 2) Resolve each PP row -> canonical sportsbook_player_id.
            IF OBJECT_ID('tempdb..#pp_map') IS NOT NULL DROP TABLE #pp_map;
            CREATE TABLE #pp_map(
                prizepicks_player_id nvarchar(20) NOT NULL,
                prizepicks_ppid nvarchar(200) NULL,
                sportsbook_player_id bigint NOT NULL,
                merge_key nvarchar(300) NOT NULL
            );

            -- Prefer ppid: link all PP ids sharing same ppid to one canonical row (existing xref wins),
            -- else fall back to lowest sportsbook_player_id by (display_name, team_abbrev).
            INSERT INTO #pp_map(prizepicks_player_id, prizepicks_ppid, sportsbook_player_id, merge_key)
            SELECT
                p.prizepicks_player_id,
                p.prizepicks_ppid,
                COALESCE(
                    (
                        SELECT TOP 1 x.sportsbook_player_id
                        FROM [dbo].[sportsbook_player_xref] x
                        WHERE x.sportsbook = N'prizepicks'
                          AND p.prizepicks_ppid IS NOT NULL
                          AND (
                               x.external_player_id = N'ppid:' + p.prizepicks_ppid
                            OR x.external_player_id = p.prizepicks_ppid
                          )
                        ORDER BY x.sportsbook_player_id ASC
                    ),
                    (
                        SELECT MIN(t.sportsbook_player_id)
                        FROM [dbo].[sportsbook_player] t
                        WHERE t.display_name = COALESCE(p.display_name, N'Player ' + p.prizepicks_player_id)
                          AND ((t.team_abbrev = p.team_abbrev) OR (t.team_abbrev IS NULL AND p.team_abbrev IS NULL))
                          AND (
                                t.jersey_number IS NULL
                             OR p.jersey_number IS NULL
                             OR t.jersey_number = p.jersey_number
                          )
                    )
                ) AS sportsbook_player_id,
                COALESCE(N'ppid:' + p.prizepicks_ppid, N'name_team:' + COALESCE(p.display_name, N'') + N'|' + COALESCE(p.team_abbrev, N'')) AS merge_key
            FROM #pp p;

            -- 3) Upsert xref mapping (store both ppid and player_id with prefixes).
            MERGE [dbo].[sportsbook_player_xref] AS x
            USING (
                SELECT N'prizepicks' AS sportsbook, N'player_id:' + prizepicks_player_id AS external_player_id, sportsbook_player_id
                FROM #pp_map
                UNION ALL
                SELECT N'prizepicks' AS sportsbook, N'ppid:' + prizepicks_ppid AS external_player_id, sportsbook_player_id
                FROM #pp_map
                WHERE prizepicks_ppid IS NOT NULL
            ) AS s
                ON x.sportsbook = s.sportsbook
               AND x.external_player_id = s.external_player_id
            WHEN MATCHED THEN
                UPDATE SET
                    x.sportsbook_player_id = s.sportsbook_player_id,
                    x.last_modified_at = {now}
            WHEN NOT MATCHED BY TARGET THEN
                INSERT (sportsbook, external_player_id, sportsbook_player_id, created_at, last_modified_at)
                VALUES (s.sportsbook, s.external_player_id, s.sportsbook_player_id, {now}, {now});

            -- 4) Fill the \"preferred\" prizepicks_player_id column only when NULL.
            UPDATE t
            SET t.prizepicks_player_id = p.prizepicks_player_id,
                t.last_modified_at = {now}
            FROM [dbo].[sportsbook_player] t
            INNER JOIN #pp_map m ON m.sportsbook_player_id = t.sportsbook_player_id
            INNER JOIN #pp p ON p.prizepicks_player_id = m.prizepicks_player_id
            WHERE t.prizepicks_player_id IS NULL;
            """,
        )

    if _table_exists(conn, "dbo", "parlay_play_player"):
        _reapply_sportsbook_player_column_alter(conn)
        _parlay_create_stage_temp_table(conn)
        _parlay_populate_stage_temp_python(conn)
        for batch in _parlay_play_player_sql_update_batches(conn, now_sql=now):
            _merge_exec(conn, batch)

    if _table_exists(conn, "dbo", "underdog_player"):
        _merge_exec(
            conn,
            f"""
            IF OBJECT_ID('tempdb..#ud') IS NOT NULL DROP TABLE #ud;
            CREATE TABLE #ud(
                underdog_player_id nvarchar(64) NOT NULL PRIMARY KEY,
                display_name nvarchar(255) NULL,
                first_name nvarchar(100) NULL,
                last_name nvarchar(100) NULL,
                position nvarchar(50) NULL,
                jersey_number nvarchar(20) NULL,
                team_abbrev nvarchar(20) NULL
            );

            INSERT INTO #ud(underdog_player_id, display_name, first_name, last_name, position, jersey_number, team_abbrev)
            SELECT
                CAST(u.[id] AS nvarchar(64)) AS underdog_player_id,
                NULLIF(LTRIM(RTRIM(CONCAT(u.[first_name], N' ', u.[last_name]))), N'') AS display_name,
                NULLIF(LTRIM(RTRIM(u.[first_name])), N'') AS first_name,
                NULLIF(LTRIM(RTRIM(u.[last_name])), N'') AS last_name,
                NULLIF(LTRIM(RTRIM(u.[position_display_name])), N'') AS position,
                NULLIF(LTRIM(RTRIM(u.[jersey_number])), N'') AS jersey_number,
                NULLIF(LTRIM(RTRIM(st.[abbreviation])), N'') AS team_abbrev
            FROM [dbo].[underdog_player] u
            LEFT JOIN [dbo].[sportsbook_team] st
                ON st.[underdog_team_id] = CAST(u.[team_id] AS nvarchar(50));

            UPDATE t
            SET
                t.display_name = COALESCE(s.display_name, t.display_name),
                t.first_name = COALESCE(s.first_name, t.first_name),
                t.last_name = COALESCE(s.last_name, t.last_name),
                t.position = COALESCE(s.position, t.position),
                t.jersey_number = COALESCE(s.jersey_number, t.jersey_number),
                t.team_abbrev = COALESCE(t.team_abbrev, s.team_abbrev),
                t.last_modified_at = {now}
            FROM [dbo].[sportsbook_player] t
            INNER JOIN #ud s ON t.underdog_player_id = s.underdog_player_id;

            ;WITH cand AS (
                SELECT
                    t.sportsbook_player_id,
                    s.underdog_player_id,
                    COUNT(*) OVER (PARTITION BY s.underdog_player_id) AS c1
                FROM #ud s
                INNER JOIN [dbo].[sportsbook_player] t
                    ON t.underdog_player_id IS NULL
                   AND t.display_name = COALESCE(s.display_name, N'Player ' + s.underdog_player_id)
                   AND (
                        s.team_abbrev IS NULL
                     OR t.team_abbrev IS NULL
                     OR t.team_abbrev = s.team_abbrev
                   )
                   AND (
                        t.jersey_number IS NULL
                     OR s.jersey_number IS NULL
                     OR t.jersey_number = s.jersey_number
                   )
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM [dbo].[sportsbook_player] t2
                    WHERE t2.underdog_player_id = s.underdog_player_id
                )
            )
            UPDATE t
            SET
                t.underdog_player_id = c.underdog_player_id,
                t.first_name = COALESCE(t.first_name, ud.first_name),
                t.last_name = COALESCE(t.last_name, ud.last_name),
                t.position = COALESCE(t.position, ud.position),
                t.jersey_number = COALESCE(t.jersey_number, ud.jersey_number),
                t.team_abbrev = COALESCE(t.team_abbrev, ud.team_abbrev),
                t.last_modified_at = {now}
            FROM [dbo].[sportsbook_player] t
            INNER JOIN cand c ON c.sportsbook_player_id = t.sportsbook_player_id
            INNER JOIN #ud ud ON ud.underdog_player_id = c.underdog_player_id
            WHERE c.c1 = 1;

            INSERT INTO [dbo].[sportsbook_player](
                display_name, first_name, last_name, position, jersey_number, team_abbrev, underdog_player_id, created_at, last_modified_at
            )
            SELECT
                COALESCE(s.display_name, N'Player ' + s.underdog_player_id),
                s.first_name,
                s.last_name,
                s.position,
                s.jersey_number,
                s.team_abbrev,
                s.underdog_player_id,
                {now}, {now}
            FROM #ud s
            WHERE NOT EXISTS (
                SELECT 1 FROM [dbo].[sportsbook_player] t WHERE t.underdog_player_id = s.underdog_player_id
            );
            """,
        )


def _sync_game(conn) -> None:
    now = _now_central_sql()

    if _table_exists(conn, "dbo", "prizepicks_game"):
        _merge_exec(
            conn,
            f"""
            MERGE [dbo].[sportsbook_game] AS t
            USING (
                SELECT
                    CAST(g.[game_id] AS nvarchar(20)) AS prizepicks_game_id,
                    TRY_CAST(g.[league_id] AS int) AS canonical_league_id,
                    TRY_CAST(g.[start_time] AS datetime2(3)) AS start_time,
                    NULLIF(LTRIM(RTRIM(g.[home_abbreviation])), N'') AS home_team_abbrev,
                    NULLIF(LTRIM(RTRIM(g.[away_abbreviation])), N'') AS away_team_abbrev,
                    NULLIF(LTRIM(RTRIM(g.[name])), N'') AS event_name
                FROM [dbo].[prizepicks_game] g
            ) AS s
                ON t.prizepicks_game_id = s.prizepicks_game_id
            WHEN MATCHED THEN
                UPDATE SET
                    t.canonical_league_id = COALESCE(s.canonical_league_id, t.canonical_league_id),
                    t.start_time = COALESCE(s.start_time, t.start_time),
                    t.home_team_abbrev = COALESCE(s.home_team_abbrev, t.home_team_abbrev),
                    t.away_team_abbrev = COALESCE(s.away_team_abbrev, t.away_team_abbrev),
                    t.event_name = COALESCE(s.event_name, t.event_name),
                    t.last_modified_at = {now}
            WHEN NOT MATCHED BY TARGET THEN
                INSERT (
                    canonical_league_id, start_time, home_team_abbrev, away_team_abbrev, event_name,
                    prizepicks_game_id, created_at, last_modified_at
                )
                VALUES (
                    s.canonical_league_id, s.start_time, s.home_team_abbrev, s.away_team_abbrev, s.event_name,
                    s.prizepicks_game_id, {now}, {now}
                );
            """,
        )

    if _table_exists(conn, "dbo", "parlay_play_match"):
        from cross_book_stat_normalize import PARLAY_MATCH_LEAGUE_ID_TO_PRIZEPICKS

        rows: list[tuple] = []
        cur = conn.cursor()
        try:
            # `parlay_play_match.match_date` is often datetimeoffset; cast to datetime2 for pyodbc compatibility.
            cur.execute(
                """
                SELECT m.id, m.league_id, CAST(m.match_date AS datetime2(3)) AS match_date,
                       ht.team_abbreviation, ht.teamname_abbr,
                       at.team_abbreviation, at.teamname_abbr,
                       m.slug
                FROM [dbo].[parlay_play_match] m
                LEFT JOIN [dbo].[parlay_play_team] ht ON ht.id = m.home_team_id
                LEFT JOIN [dbo].[parlay_play_team] at ON at.id = m.away_team_id
                """
            )
            for mid, lid, mdate, hta, htab, ata, atab, slug in cur.fetchall():
                canonical = PARLAY_MATCH_LEAGUE_ID_TO_PRIZEPICKS.get(int(lid)) if lid is not None else None
                home = (hta or htab or "").strip() or None
                away = (ata or atab or "").strip() or None
                rows.append((int(mid), canonical, mdate, home, away, (slug or "").strip() or None))
        finally:
            cur.close()

        temp_sql = """
        IF OBJECT_ID('tempdb..#gm') IS NOT NULL DROP TABLE #gm;
        CREATE TABLE #gm(
            parlay_play_match_id int NOT NULL,
            canonical_league_id int NULL,
            start_time datetime2(3) NULL,
            home_team_abbrev nvarchar(20) NULL,
            away_team_abbrev nvarchar(20) NULL,
            event_name nvarchar(200) NULL
        );
        """
        _stage_rows(
            conn,
            temp_sql,
            "INSERT INTO #gm (parlay_play_match_id, canonical_league_id, start_time, home_team_abbrev, away_team_abbrev, event_name) VALUES (?,?,?,?,?,?)",
            rows,
        )

        _merge_exec(
            conn,
            f"""
            MERGE [dbo].[sportsbook_game] AS t
            USING #gm AS s
                ON t.parlay_play_match_id = s.parlay_play_match_id
            WHEN MATCHED THEN
                UPDATE SET
                    t.canonical_league_id = COALESCE(s.canonical_league_id, t.canonical_league_id),
                    t.start_time = COALESCE(s.start_time, t.start_time),
                    t.home_team_abbrev = COALESCE(s.home_team_abbrev, t.home_team_abbrev),
                    t.away_team_abbrev = COALESCE(s.away_team_abbrev, t.away_team_abbrev),
                    t.event_name = COALESCE(s.event_name, t.event_name),
                    t.last_modified_at = {now}
            WHEN NOT MATCHED BY TARGET THEN
                INSERT (canonical_league_id, start_time, home_team_abbrev, away_team_abbrev, event_name, parlay_play_match_id, created_at, last_modified_at)
                VALUES (s.canonical_league_id, s.start_time, s.home_team_abbrev, s.away_team_abbrev, s.event_name, s.parlay_play_match_id, {now}, {now});
            """,
        )

        # Heuristic attach parlay match id onto PP games when unambiguous.
        _merge_exec(
            conn,
            f"""
            ;WITH cand AS (
                SELECT
                    t.sportsbook_game_id,
                    s.parlay_play_match_id,
                    COUNT(*) OVER (PARTITION BY s.parlay_play_match_id) AS c1
                FROM #gm s
                INNER JOIN [dbo].[sportsbook_game] t
                    ON t.parlay_play_match_id IS NULL
                   AND t.start_time IS NOT NULL
                   AND s.start_time IS NOT NULL
                   AND CONVERT(date, t.start_time) = CONVERT(date, s.start_time)
                   AND (t.canonical_league_id = s.canonical_league_id OR s.canonical_league_id IS NULL OR t.canonical_league_id IS NULL)
                   AND (t.home_team_abbrev = s.home_team_abbrev OR (t.home_team_abbrev IS NULL AND t.away_team_abbrev = s.home_team_abbrev))
                   AND (t.away_team_abbrev = s.away_team_abbrev OR (t.away_team_abbrev IS NULL AND t.home_team_abbrev = s.away_team_abbrev))
                WHERE s.parlay_play_match_id IS NOT NULL
                  AND NOT EXISTS (
                      SELECT 1
                      FROM [dbo].[sportsbook_game] t2
                      WHERE t2.parlay_play_match_id = s.parlay_play_match_id
                  )
            )
            UPDATE t
            SET t.parlay_play_match_id = c.parlay_play_match_id,
                t.last_modified_at = {now}
            FROM [dbo].[sportsbook_game] t
            INNER JOIN cand c ON c.sportsbook_game_id = t.sportsbook_game_id
            WHERE c.c1 = 1;
            """,
        )

    if _table_exists(conn, "dbo", "underdog_game"):
        _merge_exec(
            conn,
            f"""
            MERGE [dbo].[sportsbook_game] AS t
            USING (
                SELECT
                    CAST(g.[id] AS nvarchar(64)) AS underdog_game_id,
                    TRY_CAST(g.[scheduled_at] AS datetime2(3)) AS start_time,
                    NULLIF(LTRIM(RTRIM(g.[abbreviated_title])), N'') AS event_name
                FROM [dbo].[underdog_game] g
            ) AS s
                ON t.underdog_game_id = s.underdog_game_id
            WHEN MATCHED THEN
                UPDATE SET
                    t.start_time = COALESCE(s.start_time, t.start_time),
                    t.event_name = COALESCE(s.event_name, t.event_name),
                    t.last_modified_at = {now}
            WHEN NOT MATCHED BY TARGET THEN
                INSERT (start_time, event_name, underdog_game_id, created_at, last_modified_at)
                VALUES (s.start_time, s.event_name, s.underdog_game_id, {now}, {now});
            """,
        )


def _link_dimension_fks(conn) -> None:
    """Fill nullable FK columns using mapping keys (no name-based league linking)."""
    now = _now_central_sql()

    # team -> league by canonical_league_id where unambiguous.
    _merge_exec(
        conn,
        f"""
        UPDATE t
        SET t.sportsbook_league_id = l.sportsbook_league_id,
            t.last_modified_at = {now}
        FROM [dbo].[sportsbook_team] t
        INNER JOIN [dbo].[sportsbook_league] l
            ON l.canonical_league_id IS NOT NULL
           AND t.canonical_league_id IS NOT NULL
           AND l.canonical_league_id = t.canonical_league_id
        WHERE t.sportsbook_league_id IS NULL;
        """,
    )

    # player -> league by canonical_league_id (dict/CASE output only).
    _merge_exec(
        conn,
        f"""
        UPDATE p
        SET p.sportsbook_league_id = l.sportsbook_league_id,
            p.last_modified_at = {now}
        FROM [dbo].[sportsbook_player] p
        INNER JOIN [dbo].[sportsbook_league] l
            ON l.canonical_league_id IS NOT NULL
           AND p.canonical_league_id IS NOT NULL
           AND l.canonical_league_id = p.canonical_league_id
        WHERE p.sportsbook_league_id IS NULL;
        """,
    )

    # player -> team by (canonical_league_id, team_abbrev) single-candidate heuristic.
    _merge_exec(
        conn,
        f"""
        ;WITH cand AS (
            SELECT
                p.sportsbook_player_id,
                t.sportsbook_team_id,
                COUNT(*) OVER (PARTITION BY p.sportsbook_player_id) AS c1
            FROM [dbo].[sportsbook_player] p
            INNER JOIN [dbo].[sportsbook_team] t
                ON p.sportsbook_team_id IS NULL
               AND p.team_abbrev IS NOT NULL
               AND t.abbreviation IS NOT NULL
               AND UPPER(LTRIM(RTRIM(p.team_abbrev))) = UPPER(LTRIM(RTRIM(t.abbreviation)))
               AND (
                   (p.canonical_league_id IS NOT NULL AND t.canonical_league_id = p.canonical_league_id)
                   OR p.canonical_league_id IS NULL
                   OR t.canonical_league_id IS NULL
               )
        )
        UPDATE p
        SET p.sportsbook_team_id = c.sportsbook_team_id,
            p.last_modified_at = {now}
        FROM [dbo].[sportsbook_player] p
        INNER JOIN cand c
            ON c.sportsbook_player_id = p.sportsbook_player_id
        WHERE c.c1 = 1;
        """,
    )

    # player -> sport via league -> sport.
    _merge_exec(
        conn,
        f"""
        UPDATE p
        SET p.sportsbook_sport_id = l.sportsbook_sport_id,
            p.last_modified_at = {now}
        FROM [dbo].[sportsbook_player] p
        INNER JOIN [dbo].[sportsbook_league] l
            ON l.sportsbook_league_id = p.sportsbook_league_id
        WHERE p.sportsbook_sport_id IS NULL
          AND l.sportsbook_sport_id IS NOT NULL;
        """,
    )

    # game -> home/away teams by abbrev (single-candidate within league/date context is handled upstream).
    _merge_exec(
        conn,
        f"""
        UPDATE g
        SET g.home_sportsbook_team_id = t.sportsbook_team_id,
            g.last_modified_at = {now}
        FROM [dbo].[sportsbook_game] g
        INNER JOIN [dbo].[sportsbook_team] t
            ON g.home_sportsbook_team_id IS NULL
           AND g.home_team_abbrev IS NOT NULL
           AND t.abbreviation IS NOT NULL
           AND UPPER(LTRIM(RTRIM(g.home_team_abbrev))) = UPPER(LTRIM(RTRIM(t.abbreviation)))
           AND (
                (g.canonical_league_id IS NOT NULL AND t.canonical_league_id = g.canonical_league_id)
                OR g.canonical_league_id IS NULL
                OR t.canonical_league_id IS NULL
           );
        """,
    )
    _merge_exec(
        conn,
        f"""
        UPDATE g
        SET g.away_sportsbook_team_id = t.sportsbook_team_id,
            g.last_modified_at = {now}
        FROM [dbo].[sportsbook_game] g
        INNER JOIN [dbo].[sportsbook_team] t
            ON g.away_sportsbook_team_id IS NULL
           AND g.away_team_abbrev IS NOT NULL
           AND t.abbreviation IS NOT NULL
           AND UPPER(LTRIM(RTRIM(g.away_team_abbrev))) = UPPER(LTRIM(RTRIM(t.abbreviation)))
           AND (
                (g.canonical_league_id IS NOT NULL AND t.canonical_league_id = g.canonical_league_id)
                OR g.canonical_league_id IS NULL
                OR t.canonical_league_id IS NULL
           );
        """,
    )


def sync_sportsbook_dimensions(conn) -> None:
    """Sync all unified dimensions from available reference tables."""
    ensure_sportsbook_dimension_tables(conn)
    _sync_sport(conn)
    _sync_league(conn)
    _sync_team(conn)
    _sync_stat_type(conn)
    _sync_player(conn)
    _sync_game(conn)
    _link_dimension_fks(conn)


def sync_sportsbook_dimensions_snapshot(
    server: str = "localhost\\SQLEXPRESS",
    database: str = "Props",
    user: str = "dbadmin",
    password: str = "",
    trusted_connection: bool = False,
) -> None:
    conn = _get_db_conn(server, database, user, password, trusted_connection)
    try:
        sync_sportsbook_dimensions(conn)
    finally:
        conn.close()

