"""DuckDB persistence layer with incremental refresh for DataSync JSON reports.

Supports both Enhanced and Basic mode report schemas. Records are normalized
into a unified table on ingestion. Uses timestamp-based watermarks for
incremental S3 listing.
"""

from __future__ import annotations

from datetime import datetime

import duckdb
import pandas as pd

import config
from s3_client import NormalizedRecord, ReportKey

_SCHEMA_VERSION_KEY = "schema_version"

_DDL = """
CREATE TABLE IF NOT EXISTS kv_meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS ingestion_log (
    s3_key         TEXT PRIMARY KEY,
    task_name      TEXT NOT NULL,
    task_id        TEXT NOT NULL,
    execution_id   TEXT NOT NULL,
    record_count   INTEGER NOT NULL,
    ingested_at    TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS file_records (
    id                INTEGER PRIMARY KEY DEFAULT(nextval('file_records_seq')),
    task_name         TEXT NOT NULL,
    task_id           TEXT NOT NULL,
    execution_id      TEXT NOT NULL,
    report_type       TEXT NOT NULL,
    task_mode         TEXT,
    relative_path     TEXT,
    content_size      BIGINT,
    item_type         TEXT,
    status            TEXT,
    transfer_type     TEXT,
    event_timestamp   TEXT,
    overwrite         TEXT,
    error_code        TEXT,
    error_detail      TEXT,
    src_metadata_json TEXT,
    s3_key            TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_fr_task      ON file_records (task_name);
CREATE INDEX IF NOT EXISTS idx_fr_exec      ON file_records (execution_id);
CREATE INDEX IF NOT EXISTS idx_fr_type      ON file_records (report_type);
CREATE INDEX IF NOT EXISTS idx_fr_status    ON file_records (status);
CREATE INDEX IF NOT EXISTS idx_fr_tasktype  ON file_records (task_name, report_type);
"""


def _needs_recreate(conn: duckdb.DuckDBPyConnection) -> bool:
    try:
        row = conn.execute(
            "SELECT value FROM kv_meta WHERE key = ?", [_SCHEMA_VERSION_KEY]
        ).fetchone()
        if row and int(row[0]) >= config.SCHEMA_VERSION:
            return False
    except duckdb.CatalogException:
        pass
    return True


def get_connection(db_path: str | None = None) -> duckdb.DuckDBPyConnection:
    path = db_path or config.DB_PATH
    conn = duckdb.connect(path)

    if _needs_recreate(conn):
        conn.execute("DROP TABLE IF EXISTS file_records")
        conn.execute("DROP TABLE IF EXISTS ingestion_log")
        conn.execute("DROP TABLE IF EXISTS kv_meta")
        conn.execute("DROP SEQUENCE IF EXISTS file_records_seq")

    conn.execute("CREATE SEQUENCE IF NOT EXISTS file_records_seq START 1")
    conn.execute(_DDL)

    conn.execute(
        "INSERT OR REPLACE INTO kv_meta (key, value) VALUES (?, ?)",
        [_SCHEMA_VERSION_KEY, str(config.SCHEMA_VERSION)],
    )
    return conn


# ---------------------------------------------------------------------------
# Watermark (timestamp-based incremental refresh)
# ---------------------------------------------------------------------------

_WATERMARK_KEY = "last_refresh_at"


def get_watermark(conn: duckdb.DuckDBPyConnection) -> datetime | None:
    try:
        row = conn.execute(
            "SELECT value FROM kv_meta WHERE key = ?", [_WATERMARK_KEY]
        ).fetchone()
        if row:
            return datetime.fromisoformat(row[0])
    except Exception:
        pass
    return None


def set_watermark(conn: duckdb.DuckDBPyConnection, ts: datetime):
    conn.execute(
        "INSERT OR REPLACE INTO kv_meta (key, value) VALUES (?, ?)",
        [_WATERMARK_KEY, ts.isoformat()],
    )


# ---------------------------------------------------------------------------
# Ingestion
# ---------------------------------------------------------------------------

def get_ingested_keys(conn: duckdb.DuckDBPyConnection) -> set[str]:
    rows = conn.execute("SELECT s3_key FROM ingestion_log").fetchall()
    return {r[0] for r in rows}


def ingest_batch(
    conn: duckdb.DuckDBPyConnection,
    report_key: ReportKey,
    records: list[NormalizedRecord],
) -> int:
    if not records:
        conn.execute(
            "INSERT OR IGNORE INTO ingestion_log "
            "(s3_key, task_name, task_id, execution_id, record_count, ingested_at) "
            "VALUES (?, ?, ?, ?, 0, ?)",
            [report_key.key, report_key.task_name, report_key.task_id,
             report_key.exec_id, datetime.now(config.IST)],
        )
        return 0

    rows = [
        (r.task_name, r.task_id, r.execution_id, r.report_type, r.task_mode,
         r.relative_path, r.content_size, r.item_type, r.status,
         r.transfer_type, r.event_timestamp, r.overwrite,
         r.error_code, r.error_detail, r.src_metadata_json, r.s3_key)
        for r in records
    ]

    df = pd.DataFrame(rows, columns=[
        "task_name", "task_id", "execution_id", "report_type", "task_mode",
        "relative_path", "content_size", "item_type", "status",
        "transfer_type", "event_timestamp", "overwrite",
        "error_code", "error_detail", "src_metadata_json", "s3_key",
    ])

    conn.execute(
        "INSERT INTO file_records "
        "(task_name, task_id, execution_id, report_type, task_mode, "
        "relative_path, content_size, item_type, status, "
        "transfer_type, event_timestamp, overwrite, "
        "error_code, error_detail, src_metadata_json, s3_key) "
        "SELECT * FROM df"
    )

    conn.execute(
        "INSERT OR IGNORE INTO ingestion_log "
        "(s3_key, task_name, task_id, execution_id, record_count, ingested_at) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        [report_key.key, report_key.task_name, report_key.task_id,
         report_key.exec_id, len(rows), datetime.now(config.IST)],
    )

    return len(rows)


# ---------------------------------------------------------------------------
# Query helpers
# ---------------------------------------------------------------------------

def task_summary(conn: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    return conn.execute("""
        SELECT
            task_name,
            COUNT(DISTINCT execution_id)                                     AS executions,
            SUM(CASE WHEN report_type = 'transferred' THEN 1 ELSE 0 END)    AS transferred,
            SUM(CASE WHEN report_type = 'verified'    THEN 1 ELSE 0 END)    AS verified,
            SUM(CASE WHEN report_type = 'transferred' AND status = 'FAILED' THEN 1 ELSE 0 END) AS transfer_failed,
            SUM(CASE WHEN report_type = 'skipped'     THEN 1 ELSE 0 END)    AS skipped,
            SUM(CASE WHEN report_type = 'deleted'     THEN 1 ELSE 0 END)    AS deleted,
            SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END)              AS total_failed,
            COUNT(*)                                                         AS total_records
        FROM file_records
        GROUP BY task_name
        ORDER BY task_name
    """).fetchdf()


def execution_summary(conn: duckdb.DuckDBPyConnection, task_name: str) -> pd.DataFrame:
    return conn.execute("""
        SELECT
            execution_id,
            MAX(task_mode)                                                   AS task_mode,
            SUM(CASE WHEN report_type = 'transferred' THEN 1 ELSE 0 END)    AS transferred,
            SUM(CASE WHEN report_type = 'verified'    THEN 1 ELSE 0 END)    AS verified,
            SUM(CASE WHEN report_type = 'transferred' AND status = 'FAILED' THEN 1 ELSE 0 END) AS transfer_failed,
            SUM(CASE WHEN report_type = 'skipped'     THEN 1 ELSE 0 END)    AS skipped,
            SUM(CASE WHEN report_type = 'deleted'     THEN 1 ELSE 0 END)    AS deleted,
            SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END)              AS total_failed,
            COUNT(*)                                                         AS total_records,
            MIN(event_timestamp)                                             AS first_event,
            MAX(event_timestamp)                                             AS last_event
        FROM file_records
        WHERE task_name = ?
        GROUP BY execution_id
        ORDER BY execution_id
    """, [task_name]).fetchdf()


def file_detail(
    conn: duckdb.DuckDBPyConnection,
    task_name: str | None = None,
    execution_id: str | None = None,
    report_type: str | None = None,
    status: str | None = None,
    search: str | None = None,
    limit: int = 500,
    offset: int = 0,
) -> pd.DataFrame:
    clauses: list[str] = []
    params: list = []

    if task_name:
        clauses.append("task_name = ?")
        params.append(task_name)
    if execution_id:
        clauses.append("execution_id = ?")
        params.append(execution_id)
    if report_type:
        clauses.append("report_type = ?")
        params.append(report_type)
    if status:
        clauses.append("status = ?")
        params.append(status)
    if search:
        clauses.append("relative_path ILIKE ?")
        params.append(f"%{search}%")

    where = (" WHERE " + " AND ".join(clauses)) if clauses else ""

    return conn.execute(
        f"SELECT task_name, execution_id, report_type, task_mode, "
        f"relative_path, content_size, item_type, status, transfer_type, "
        f"event_timestamp, error_code, error_detail "
        f"FROM file_records{where} "
        f"ORDER BY task_name, execution_id, report_type "
        f"LIMIT ? OFFSET ?",
        params + [limit, offset],
    ).fetchdf()


def file_detail_count(
    conn: duckdb.DuckDBPyConnection,
    task_name: str | None = None,
    execution_id: str | None = None,
    report_type: str | None = None,
    status: str | None = None,
    search: str | None = None,
) -> int:
    clauses: list[str] = []
    params: list = []
    if task_name:
        clauses.append("task_name = ?")
        params.append(task_name)
    if execution_id:
        clauses.append("execution_id = ?")
        params.append(execution_id)
    if report_type:
        clauses.append("report_type = ?")
        params.append(report_type)
    if status:
        clauses.append("status = ?")
        params.append(status)
    if search:
        clauses.append("relative_path ILIKE ?")
        params.append(f"%{search}%")
    where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
    return conn.execute(f"SELECT COUNT(*) FROM file_records{where}", params).fetchone()[0]


def global_stats(conn: duckdb.DuckDBPyConnection) -> dict:
    row = conn.execute("""
        SELECT
            COUNT(DISTINCT task_name)     AS total_tasks,
            COUNT(DISTINCT execution_id)  AS total_executions,
            COUNT(*)                      AS total_file_records,
            SUM(CASE WHEN report_type = 'transferred' THEN 1 ELSE 0 END)                       AS transferred,
            SUM(CASE WHEN report_type = 'transferred' AND status = 'SUCCESS' THEN 1 ELSE 0 END) AS transfer_success,
            SUM(CASE WHEN report_type = 'transferred' AND status = 'FAILED' THEN 1 ELSE 0 END)  AS transfer_failed,
            SUM(CASE WHEN report_type = 'verified'    THEN 1 ELSE 0 END)                       AS verified,
            SUM(CASE WHEN report_type = 'verified'    AND status = 'SUCCESS' THEN 1 ELSE 0 END) AS verify_success,
            SUM(CASE WHEN report_type = 'verified'    AND status = 'FAILED' THEN 1 ELSE 0 END)  AS verify_failed,
            SUM(CASE WHEN report_type = 'skipped'     THEN 1 ELSE 0 END)                       AS skipped,
            SUM(CASE WHEN report_type = 'deleted'     THEN 1 ELSE 0 END)                       AS deleted,
            SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END)                                 AS total_failed,
            SUM(CASE WHEN report_type = 'transferred' THEN content_size ELSE 0 END)              AS total_bytes
        FROM file_records
    """).fetchone()
    if not row:
        return {k: 0 for k in [
            "total_tasks", "total_executions", "total_file_records",
            "transferred", "transfer_success", "transfer_failed",
            "verified", "verify_success", "verify_failed",
            "skipped", "deleted", "total_failed", "total_bytes",
        ]}
    cols = [
        "total_tasks", "total_executions", "total_file_records",
        "transferred", "transfer_success", "transfer_failed",
        "verified", "verify_success", "verify_failed",
        "skipped", "deleted", "total_failed", "total_bytes",
    ]
    return dict(zip(cols, row))


def tasks_with_multiple_executions(conn: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    return conn.execute("""
        SELECT task_name, COUNT(DISTINCT execution_id) AS executions
        FROM file_records
        GROUP BY task_name
        HAVING COUNT(DISTINCT execution_id) > 1
        ORDER BY executions DESC
    """).fetchdf()


def top_failing_tasks(conn: duckdb.DuckDBPyConnection, limit: int = 20) -> pd.DataFrame:
    return conn.execute("""
        SELECT task_name,
               SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) AS failed_records
        FROM file_records
        GROUP BY task_name
        HAVING SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) > 0
        ORDER BY failed_records DESC
        LIMIT ?
    """, [limit]).fetchdf()


def error_code_distribution(
    conn: duckdb.DuckDBPyConnection,
    task_name: str | None = None,
) -> pd.DataFrame:
    clause = ""
    params: list = []
    if task_name:
        clause = " AND task_name = ?"
        params.append(task_name)
    return conn.execute(f"""
        SELECT COALESCE(error_code, 'UNKNOWN') AS error_code, COUNT(*) AS count
        FROM file_records
        WHERE status = 'FAILED'{clause}
        GROUP BY error_code
        ORDER BY count DESC
    """, params).fetchdf()


def task_mode_distribution(conn: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    return conn.execute("""
        SELECT COALESCE(task_mode, 'unknown') AS task_mode,
               COUNT(DISTINCT task_name) AS tasks
        FROM file_records
        GROUP BY task_mode
    """).fetchdf()


def ingestion_stats(conn: duckdb.DuckDBPyConnection) -> dict:
    row = conn.execute("""
        SELECT COUNT(*) AS reports_ingested,
               SUM(record_count) AS total_records,
               MIN(ingested_at) AS first_ingestion,
               MAX(ingested_at) AS last_ingestion
        FROM ingestion_log
    """).fetchone()
    if not row:
        return {"reports_ingested": 0, "total_records": 0,
                "first_ingestion": None, "last_ingestion": None}
    return {"reports_ingested": row[0], "total_records": row[1],
            "first_ingestion": row[2], "last_ingestion": row[3]}


_IST_OFFSET = "INTERVAL '5 hours 30 minutes'"


def _to_ist(ts_expr: str) -> str:
    return f"({ts_expr} + {_IST_OFFSET})"


def _time_bucket_expr(granularity: str) -> str:
    ist = _to_ist("TRY_CAST(event_timestamp AS TIMESTAMP)")
    if granularity == "hour":
        return f"date_trunc('hour', {ist})"
    return f"CAST({ist} AS DATE)"


def transfer_volume_trend(
    conn: duckdb.DuckDBPyConnection,
    granularity: str = "day",
) -> pd.DataFrame:
    bucket = _time_bucket_expr(granularity)
    return conn.execute(f"""
        SELECT
            {bucket} AS period,
            SUM(content_size) AS bytes_moved,
            COUNT(*)          AS files_transferred
        FROM file_records
        WHERE report_type = 'transferred'
          AND event_timestamp IS NOT NULL
        GROUP BY period
        HAVING period IS NOT NULL
        ORDER BY period
    """).fetchdf()


def execution_count_trend(
    conn: duckdb.DuckDBPyConnection,
    granularity: str = "day",
) -> pd.DataFrame:
    bucket = _time_bucket_expr(granularity)
    return conn.execute(f"""
        SELECT
            {bucket} AS period,
            COUNT(DISTINCT execution_id) AS executions
        FROM file_records
        WHERE event_timestamp IS NOT NULL
        GROUP BY period
        HAVING period IS NOT NULL
        ORDER BY period
    """).fetchdf()


def failure_count_trend(
    conn: duckdb.DuckDBPyConnection,
    granularity: str = "day",
) -> pd.DataFrame:
    bucket = _time_bucket_expr(granularity)
    return conn.execute(f"""
        SELECT
            {bucket} AS period,
            SUM(CASE WHEN report_type = 'transferred' AND status = 'FAILED' THEN 1 ELSE 0 END) AS transfer_failures,
            SUM(CASE WHEN report_type = 'verified'    AND status = 'FAILED' THEN 1 ELSE 0 END) AS verify_failures
        FROM file_records
        WHERE status = 'FAILED' AND event_timestamp IS NOT NULL
        GROUP BY period
        HAVING period IS NOT NULL
        ORDER BY period
    """).fetchdf()


def _time_filter(start: str | None, end: str | None) -> tuple[str, list]:
    ist = f"(TRY_CAST(event_timestamp AS TIMESTAMP) + {_IST_OFFSET})"
    clauses: list[str] = []
    params: list = []
    if start:
        clauses.append(f"{ist} >= ?::TIMESTAMP")
        params.append(start)
    if end:
        clauses.append(f"{ist} <= ?::TIMESTAMP")
        params.append(end)
    return (" AND ".join(clauses), params) if clauses else ("", [])


def hourly_transfer_volume(
    conn: duckdb.DuckDBPyConnection,
    start: str | None = None,
    end: str | None = None,
) -> pd.DataFrame:
    time_clause, params = _time_filter(start, end)
    where = f"AND {time_clause}" if time_clause else ""
    return conn.execute(f"""
        SELECT
            date_trunc('hour', TRY_CAST(event_timestamp AS TIMESTAMP) + INTERVAL '5 hours 30 minutes') AS hour,
            SUM(content_size) AS bytes_moved,
            COUNT(*)          AS files_transferred,
            SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) AS succeeded,
            SUM(CASE WHEN status = 'FAILED'  THEN 1 ELSE 0 END) AS failed
        FROM file_records
        WHERE report_type = 'transferred'
          AND event_timestamp IS NOT NULL
          {where}
        GROUP BY hour
        HAVING hour IS NOT NULL
        ORDER BY hour
    """, params).fetchdf()


def hourly_verification(
    conn: duckdb.DuckDBPyConnection,
    start: str | None = None,
    end: str | None = None,
) -> pd.DataFrame:
    time_clause, params = _time_filter(start, end)
    where = f"AND {time_clause}" if time_clause else ""
    return conn.execute(f"""
        SELECT
            date_trunc('hour', TRY_CAST(event_timestamp AS TIMESTAMP) + INTERVAL '5 hours 30 minutes') AS hour,
            COUNT(*) AS total,
            SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) AS succeeded,
            SUM(CASE WHEN status = 'FAILED'  THEN 1 ELSE 0 END) AS failed
        FROM file_records
        WHERE report_type = 'verified'
          AND event_timestamp IS NOT NULL
          {where}
        GROUP BY hour
        HAVING hour IS NOT NULL
        ORDER BY hour
    """, params).fetchdf()


def hourly_execution_count(
    conn: duckdb.DuckDBPyConnection,
    start: str | None = None,
    end: str | None = None,
) -> pd.DataFrame:
    time_clause, params = _time_filter(start, end)
    where = f"AND {time_clause}" if time_clause else ""
    return conn.execute(f"""
        SELECT
            date_trunc('hour', TRY_CAST(event_timestamp AS TIMESTAMP) + INTERVAL '5 hours 30 minutes') AS hour,
            COUNT(DISTINCT execution_id) AS executions,
            COUNT(DISTINCT task_name)    AS tasks
        FROM file_records
        WHERE event_timestamp IS NOT NULL
          {where}
        GROUP BY hour
        HAVING hour IS NOT NULL
        ORDER BY hour
    """, params).fetchdf()


def hourly_failure_breakdown(
    conn: duckdb.DuckDBPyConnection,
    start: str | None = None,
    end: str | None = None,
) -> pd.DataFrame:
    time_clause, params = _time_filter(start, end)
    where = f"AND {time_clause}" if time_clause else ""
    return conn.execute(f"""
        SELECT
            date_trunc('hour', TRY_CAST(event_timestamp AS TIMESTAMP) + INTERVAL '5 hours 30 minutes') AS hour,
            SUM(CASE WHEN report_type = 'transferred' THEN 1 ELSE 0 END) AS transfer_failures,
            SUM(CASE WHEN report_type = 'verified'    THEN 1 ELSE 0 END) AS verify_failures
        FROM file_records
        WHERE status = 'FAILED'
          AND event_timestamp IS NOT NULL
          {where}
        GROUP BY hour
        HAVING hour IS NOT NULL
        ORDER BY hour
    """, params).fetchdf()


def time_range_stats(
    conn: duckdb.DuckDBPyConnection,
    start: str | None = None,
    end: str | None = None,
) -> dict:
    time_clause, params = _time_filter(start, end)
    where = f"AND {time_clause}" if time_clause else ""
    row = conn.execute(f"""
        SELECT
            COUNT(DISTINCT task_name)    AS tasks,
            COUNT(DISTINCT execution_id) AS executions,
            SUM(CASE WHEN report_type = 'transferred' THEN 1 ELSE 0 END) AS files_transferred,
            SUM(CASE WHEN report_type = 'transferred' AND status = 'SUCCESS' THEN 1 ELSE 0 END) AS transfer_ok,
            SUM(CASE WHEN report_type = 'transferred' AND status = 'FAILED'  THEN 1 ELSE 0 END) AS transfer_failed,
            SUM(CASE WHEN report_type = 'verified'    THEN 1 ELSE 0 END) AS files_verified,
            SUM(CASE WHEN report_type = 'verified' AND status = 'FAILED' THEN 1 ELSE 0 END) AS verify_failed,
            SUM(CASE WHEN report_type = 'transferred' THEN content_size ELSE 0 END) AS bytes_transferred,
            AVG(CASE WHEN report_type = 'transferred' THEN content_size END) AS avg_file_size
        FROM file_records
        WHERE event_timestamp IS NOT NULL
          {where}
    """, params).fetchone()
    cols = [
        "tasks", "executions", "files_transferred", "transfer_ok",
        "transfer_failed", "files_verified", "verify_failed",
        "bytes_transferred", "avg_file_size",
    ]
    return dict(zip(cols, row)) if row else {c: 0 for c in cols}


def file_size_stats(conn: duckdb.DuckDBPyConnection) -> dict:
    row = conn.execute("""
        SELECT
            COUNT(*)                    AS total_files,
            SUM(content_size)           AS total_bytes,
            AVG(content_size)           AS avg_size,
            MEDIAN(content_size)        AS median_size,
            MIN(content_size)           AS min_size,
            MAX(content_size)           AS max_size
        FROM file_records
        WHERE report_type = 'transferred' AND content_size IS NOT NULL
    """).fetchone()
    cols = ["total_files", "total_bytes", "avg_size", "median_size", "min_size", "max_size"]
    return dict(zip(cols, row)) if row else {c: 0 for c in cols}


def avg_file_size_by_task(conn: duckdb.DuckDBPyConnection, limit: int = 30) -> pd.DataFrame:
    return conn.execute("""
        SELECT
            task_name,
            COUNT(*)           AS files,
            AVG(content_size)  AS avg_size,
            SUM(content_size)  AS total_bytes
        FROM file_records
        WHERE report_type = 'transferred' AND content_size IS NOT NULL
        GROUP BY task_name
        ORDER BY total_bytes DESC
        LIMIT ?
    """, [limit]).fetchdf()


def purge_all(conn: duckdb.DuckDBPyConnection):
    conn.execute("DELETE FROM file_records")
    conn.execute("DELETE FROM ingestion_log")
    conn.execute(
        "DELETE FROM kv_meta WHERE key = ?", [_WATERMARK_KEY]
    )
