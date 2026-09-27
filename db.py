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
from s3_client import NormalizedRecord, ReportKey, SummaryRecord
from datasync_api import TaskInfo, ExecutionInfo

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

CREATE TABLE IF NOT EXISTS summary_records (
    id                          INTEGER PRIMARY KEY DEFAULT(nextval('summary_records_seq')),
    task_name                   TEXT NOT NULL,
    task_id                     TEXT NOT NULL,
    execution_id                TEXT NOT NULL,
    task_mode                   TEXT,
    overall_status              TEXT,
    start_time                  TEXT,
    end_time                    TEXT,
    total_time                  TEXT,
    files_transferred           BIGINT,
    files_verified              BIGINT,
    files_skipped               BIGINT,
    files_deleted               BIGINT,
    bytes_written               BIGINT,
    bytes_transferred           BIGINT,
    bytes_compressed            BIGINT,
    prepare_duration            TEXT,
    prepare_status              TEXT,
    transfer_duration           TEXT,
    transfer_status             TEXT,
    verify_duration             TEXT,
    verify_status               TEXT,
    error_code                  TEXT,
    error_detail                TEXT,
    files_failed_prepare        BIGINT,
    files_failed_transfer       BIGINT,
    files_failed_verify         BIGINT,
    files_failed_delete         BIGINT,
    files_prepared              BIGINT,
    estimated_bytes_to_transfer BIGINT,
    estimated_files_to_transfer BIGINT,
    source_location_type        TEXT,
    destination_location_type   TEXT,
    s3_key                      TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_sr_task ON summary_records (task_name);
CREATE INDEX IF NOT EXISTS idx_sr_exec ON summary_records (execution_id);
CREATE INDEX IF NOT EXISTS idx_sr_status ON summary_records (overall_status);

CREATE TABLE IF NOT EXISTS api_tasks (
    task_arn    TEXT PRIMARY KEY,
    task_id    TEXT NOT NULL,
    name       TEXT NOT NULL,
    status     TEXT NOT NULL,
    source_location_arn      TEXT,
    destination_location_arn TEXT,
    polled_at  TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS api_executions (
    execution_arn             TEXT PRIMARY KEY,
    execution_id              TEXT NOT NULL,
    task_arn                  TEXT NOT NULL,
    status                    TEXT NOT NULL,
    start_time                TIMESTAMP,
    end_time                  TIMESTAMP,
    bytes_written             BIGINT,
    bytes_transferred         BIGINT,
    bytes_compressed          BIGINT,
    files_transferred         BIGINT,
    files_verified            BIGINT,
    files_skipped             BIGINT,
    files_deleted             BIGINT,
    files_prepared            BIGINT,
    estimated_bytes_to_transfer BIGINT,
    estimated_files_to_transfer BIGINT,
    error_code                TEXT,
    error_detail              TEXT,
    prepare_duration          BIGINT,
    prepare_status            TEXT,
    transfer_duration         BIGINT,
    transfer_status           TEXT,
    verify_duration           BIGINT,
    verify_status             TEXT,
    total_duration            BIGINT,
    polled_at                 TIMESTAMP NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_ae_task ON api_executions (task_arn);
CREATE INDEX IF NOT EXISTS idx_ae_status ON api_executions (status);
CREATE INDEX IF NOT EXISTS idx_ae_exec_id ON api_executions (execution_id);
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
        conn.execute("DROP TABLE IF EXISTS summary_records")
        conn.execute("DROP TABLE IF EXISTS api_tasks")
        conn.execute("DROP TABLE IF EXISTS api_executions")
        conn.execute("DROP TABLE IF EXISTS ingestion_log")
        conn.execute("DROP TABLE IF EXISTS kv_meta")
        conn.execute("DROP SEQUENCE IF EXISTS file_records_seq")
        conn.execute("DROP SEQUENCE IF EXISTS summary_records_seq")

    conn.execute("CREATE SEQUENCE IF NOT EXISTS file_records_seq START 1")
    conn.execute("CREATE SEQUENCE IF NOT EXISTS summary_records_seq START 1")
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


def ingest_chunk_bulk(
    conn: duckdb.DuckDBPyConnection,
    results: list[tuple[ReportKey, list[NormalizedRecord] | "SummaryRecord | None"]],
) -> tuple[int, int]:
    """Bulk-ingest a chunk of download results in two DataFrame inserts.

    Returns (detail_record_count, summary_record_count).
    """
    all_detail_rows = []
    all_log_rows = []
    summary_rows = []
    summary_log_rows = []
    now = datetime.now(config.IST)

    for rk, parsed in results:
        if rk.report_category == "summary":
            if isinstance(parsed, SummaryRecord):
                s = parsed
                summary_rows.append((
                    s.task_name, s.task_id, s.execution_id, s.task_mode, s.overall_status,
                    s.start_time, s.end_time, s.total_time,
                    s.files_transferred, s.files_verified, s.files_skipped, s.files_deleted,
                    s.bytes_written, s.bytes_transferred, s.bytes_compressed,
                    s.prepare_duration, s.prepare_status,
                    s.transfer_duration, s.transfer_status,
                    s.verify_duration, s.verify_status,
                    s.error_code, s.error_detail,
                    s.files_failed_prepare, s.files_failed_transfer,
                    s.files_failed_verify, s.files_failed_delete,
                    s.files_prepared, s.estimated_bytes_to_transfer,
                    s.estimated_files_to_transfer,
                    s.source_location_type, s.destination_location_type,
                    s.s3_key,
                ))
                summary_log_rows.append((
                    rk.key, rk.task_name, rk.task_id, rk.exec_id, 1, now,
                ))
        else:
            records = parsed if parsed else []
            for r in records:
                all_detail_rows.append((
                    r.task_name, r.task_id, r.execution_id, r.report_type, r.task_mode,
                    r.relative_path, r.content_size, r.item_type, r.status,
                    r.transfer_type, r.event_timestamp, r.overwrite,
                    r.error_code, r.error_detail, r.src_metadata_json, r.s3_key,
                ))
            all_log_rows.append((
                rk.key, rk.task_name, rk.task_id, rk.exec_id, len(records), now,
            ))

    detail_count = 0
    if all_detail_rows:
        df = pd.DataFrame(all_detail_rows, columns=[
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
        detail_count = len(all_detail_rows)

    if all_log_rows:
        df_log = pd.DataFrame(all_log_rows, columns=[
            "s3_key", "task_name", "task_id", "execution_id", "record_count", "ingested_at",
        ])
        conn.execute(
            "INSERT OR IGNORE INTO ingestion_log "
            "(s3_key, task_name, task_id, execution_id, record_count, ingested_at) "
            "SELECT * FROM df_log"
        )

    summary_count = 0
    if summary_rows:
        df_sum = pd.DataFrame(summary_rows, columns=[
            "task_name", "task_id", "execution_id", "task_mode", "overall_status",
            "start_time", "end_time", "total_time",
            "files_transferred", "files_verified", "files_skipped", "files_deleted",
            "bytes_written", "bytes_transferred", "bytes_compressed",
            "prepare_duration", "prepare_status",
            "transfer_duration", "transfer_status",
            "verify_duration", "verify_status",
            "error_code", "error_detail",
            "files_failed_prepare", "files_failed_transfer",
            "files_failed_verify", "files_failed_delete",
            "files_prepared", "estimated_bytes_to_transfer",
            "estimated_files_to_transfer",
            "source_location_type", "destination_location_type",
            "s3_key",
        ])
        conn.execute(
            "INSERT INTO summary_records "
            "(task_name, task_id, execution_id, task_mode, overall_status, "
            "start_time, end_time, total_time, "
            "files_transferred, files_verified, files_skipped, files_deleted, "
            "bytes_written, bytes_transferred, bytes_compressed, "
            "prepare_duration, prepare_status, transfer_duration, transfer_status, "
            "verify_duration, verify_status, error_code, error_detail, "
            "files_failed_prepare, files_failed_transfer, files_failed_verify, files_failed_delete, "
            "files_prepared, estimated_bytes_to_transfer, estimated_files_to_transfer, "
            "source_location_type, destination_location_type, s3_key) "
            "SELECT * FROM df_sum"
        )
        summary_count = len(summary_rows)

    if summary_log_rows:
        df_slog = pd.DataFrame(summary_log_rows, columns=[
            "s3_key", "task_name", "task_id", "execution_id", "record_count", "ingested_at",
        ])
        conn.execute(
            "INSERT OR IGNORE INTO ingestion_log "
            "(s3_key, task_name, task_id, execution_id, record_count, ingested_at) "
            "SELECT * FROM df_slog"
        )

    return detail_count, summary_count


def ingest_summary(
    conn: duckdb.DuckDBPyConnection,
    report_key: ReportKey,
    summary: SummaryRecord,
) -> int:
    conn.execute(
        "INSERT INTO summary_records "
        "(task_name, task_id, execution_id, task_mode, overall_status, "
        "start_time, end_time, total_time, "
        "files_transferred, files_verified, files_skipped, files_deleted, "
        "bytes_written, bytes_transferred, bytes_compressed, "
        "prepare_duration, prepare_status, transfer_duration, transfer_status, "
        "verify_duration, verify_status, error_code, error_detail, "
        "files_failed_prepare, files_failed_transfer, files_failed_verify, files_failed_delete, "
        "files_prepared, estimated_bytes_to_transfer, estimated_files_to_transfer, "
        "source_location_type, destination_location_type, s3_key) "
        "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        [
            summary.task_name, summary.task_id, summary.execution_id,
            summary.task_mode, summary.overall_status,
            summary.start_time, summary.end_time, summary.total_time,
            summary.files_transferred, summary.files_verified,
            summary.files_skipped, summary.files_deleted,
            summary.bytes_written, summary.bytes_transferred, summary.bytes_compressed,
            summary.prepare_duration, summary.prepare_status,
            summary.transfer_duration, summary.transfer_status,
            summary.verify_duration, summary.verify_status,
            summary.error_code, summary.error_detail,
            summary.files_failed_prepare, summary.files_failed_transfer,
            summary.files_failed_verify, summary.files_failed_delete,
            summary.files_prepared,
            summary.estimated_bytes_to_transfer, summary.estimated_files_to_transfer,
            summary.source_location_type, summary.destination_location_type,
            summary.s3_key,
        ],
    )

    conn.execute(
        "INSERT OR IGNORE INTO ingestion_log "
        "(s3_key, task_name, task_id, execution_id, record_count, ingested_at) "
        "VALUES (?, ?, ?, ?, 1, ?)",
        [report_key.key, report_key.task_name, report_key.task_id,
         report_key.exec_id, datetime.now(config.IST)],
    )
    return 1


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


def execution_summaries(
    conn: duckdb.DuckDBPyConnection,
    task_name: str | None = None,
    execution_id: str | None = None,
) -> pd.DataFrame:
    clauses: list[str] = []
    params: list = []
    if task_name:
        clauses.append("task_name = ?")
        params.append(task_name)
    if execution_id:
        clauses.append("execution_id = ?")
        params.append(execution_id)
    where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
    return conn.execute(f"""
        SELECT
            task_name, task_id, execution_id, task_mode, overall_status,
            start_time, end_time, total_time,
            files_transferred, files_verified, files_skipped, files_deleted,
            bytes_written, bytes_transferred, bytes_compressed,
            prepare_duration, prepare_status,
            transfer_duration, transfer_status,
            verify_duration, verify_status,
            error_code, error_detail,
            files_failed_prepare, files_failed_transfer,
            files_failed_verify, files_failed_delete,
            files_prepared, estimated_bytes_to_transfer, estimated_files_to_transfer,
            source_location_type, destination_location_type
        FROM summary_records{where}
        ORDER BY task_name, execution_id
    """, params).fetchdf()


def summary_global_stats(conn: duckdb.DuckDBPyConnection) -> dict:
    row = conn.execute("""
        SELECT
            COUNT(*)                                AS total_summaries,
            COUNT(DISTINCT task_name)               AS tasks_with_summary,
            COUNT(DISTINCT execution_id)            AS executions_with_summary,
            SUM(COALESCE(files_transferred, 0))     AS sum_files_transferred,
            SUM(COALESCE(files_verified, 0))        AS sum_files_verified,
            SUM(COALESCE(files_skipped, 0))         AS sum_files_skipped,
            SUM(COALESCE(files_deleted, 0))         AS sum_files_deleted,
            SUM(COALESCE(bytes_written, 0))         AS sum_bytes_written,
            SUM(COALESCE(bytes_transferred, 0))     AS sum_bytes_transferred,
            SUM(CASE WHEN overall_status IN ('ERROR', 'FAILED') THEN 1 ELSE 0 END) AS failed_executions
        FROM summary_records
    """).fetchone()
    cols = [
        "total_summaries", "tasks_with_summary", "executions_with_summary",
        "sum_files_transferred", "sum_files_verified", "sum_files_skipped",
        "sum_files_deleted", "sum_bytes_written", "sum_bytes_transferred",
        "failed_executions",
    ]
    return dict(zip(cols, row)) if row else {c: 0 for c in cols}


def executions_without_details(conn: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    return conn.execute("""
        SELECT s.*
        FROM summary_records s
        LEFT JOIN (
            SELECT DISTINCT execution_id FROM file_records
        ) d ON s.execution_id = d.execution_id
        WHERE d.execution_id IS NULL
        ORDER BY s.task_name, s.execution_id
    """).fetchdf()


def task_summary_combined(conn: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    return conn.execute("""
        WITH detail_stats AS (
            SELECT
                task_name,
                COUNT(DISTINCT execution_id) AS detail_executions,
                SUM(CASE WHEN report_type = 'transferred' THEN 1 ELSE 0 END) AS transferred,
                SUM(CASE WHEN report_type = 'verified'    THEN 1 ELSE 0 END) AS verified,
                SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) AS total_failed,
                SUM(CASE WHEN report_type = 'skipped'     THEN 1 ELSE 0 END) AS skipped,
                SUM(CASE WHEN report_type = 'deleted'     THEN 1 ELSE 0 END) AS deleted,
                COUNT(*) AS total_records
            FROM file_records
            GROUP BY task_name
        ),
        summary_stats AS (
            SELECT
                task_name,
                COUNT(DISTINCT execution_id) AS summary_executions,
                SUM(COALESCE(files_transferred, 0)) AS sum_files_transferred,
                SUM(COALESCE(files_verified, 0)) AS sum_files_verified,
                SUM(COALESCE(files_skipped, 0)) AS sum_files_skipped,
                SUM(COALESCE(files_deleted, 0)) AS sum_files_deleted,
                SUM(COALESCE(bytes_written, 0)) AS sum_bytes_written,
                SUM(CASE WHEN overall_status IN ('ERROR', 'FAILED') THEN 1 ELSE 0 END) AS failed_executions
            FROM summary_records
            GROUP BY task_name
        ),
        all_tasks AS (
            SELECT task_name FROM detail_stats
            UNION
            SELECT task_name FROM summary_stats
        )
        SELECT
            a.task_name,
            COALESCE(d.detail_executions, 0) + COALESCE(
                s.summary_executions - COALESCE(d.detail_executions, 0), 0
            ) AS executions,
            COALESCE(d.transferred, 0) AS transferred,
            COALESCE(d.verified, 0) AS verified,
            COALESCE(d.total_failed, 0) AS total_failed,
            COALESCE(d.skipped, 0) AS skipped,
            COALESCE(d.deleted, 0) AS deleted,
            COALESCE(d.total_records, 0) AS total_records,
            COALESCE(s.sum_files_transferred, 0) AS summary_files_transferred,
            COALESCE(s.sum_bytes_written, 0) AS summary_bytes_written,
            COALESCE(s.failed_executions, 0) AS failed_executions,
            CASE WHEN d.task_name IS NOT NULL THEN true ELSE false END AS has_details,
            CASE WHEN s.task_name IS NOT NULL THEN true ELSE false END AS has_summary
        FROM all_tasks a
        LEFT JOIN detail_stats d ON a.task_name = d.task_name
        LEFT JOIN summary_stats s ON a.task_name = s.task_name
        ORDER BY a.task_name
    """).fetchdf()


def purge_all(conn: duckdb.DuckDBPyConnection):
    conn.execute("DELETE FROM file_records")
    conn.execute("DELETE FROM summary_records")
    conn.execute("DELETE FROM api_tasks")
    conn.execute("DELETE FROM api_executions")
    conn.execute("DELETE FROM ingestion_log")
    conn.execute(
        "DELETE FROM kv_meta WHERE key = ?", [_WATERMARK_KEY]
    )


# ---------------------------------------------------------------------------
# API data ingestion (management plane)
# ---------------------------------------------------------------------------

def upsert_api_tasks(conn: duckdb.DuckDBPyConnection, tasks: list[TaskInfo]):
    now = datetime.now(config.IST)
    for t in tasks:
        conn.execute(
            "INSERT OR REPLACE INTO api_tasks "
            "(task_arn, task_id, name, status, source_location_arn, "
            "destination_location_arn, polled_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            [t.task_arn, t.task_id, t.name, t.status,
             t.source_location_arn, t.destination_location_arn, now],
        )


def upsert_api_executions(conn: duckdb.DuckDBPyConnection, execs: list[ExecutionInfo]):
    now = datetime.now(config.IST)
    for e in execs:
        conn.execute(
            "INSERT OR REPLACE INTO api_executions "
            "(execution_arn, execution_id, task_arn, status, start_time, end_time, "
            "bytes_written, bytes_transferred, bytes_compressed, "
            "files_transferred, files_verified, files_skipped, files_deleted, "
            "files_prepared, estimated_bytes_to_transfer, estimated_files_to_transfer, "
            "error_code, error_detail, prepare_duration, prepare_status, "
            "transfer_duration, transfer_status, verify_duration, verify_status, "
            "total_duration, polled_at) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            [e.execution_arn, e.execution_id, e.task_arn, e.status,
             e.start_time, e.end_time,
             e.bytes_written, e.bytes_transferred, e.bytes_compressed,
             e.files_transferred, e.files_verified, e.files_skipped,
             e.files_deleted, e.files_prepared,
             e.estimated_bytes_to_transfer, e.estimated_files_to_transfer,
             e.error_code, e.error_detail,
             e.prepare_duration, e.prepare_status,
             e.transfer_duration, e.transfer_status,
             e.verify_duration, e.verify_status,
             e.total_duration, now],
        )


def api_task_stats(conn: duckdb.DuckDBPyConnection) -> dict:
    row = conn.execute("""
        SELECT
            COUNT(*)                                                     AS total_tasks,
            SUM(CASE WHEN status = 'AVAILABLE' THEN 1 ELSE 0 END)       AS available,
            SUM(CASE WHEN status = 'RUNNING' THEN 1 ELSE 0 END)         AS running,
            SUM(CASE WHEN status = 'UNAVAILABLE' THEN 1 ELSE 0 END)     AS unavailable,
            MAX(polled_at)                                               AS last_polled
        FROM api_tasks
    """).fetchone()
    cols = ["total_tasks", "available", "running", "unavailable", "last_polled"]
    if not row or row[0] == 0:
        return {c: 0 for c in cols}
    return dict(zip(cols, row))


def api_execution_stats(conn: duckdb.DuckDBPyConnection) -> dict:
    row = conn.execute("""
        SELECT
            COUNT(*)                                                              AS total_executions,
            SUM(CASE WHEN status IN ('QUEUED','LAUNCHING')                 THEN 1 ELSE 0 END) AS queued,
            SUM(CASE WHEN status IN ('PREPARING')                          THEN 1 ELSE 0 END) AS preparing,
            SUM(CASE WHEN status IN ('TRANSFERRING')                       THEN 1 ELSE 0 END) AS transferring,
            SUM(CASE WHEN status IN ('VERIFYING')                          THEN 1 ELSE 0 END) AS verifying,
            SUM(CASE WHEN status IN ('SUCCESS')                            THEN 1 ELSE 0 END) AS succeeded,
            SUM(CASE WHEN status IN ('ERROR')                              THEN 1 ELSE 0 END) AS failed,
            SUM(CASE WHEN status IN ('QUEUED','LAUNCHING','PREPARING','TRANSFERRING','VERIFYING') THEN 1 ELSE 0 END) AS active,
            MAX(polled_at)                                                        AS last_polled
        FROM api_executions
    """).fetchone()
    cols = [
        "total_executions", "queued", "preparing", "transferring",
        "verifying", "succeeded", "failed", "active", "last_polled",
    ]
    if not row or row[0] == 0:
        return {c: 0 for c in cols}
    return dict(zip(cols, row))


def api_tasks_list(conn: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    return conn.execute("""
        SELECT
            t.name AS task_name,
            t.task_id,
            t.status AS task_status,
            COUNT(e.execution_arn) AS total_executions,
            SUM(CASE WHEN e.status IN ('QUEUED','LAUNCHING','PREPARING','TRANSFERRING','VERIFYING') THEN 1 ELSE 0 END) AS active_executions,
            SUM(CASE WHEN e.status = 'SUCCESS' THEN 1 ELSE 0 END) AS succeeded_executions,
            SUM(CASE WHEN e.status = 'ERROR' THEN 1 ELSE 0 END) AS failed_executions
        FROM api_tasks t
        LEFT JOIN api_executions e ON t.task_arn = e.task_arn
        GROUP BY t.name, t.task_id, t.status
        ORDER BY t.name
    """).fetchdf()


def api_executions_for_task(conn: duckdb.DuckDBPyConnection, task_id: str) -> pd.DataFrame:
    return conn.execute("""
        SELECT
            execution_id, status, start_time, end_time,
            bytes_written, bytes_transferred, bytes_compressed,
            files_transferred, files_verified, files_skipped, files_deleted,
            error_code, error_detail,
            prepare_status, transfer_status, verify_status,
            total_duration
        FROM api_executions
        WHERE task_arn LIKE '%/' || ?
        ORDER BY start_time DESC
    """, [task_id]).fetchdf()


# ---------------------------------------------------------------------------
# Unified KPIs — single source of truth
# ---------------------------------------------------------------------------

def unified_kpis(conn: duckdb.DuckDBPyConnection) -> dict:
    """Single source of truth for all KPIs.

    Priority: detailed reports > summary reports.
    For each execution:
      - If file_records exist → use them for file counts and byte totals
      - Else if summary_records exist → use summary for that execution
    Net data moved = SUM across all executions using the appropriate source.
    """
    row = conn.execute("""
        WITH exec_source AS (
            SELECT DISTINCT execution_id, 'detail' AS source
            FROM file_records
            WHERE report_type = 'transferred'
        ),
        detail_agg AS (
            SELECT
                COUNT(DISTINCT fr.task_name)                                          AS tasks,
                COUNT(DISTINCT fr.execution_id)                                       AS executions,
                SUM(CASE WHEN fr.report_type='transferred' THEN 1 ELSE 0 END)        AS files_transferred,
                SUM(CASE WHEN fr.report_type='transferred' AND fr.status='SUCCESS' THEN 1 ELSE 0 END) AS transfer_ok,
                SUM(CASE WHEN fr.report_type='transferred' AND fr.status='FAILED'  THEN 1 ELSE 0 END) AS transfer_failed,
                SUM(CASE WHEN fr.report_type='verified'    THEN 1 ELSE 0 END)        AS files_verified,
                SUM(CASE WHEN fr.report_type='verified' AND fr.status='SUCCESS' THEN 1 ELSE 0 END) AS verify_ok,
                SUM(CASE WHEN fr.report_type='verified' AND fr.status='FAILED'  THEN 1 ELSE 0 END) AS verify_failed,
                SUM(CASE WHEN fr.report_type='skipped'  THEN 1 ELSE 0 END)           AS files_skipped,
                SUM(CASE WHEN fr.report_type='deleted'  THEN 1 ELSE 0 END)           AS files_deleted,
                SUM(CASE WHEN fr.status='FAILED' THEN 1 ELSE 0 END)                  AS total_failed,
                SUM(CASE WHEN fr.report_type='transferred' THEN fr.content_size ELSE 0 END) AS bytes_moved
            FROM file_records fr
        ),
        summary_only AS (
            SELECT sr.*
            FROM summary_records sr
            LEFT JOIN exec_source es ON sr.execution_id = es.execution_id
            WHERE es.execution_id IS NULL
        ),
        summary_agg AS (
            SELECT
                COUNT(DISTINCT task_name)                    AS tasks,
                COUNT(DISTINCT execution_id)                 AS executions,
                SUM(COALESCE(files_transferred, 0))          AS files_transferred,
                SUM(COALESCE(files_verified, 0))             AS files_verified,
                SUM(COALESCE(files_skipped, 0))              AS files_skipped,
                SUM(COALESCE(files_deleted, 0))              AS files_deleted,
                SUM(COALESCE(bytes_written, 0))              AS bytes_moved,
                SUM(CASE WHEN overall_status IN ('ERROR','FAILED') THEN 1 ELSE 0 END) AS failed_executions,
                SUM(COALESCE(files_failed_transfer, 0))      AS transfer_failed,
                SUM(COALESCE(files_failed_verify, 0))        AS verify_failed
            FROM summary_only
        )
        SELECT
            (SELECT COUNT(DISTINCT task_name) FROM (
                SELECT task_name FROM file_records
                UNION ALL
                SELECT task_name FROM summary_only
            ))                                                      AS total_tasks,
            COALESCE(d.executions, 0) + COALESCE(s.executions, 0)   AS total_executions,
            COALESCE(d.files_transferred, 0) + COALESCE(s.files_transferred, 0) AS files_transferred,
            COALESCE(d.transfer_ok, 0)                               AS transfer_ok,
            COALESCE(d.transfer_failed, 0) + COALESCE(s.transfer_failed, 0) AS transfer_failed,
            COALESCE(d.files_verified, 0) + COALESCE(s.files_verified, 0)   AS files_verified,
            COALESCE(d.verify_ok, 0)                                 AS verify_ok,
            COALESCE(d.verify_failed, 0) + COALESCE(s.verify_failed, 0) AS verify_failed,
            COALESCE(d.files_skipped, 0) + COALESCE(s.files_skipped, 0) AS files_skipped,
            COALESCE(d.files_deleted, 0) + COALESCE(s.files_deleted, 0) AS files_deleted,
            COALESCE(d.total_failed, 0) + COALESCE(s.failed_executions, 0) AS total_failed,
            COALESCE(d.bytes_moved, 0) + COALESCE(s.bytes_moved, 0) AS net_bytes_moved,
            COALESCE(d.executions, 0)                               AS detail_executions,
            COALESCE(s.executions, 0)                               AS summary_only_executions,
            COALESCE(s.failed_executions, 0)                        AS failed_executions_summary
        FROM detail_agg d, summary_agg s
    """).fetchone()

    cols = [
        "total_tasks", "total_executions", "files_transferred",
        "transfer_ok", "transfer_failed", "files_verified",
        "verify_ok", "verify_failed", "files_skipped", "files_deleted",
        "total_failed", "net_bytes_moved",
        "detail_executions", "summary_only_executions", "failed_executions_summary",
    ]
    if not row:
        return {c: 0 for c in cols}
    return dict(zip(cols, row))


def unified_task_table(conn: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    """Per-task KPIs using detailed-first, summary-fallback logic."""
    return conn.execute("""
        WITH detail_tasks AS (
            SELECT DISTINCT execution_id FROM file_records WHERE report_type = 'transferred'
        ),
        detail_by_task AS (
            SELECT
                task_name,
                COUNT(DISTINCT execution_id) AS executions,
                SUM(CASE WHEN report_type='transferred' THEN 1 ELSE 0 END) AS files_transferred,
                SUM(CASE WHEN report_type='transferred' AND status='FAILED' THEN 1 ELSE 0 END) AS transfer_failed,
                SUM(CASE WHEN report_type='verified'    THEN 1 ELSE 0 END) AS files_verified,
                SUM(CASE WHEN report_type='verified' AND status='FAILED' THEN 1 ELSE 0 END) AS verify_failed,
                SUM(CASE WHEN report_type='skipped'  THEN 1 ELSE 0 END) AS files_skipped,
                SUM(CASE WHEN report_type='deleted'  THEN 1 ELSE 0 END) AS files_deleted,
                SUM(CASE WHEN status='FAILED' THEN 1 ELSE 0 END) AS total_failed,
                SUM(CASE WHEN report_type='transferred' THEN content_size ELSE 0 END) AS bytes_moved,
                MODE(task_mode) AS task_mode
            FROM file_records
            GROUP BY task_name
        ),
        summary_only_by_task AS (
            SELECT
                sr.task_name,
                COUNT(DISTINCT sr.execution_id) AS executions,
                SUM(COALESCE(sr.files_transferred, 0)) AS files_transferred,
                SUM(COALESCE(sr.files_failed_transfer, 0)) AS transfer_failed,
                SUM(COALESCE(sr.files_verified, 0)) AS files_verified,
                SUM(COALESCE(sr.files_failed_verify, 0)) AS verify_failed,
                SUM(COALESCE(sr.files_skipped, 0)) AS files_skipped,
                SUM(COALESCE(sr.files_deleted, 0)) AS files_deleted,
                SUM(COALESCE(sr.files_failed_transfer, 0) + COALESCE(sr.files_failed_verify, 0)) AS total_failed,
                SUM(COALESCE(sr.bytes_written, 0)) AS bytes_moved,
                'summary' AS task_mode
            FROM summary_records sr
            LEFT JOIN detail_tasks dt ON sr.execution_id = dt.execution_id
            WHERE dt.execution_id IS NULL
            GROUP BY sr.task_name
        ),
        all_tasks AS (
            SELECT task_name FROM detail_by_task
            UNION
            SELECT task_name FROM summary_only_by_task
        )
        SELECT
            a.task_name,
            COALESCE(d.executions, 0) + COALESCE(s.executions, 0) AS executions,
            COALESCE(d.files_transferred, 0) + COALESCE(s.files_transferred, 0) AS files_transferred,
            COALESCE(d.transfer_failed, 0) + COALESCE(s.transfer_failed, 0) AS transfer_failed,
            COALESCE(d.files_verified, 0) + COALESCE(s.files_verified, 0) AS files_verified,
            COALESCE(d.verify_failed, 0) + COALESCE(s.verify_failed, 0) AS verify_failed,
            COALESCE(d.files_skipped, 0) + COALESCE(s.files_skipped, 0) AS files_skipped,
            COALESCE(d.files_deleted, 0) + COALESCE(s.files_deleted, 0) AS files_deleted,
            COALESCE(d.total_failed, 0) + COALESCE(s.total_failed, 0) AS total_failed,
            COALESCE(d.bytes_moved, 0) + COALESCE(s.bytes_moved, 0) AS bytes_moved,
            CASE WHEN d.task_name IS NOT NULL THEN 'detailed' ELSE 'summary' END AS data_source,
            COALESCE(d.task_mode, s.task_mode, 'unknown') AS task_mode
        FROM all_tasks a
        LEFT JOIN detail_by_task d ON a.task_name = d.task_name
        LEFT JOIN summary_only_by_task s ON a.task_name = s.task_name
        ORDER BY a.task_name
    """).fetchdf()
