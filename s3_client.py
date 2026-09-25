"""S3 operations for discovering and reading DataSync JSON task reports.

Supports both Enhanced mode and Basic mode report schemas automatically.
Reports are JSON files organized as:
  {prefix}/{task_name}/Detailed-Reports/{task-id}/{exec-id}/*.json

Each JSON contains TaskExecutionId and one report array keyed by:
  Transferred | Skipped | Verified | Deleted

Uses asyncio + aiobotocore for concurrent S3 downloads and timestamp-based
incremental listing.
"""

from __future__ import annotations

import asyncio
import json
import re
from dataclasses import dataclass
from datetime import datetime
from typing import Callable

import aiobotocore.session
import boto3
from botocore.config import Config as BotoConfig

import config


@dataclass
class ReportKey:
    key: str
    task_name: str
    task_id: str
    exec_id: str
    last_modified: datetime
    report_category: str = "detailed"


@dataclass
class NormalizedRecord:
    task_name: str
    task_id: str
    execution_id: str
    report_type: str
    task_mode: str
    relative_path: str
    content_size: int | None
    item_type: str | None
    status: str | None
    transfer_type: str | None
    event_timestamp: str | None
    overwrite: str | None
    error_code: str | None
    error_detail: str | None
    src_metadata_json: str | None
    s3_key: str


@dataclass
class SummaryRecord:
    task_name: str
    task_id: str
    execution_id: str
    task_mode: str | None
    overall_status: str | None
    start_time: str | None
    end_time: str | None
    total_time: str | None
    files_transferred: int | None
    files_verified: int | None
    files_skipped: int | None
    files_deleted: int | None
    bytes_written: int | None
    bytes_transferred: int | None
    bytes_compressed: int | None
    prepare_duration: str | None
    prepare_status: str | None
    transfer_duration: str | None
    transfer_status: str | None
    verify_duration: str | None
    verify_status: str | None
    error_code: str | None
    error_detail: str | None
    files_failed_prepare: int | None
    files_failed_transfer: int | None
    files_failed_verify: int | None
    files_failed_delete: int | None
    files_prepared: int | None
    estimated_bytes_to_transfer: int | None
    estimated_files_to_transfer: int | None
    source_location_type: str | None
    destination_location_type: str | None
    s3_key: str


_DETAILED_PATTERN = re.compile(
    r"^(?P<prefix>.+?)/(?P<task_name>[^/]+)"
    r"/Detailed-Reports"
    r"/(?P<task_id>task-[0-9a-f]+)"
    r"/(?P<exec_id>exec-[0-9a-f]+)"
    r"/(?P<filename>.+\.json)$"
)

_SUMMARY_PATTERN = re.compile(
    r"^(?P<prefix>.+?)/(?P<task_name>[^/]+)"
    r"/Summary-Reports"
    r"/(?P<task_id>task-[0-9a-f]+)"
    r"/(?P<exec_id>exec-[0-9a-f]+)"
    r"/(?P<filename>.+\.json)$"
)

_MAX_CONCURRENT = 32


# ---------------------------------------------------------------------------
# S3 listing (synchronous boto3 — paginator support is better here)
# ---------------------------------------------------------------------------

def _get_s3_client():
    if config.AWS_PROFILE:
        session = boto3.Session(profile_name=config.AWS_PROFILE, region_name=config.AWS_REGION)
    else:
        session = boto3.Session(region_name=config.AWS_REGION)
    return session.client("s3", config=BotoConfig(max_pool_connections=25))


def list_report_keys(
    bucket: str,
    prefix: str,
    since: datetime | None = None,
) -> list[ReportKey]:
    """List Detailed-Reports and Summary-Reports JSON files, optionally only those modified after `since`."""
    client = _get_s3_client()
    keys: list[ReportKey] = []
    paginator = client.get_paginator("list_objects_v2")

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            s3_key = obj["Key"]
            last_mod = obj["LastModified"]

            if since and last_mod <= since:
                continue

            m = _DETAILED_PATTERN.match(s3_key)
            if m:
                keys.append(ReportKey(
                    key=s3_key,
                    task_name=m.group("task_name"),
                    task_id=m.group("task_id"),
                    exec_id=m.group("exec_id"),
                    last_modified=last_mod,
                    report_category="detailed",
                ))
                continue

            m = _SUMMARY_PATTERN.match(s3_key)
            if m:
                keys.append(ReportKey(
                    key=s3_key,
                    task_name=m.group("task_name"),
                    task_id=m.group("task_id"),
                    exec_id=m.group("exec_id"),
                    last_modified=last_mod,
                    report_category="summary",
                ))

    return keys


# ---------------------------------------------------------------------------
# JSON parsing & normalization
# ---------------------------------------------------------------------------

def _find_field(record: dict, *candidates: str) -> str | None:
    for key in candidates:
        if key in record:
            val = record[key]
            return str(val) if val is not None else None
    return None


def _normalize_item(
    item: dict,
    report_type: str,
    rk: ReportKey,
    execution_id: str,
) -> NormalizedRecord:
    if "SourceMetadata" in item:
        mode, metadata = "enhanced", item["SourceMetadata"]
    elif "SrcMetadata" in item:
        mode, metadata = "basic", item["SrcMetadata"]
    else:
        mode, metadata = "unknown", {}

    error_code = _find_field(item, "ErrorCode") or _find_field(item, "FailureCode")
    error_detail = (
        _find_field(item, "ErrorDetail")
        or _find_field(item, "FailureReason")
        or _find_field(item, "SkipReason")
    )

    return NormalizedRecord(
        task_name=rk.task_name,
        task_id=rk.task_id,
        execution_id=execution_id,
        report_type=report_type,
        task_mode=mode,
        relative_path=item.get("RelativePath", ""),
        content_size=metadata.get("ContentSize"),
        item_type=metadata.get("Type"),
        status=_find_field(
            item, "TransferStatus", "VerifyStatus",
            "SkipStatus", "DeleteStatus", "Status",
        ),
        transfer_type=item.get("TransferType"),
        event_timestamp=_find_field(
            item, "TransferTimestamp", "VerifyTimestamp",
            "SkipTimestamp", "DeleteTimestamp", "Timestamp",
        ),
        overwrite=str(item["Overwrite"]) if "Overwrite" in item else None,
        error_code=error_code,
        error_detail=error_detail,
        src_metadata_json=json.dumps(metadata) if metadata else None,
        s3_key=rk.key,
    )


def _parse_report_json(raw: bytes, rk: ReportKey) -> list[NormalizedRecord]:
    try:
        data = json.loads(raw)
    except (json.JSONDecodeError, UnicodeDecodeError):
        return []

    if not isinstance(data, dict):
        return []

    exec_id = data.get("TaskExecutionId", rk.exec_id)
    records: list[NormalizedRecord] = []

    for report_key in config.REPORT_TYPE_KEYS:
        items = data.get(report_key)
        if not items or not isinstance(items, list):
            continue
        for item in items:
            if isinstance(item, dict):
                records.append(_normalize_item(item, report_key.lower(), rk, exec_id))

    return records


def _safe_int(val) -> int | None:
    if val is None:
        return None
    try:
        return int(val)
    except (ValueError, TypeError):
        return None


def _parse_summary_json(raw: bytes, rk: ReportKey) -> SummaryRecord | None:
    try:
        data = json.loads(raw)
    except (json.JSONDecodeError, UnicodeDecodeError):
        return None

    if not isinstance(data, dict):
        return None

    exec_id = data.get("TaskExecutionId", rk.exec_id)
    result = data.get("Result", data.get("result", {})) or {}
    files_failed = result.get("FilesFailed", {}) or {}

    src_loc = data.get("SourceLocation", data.get("sourcelocation", {})) or {}
    dst_loc = data.get("DestinationLocation", data.get("destinationlocation", {})) or {}

    return SummaryRecord(
        task_name=rk.task_name,
        task_id=rk.task_id,
        execution_id=exec_id,
        task_mode=data.get("TaskMode"),
        overall_status=data.get("OverallStatus", data.get("overallstatus")),
        start_time=data.get("StartTime", data.get("starttime")),
        end_time=data.get("EndTime", data.get("endtime")),
        total_time=data.get("TotalTime", data.get("totaltime")),
        files_transferred=_safe_int(result.get("FilesTransferred", result.get("filestransferred"))),
        files_verified=_safe_int(result.get("FilesVerified", result.get("filesverified"))),
        files_skipped=_safe_int(result.get("FilesSkipped", result.get("filesskipped"))),
        files_deleted=_safe_int(result.get("FilesDeleted", result.get("filesdeleted"))),
        bytes_written=_safe_int(result.get("BytesWritten", result.get("byteswritten"))),
        bytes_transferred=_safe_int(result.get("BytesTransferred", result.get("bytestransferred"))),
        bytes_compressed=_safe_int(result.get("BytesCompressed", result.get("bytescompressed"))),
        prepare_duration=result.get("PrepareDuration", result.get("prepareduration")),
        prepare_status=result.get("PrepareStatus", result.get("preparestatus")),
        transfer_duration=result.get("TransferDuration", result.get("transferduration")),
        transfer_status=result.get("TransferStatus", result.get("transferstatus")),
        verify_duration=result.get("VerifyDuration", result.get("verifyduration")),
        verify_status=result.get("VerifyStatus", result.get("verifystatus")),
        error_code=result.get("ErrorCode", result.get("errorcode")),
        error_detail=result.get("ErrorDetail", result.get("errordetail")),
        files_failed_prepare=_safe_int(files_failed.get("Prepare")),
        files_failed_transfer=_safe_int(files_failed.get("Transfer")),
        files_failed_verify=_safe_int(files_failed.get("Verify")),
        files_failed_delete=_safe_int(files_failed.get("Delete")),
        files_prepared=_safe_int(result.get("FilesPrepared", result.get("filesprepared"))),
        estimated_bytes_to_transfer=_safe_int(result.get("EstimatedBytesToTransfer")),
        estimated_files_to_transfer=_safe_int(result.get("EstimatedFilesToTransfer")),
        source_location_type=src_loc.get("LocationType", src_loc.get("locationtype")),
        destination_location_type=dst_loc.get("LocationType", dst_loc.get("locationtype")),
        s3_key=rk.key,
    )


# ---------------------------------------------------------------------------
# Async download + parse via aiobotocore
# ---------------------------------------------------------------------------

async def _download_one(
    client,
    bucket: str,
    rk: ReportKey,
) -> tuple[ReportKey, list[NormalizedRecord] | SummaryRecord | None]:
    response = await client.get_object(Bucket=bucket, Key=rk.key)
    async with response["Body"] as stream:
        raw = await stream.read()
    if rk.report_category == "summary":
        return rk, _parse_summary_json(raw, rk)
    return rk, _parse_report_json(raw, rk)


async def _download_chunk(
    client,
    bucket: str,
    chunk: list[ReportKey],
    max_concurrent: int,
    progress_callback: Callable[[int, int], None] | None,
    global_offset: int,
    global_total: int,
) -> list[tuple[ReportKey, list[NormalizedRecord] | SummaryRecord | None]]:
    sem = asyncio.Semaphore(max_concurrent)
    completed = 0

    async def _bounded(rk: ReportKey):
        nonlocal completed
        async with sem:
            try:
                result = await _download_one(client, bucket, rk)
            except Exception:
                result = (rk, [] if rk.report_category == "detailed" else None)
            completed += 1
            if progress_callback:
                progress_callback(global_offset + completed, global_total)
            return result

    tasks = [asyncio.ensure_future(_bounded(rk)) for rk in chunk]
    return list(await asyncio.gather(*tasks))


_CHUNK_SIZE = 500


async def _download_and_ingest_chunked(
    bucket: str,
    report_keys: list[ReportKey],
    max_concurrent: int,
    chunk_size: int,
    progress_callback: Callable[[int, int], None] | None,
    ingest_callback: Callable[
        [list[tuple[ReportKey, "list[NormalizedRecord] | SummaryRecord | None"]]],
        tuple[int, int],
    ] | None,
) -> tuple[int, int]:
    """Download in chunks and call ingest_callback after each chunk.

    Returns (total_detail_records, total_summary_records).
    """
    session = aiobotocore.session.get_session()
    if config.AWS_PROFILE:
        session.set_config_variable("profile", config.AWS_PROFILE)

    total = len(report_keys)
    total_details = 0
    total_summaries = 0

    async with session.create_client("s3", region_name=config.AWS_REGION) as client:
        for i in range(0, total, chunk_size):
            chunk = report_keys[i : i + chunk_size]
            results = await _download_chunk(
                client, bucket, chunk, max_concurrent,
                progress_callback, i, total,
            )
            if ingest_callback:
                d, s = ingest_callback(results)
                total_details += d
                total_summaries += s

    return total_details, total_summaries


def download_and_ingest_streaming(
    bucket: str,
    report_keys: list[ReportKey],
    max_concurrent: int = _MAX_CONCURRENT,
    chunk_size: int = _CHUNK_SIZE,
    progress_callback: Callable[[int, int], None] | None = None,
    ingest_callback: Callable | None = None,
) -> tuple[int, int]:
    """Download and ingest report files in streaming chunks.

    Downloads `chunk_size` files concurrently, calls `ingest_callback` with the
    parsed results for immediate DB insertion, then moves to the next chunk.
    This keeps memory flat and overlaps download with ingestion.

    ingest_callback receives list[(ReportKey, parsed_data)] and should return
    (detail_record_count, summary_record_count).

    Returns (total_detail_records, total_summary_records).
    """
    if not report_keys:
        return 0, 0
    return asyncio.run(
        _download_and_ingest_chunked(
            bucket, report_keys, max_concurrent, chunk_size,
            progress_callback, ingest_callback,
        )
    )


def download_and_parse_async(
    bucket: str,
    report_keys: list[ReportKey],
    max_concurrent: int = _MAX_CONCURRENT,
    progress_callback: Callable[[int, int], None] | None = None,
) -> list[tuple[ReportKey, list[NormalizedRecord] | SummaryRecord | None]]:
    """Download and parse all report files. Kept for backward compatibility.

    For large ingestions, prefer download_and_ingest_streaming().
    """
    if not report_keys:
        return []
    all_results: list[tuple[ReportKey, list[NormalizedRecord] | SummaryRecord | None]] = []

    def _collect(results):
        all_results.extend(results)
        return 0, 0

    asyncio.run(
        _download_and_ingest_chunked(
            bucket, report_keys, max_concurrent, _CHUNK_SIZE,
            progress_callback, _collect,
        )
    )
    return all_results
