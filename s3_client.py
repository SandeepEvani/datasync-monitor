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


_DETAILED_PATTERN = re.compile(
    r"^(?P<prefix>.+?)/(?P<task_name>[^/]+)"
    r"/Detailed-Reports"
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
    """List Detailed-Reports JSON files, optionally only those modified after `since`."""
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
            if not m:
                continue

            keys.append(ReportKey(
                key=s3_key,
                task_name=m.group("task_name"),
                task_id=m.group("task_id"),
                exec_id=m.group("exec_id"),
                last_modified=last_mod,
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


# ---------------------------------------------------------------------------
# Async download + parse via aiobotocore
# ---------------------------------------------------------------------------

async def _download_one(
    client,
    bucket: str,
    rk: ReportKey,
) -> tuple[ReportKey, list[NormalizedRecord]]:
    response = await client.get_object(Bucket=bucket, Key=rk.key)
    async with response["Body"] as stream:
        raw = await stream.read()
    return rk, _parse_report_json(raw, rk)


async def _download_all(
    bucket: str,
    report_keys: list[ReportKey],
    max_concurrent: int,
    progress_callback: Callable[[int, int], None] | None,
) -> list[tuple[ReportKey, list[NormalizedRecord]]]:
    session = aiobotocore.session.get_session()
    if config.AWS_PROFILE:
        session.set_config_variable("profile", config.AWS_PROFILE)

    sem = asyncio.Semaphore(max_concurrent)
    total = len(report_keys)
    completed = 0
    results: list[tuple[ReportKey, list[NormalizedRecord]]] = []

    async with session.create_client("s3", region_name=config.AWS_REGION) as client:

        async def _bounded(rk: ReportKey):
            nonlocal completed
            async with sem:
                try:
                    result = await _download_one(client, bucket, rk)
                except Exception:
                    result = (rk, [])
                completed += 1
                if progress_callback:
                    progress_callback(completed, total)
                return result

        tasks = [asyncio.ensure_future(_bounded(rk)) for rk in report_keys]
        results = await asyncio.gather(*tasks)

    return list(results)


def download_and_parse_async(
    bucket: str,
    report_keys: list[ReportKey],
    max_concurrent: int = _MAX_CONCURRENT,
    progress_callback: Callable[[int, int], None] | None = None,
) -> list[tuple[ReportKey, list[NormalizedRecord]]]:
    """Download and parse report files concurrently using asyncio + aiobotocore.

    Returns list of (report_key, records).
    Calls progress_callback(completed, total) after each file if provided.
    """
    if not report_keys:
        return []
    return asyncio.run(
        _download_all(bucket, report_keys, max_concurrent, progress_callback)
    )
