"""DataSync management plane client — polls task and execution state via boto3."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime

import boto3

import config


@dataclass
class TaskInfo:
    task_arn: str
    task_id: str
    name: str
    status: str
    source_location_arn: str | None = None
    destination_location_arn: str | None = None
    cloud_watch_log_group_arn: str | None = None
    options: dict = field(default_factory=dict)
    created_at: datetime | None = None


@dataclass
class ExecutionInfo:
    execution_arn: str
    execution_id: str
    task_arn: str
    status: str
    start_time: datetime | None = None
    end_time: datetime | None = None
    bytes_written: int | None = None
    bytes_transferred: int | None = None
    bytes_compressed: int | None = None
    files_transferred: int | None = None
    files_verified: int | None = None
    files_skipped: int | None = None
    files_deleted: int | None = None
    files_prepared: int | None = None
    estimated_bytes_to_transfer: int | None = None
    estimated_files_to_transfer: int | None = None
    error_code: str | None = None
    error_detail: str | None = None
    prepare_duration: int | None = None
    prepare_status: str | None = None
    transfer_duration: int | None = None
    transfer_status: str | None = None
    verify_duration: int | None = None
    verify_status: str | None = None
    total_duration: int | None = None


def _get_datasync_client():
    if config.AWS_PROFILE:
        session = boto3.Session(profile_name=config.AWS_PROFILE, region_name=config.AWS_REGION)
    else:
        session = boto3.Session(region_name=config.AWS_REGION)
    return session.client("datasync")


def _extract_task_id(task_arn: str) -> str:
    return task_arn.rsplit("/", 1)[-1] if "/" in task_arn else task_arn


def _extract_exec_id(exec_arn: str) -> str:
    return exec_arn.rsplit("/", 1)[-1] if "/" in exec_arn else exec_arn


def _safe_int(val) -> int | None:
    if val is None:
        return None
    try:
        return int(val)
    except (ValueError, TypeError):
        return None


def list_tasks() -> list[TaskInfo]:
    client = _get_datasync_client()
    tasks: list[TaskInfo] = []
    paginator = client.get_paginator("list_tasks")

    for page in paginator.paginate():
        for t in page.get("Tasks", []):
            task_arn = t["TaskArn"]
            tasks.append(TaskInfo(
                task_arn=task_arn,
                task_id=_extract_task_id(task_arn),
                name=t.get("Name", _extract_task_id(task_arn)),
                status=t.get("Status", "UNKNOWN"),
            ))

    return tasks


def describe_task(task_arn: str) -> TaskInfo:
    client = _get_datasync_client()
    r = client.describe_task(TaskArn=task_arn)
    return TaskInfo(
        task_arn=task_arn,
        task_id=_extract_task_id(task_arn),
        name=r.get("Name", _extract_task_id(task_arn)),
        status=r.get("Status", "UNKNOWN"),
        source_location_arn=r.get("SourceLocationArn"),
        destination_location_arn=r.get("DestinationLocationArn"),
        cloud_watch_log_group_arn=r.get("CloudWatchLogGroupArn"),
        options=r.get("Options", {}),
        created_at=r.get("CreationTime"),
    )


def list_task_executions(task_arn: str) -> list[ExecutionInfo]:
    client = _get_datasync_client()
    execs: list[ExecutionInfo] = []
    paginator = client.get_paginator("list_task_executions")

    for page in paginator.paginate(TaskArn=task_arn):
        for e in page.get("TaskExecutions", []):
            exec_arn = e["TaskExecutionArn"]
            execs.append(ExecutionInfo(
                execution_arn=exec_arn,
                execution_id=_extract_exec_id(exec_arn),
                task_arn=task_arn,
                status=e.get("Status", "UNKNOWN"),
            ))

    return execs


def describe_task_execution(exec_arn: str) -> ExecutionInfo:
    client = _get_datasync_client()
    r = client.describe_task_execution(TaskExecutionArn=exec_arn)

    result = r.get("Result", {}) or {}
    task_arn = r.get("TaskArn", "")

    return ExecutionInfo(
        execution_arn=exec_arn,
        execution_id=_extract_exec_id(exec_arn),
        task_arn=task_arn,
        status=r.get("Status", "UNKNOWN"),
        start_time=r.get("StartTime"),
        end_time=r.get("EndTime"),
        bytes_written=_safe_int(result.get("BytesWritten")),
        bytes_transferred=_safe_int(result.get("BytesTransferred")),
        bytes_compressed=_safe_int(result.get("BytesCompressed")),
        files_transferred=_safe_int(result.get("FilesTransferred")),
        files_verified=_safe_int(result.get("FilesVerified")),
        files_skipped=_safe_int(result.get("FilesSkipped")),
        files_deleted=_safe_int(result.get("FilesDeleted")),
        files_prepared=_safe_int(result.get("FilesPrepared")),
        estimated_bytes_to_transfer=_safe_int(result.get("EstimatedBytesToTransfer")),
        estimated_files_to_transfer=_safe_int(result.get("EstimatedFilesToTransfer")),
        error_code=result.get("ErrorCode"),
        error_detail=result.get("ErrorDetail"),
        prepare_duration=_safe_int(result.get("PrepareDuration")),
        prepare_status=result.get("PrepareStatus"),
        transfer_duration=_safe_int(result.get("TransferDuration")),
        transfer_status=result.get("TransferStatus"),
        verify_duration=_safe_int(result.get("VerifyDuration")),
        verify_status=result.get("VerifyStatus"),
        total_duration=_safe_int(result.get("TotalDuration")),
    )


def poll_all_executions(
    tasks: list[TaskInfo] | None = None,
    progress_callback=None,
) -> tuple[list[TaskInfo], list[ExecutionInfo]]:
    """Fetch all tasks and their executions from the DataSync API.

    Returns (tasks, executions).
    """
    if tasks is None:
        tasks = list_tasks()

    all_execs: list[ExecutionInfo] = []
    total = len(tasks)

    for i, task in enumerate(tasks):
        try:
            execs = list_task_executions(task.task_arn)
            all_execs.extend(execs)
        except Exception:
            pass
        if progress_callback:
            progress_callback(i + 1, total)

    return tasks, all_execs


# Execution status categories
ACTIVE_STATUSES = {"QUEUED", "LAUNCHING", "PREPARING", "TRANSFERRING", "VERIFYING"}
SUCCESS_STATUSES = {"SUCCESS"}
FAILED_STATUSES = {"ERROR"}
