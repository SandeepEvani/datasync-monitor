"""Shared helpers for the DataSync monitoring dashboard."""

import config


def truncate_task_name(name: str, max_len: int | None = None) -> str:
    limit = max_len or config.TASK_NAME_MAX_LEN
    if len(name) <= limit:
        return name
    return name[: limit - 1] + "…"


def truncate_task_names(names, max_len: int | None = None):
    return [truncate_task_name(n, max_len) for n in names]


def format_bytes(b):
    if b is None or b == 0:
        return "0 B"
    for unit, threshold in [("TB", 1e12), ("GB", 1e9), ("MB", 1e6), ("KB", 1e3)]:
        if abs(b) >= threshold:
            return f"{b / threshold:.2f} {unit}"
    return f"{b:.0f} B"


def best_byte_unit(max_bytes):
    if max_bytes is None or max_bytes == 0:
        return "B", 1
    for unit, threshold in [("TB", 1e12), ("GB", 1e9), ("MB", 1e6), ("KB", 1e3)]:
        if max_bytes >= threshold:
            return unit, threshold
    return "B", 1
