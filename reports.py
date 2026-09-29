"""Excel report generation for DataSync monitoring dashboard."""

from __future__ import annotations

from io import BytesIO
from datetime import date

import pandas as pd
from openpyxl.utils import get_column_letter

from db import execution_summaries, file_detail


_BYTES_TO_MB = 1 / (1024 * 1024)
_BYTES_TO_GB = 1 / (1024 ** 3)
_BYTES_TO_TB = 1 / (1024 ** 4)


def _add_byte_conversions(df: pd.DataFrame, raw_col: str, label: str) -> None:
    idx = df.columns.get_loc(raw_col) + 1
    for suffix, factor in [("MB", _BYTES_TO_MB), ("GB", _BYTES_TO_GB), ("TB", _BYTES_TO_TB)]:
        col_name = f"{label} ({suffix})"
        df.insert(idx, col_name, pd.to_numeric(df[raw_col], errors="coerce").fillna(0) * factor)
        df[col_name] = df[col_name].round(3)
        idx += 1


def _build_summary_df(conn, task_name: str | None = None) -> pd.DataFrame:
    df = execution_summaries(conn, task_name=task_name)
    if df.empty:
        return df

    cols_to_drop = [c for c in ["bytes_transferred"] if c in df.columns]
    if cols_to_drop:
        df = df.drop(columns=cols_to_drop)

    rename = {
        "task_name": "Task Name",
        "task_id": "Task ID",
        "execution_id": "Execution ID",
        "task_mode": "Task Mode",
        "overall_status": "Status",
        "start_time": "Start Time",
        "end_time": "End Time",
        "total_time": "Total Time",
        "files_transferred": "Files Transferred",
        "files_verified": "Files Verified",
        "files_skipped": "Files Skipped",
        "files_deleted": "Files Deleted",
        "bytes_written": "Bytes Written",
        "bytes_compressed": "Bytes Compressed",
        "prepare_duration": "Prepare Duration",
        "prepare_status": "Prepare Status",
        "transfer_duration": "Transfer Duration",
        "transfer_status": "Transfer Status",
        "verify_duration": "Verify Duration",
        "verify_status": "Verify Status",
        "error_code": "Error Code",
        "error_detail": "Error Detail",
        "files_failed_prepare": "Files Failed Prepare",
        "files_failed_transfer": "Files Failed Transfer",
        "files_failed_verify": "Files Failed Verify",
        "files_failed_delete": "Files Failed Delete",
        "files_prepared": "Files Prepared",
        "estimated_bytes_to_transfer": "Est. Bytes to Transfer",
        "estimated_files_to_transfer": "Est. Files to Transfer",
        "source_location_type": "Source Location",
        "destination_location_type": "Dest Location",
    }
    df = df.rename(columns={k: v for k, v in rename.items() if k in df.columns})

    if "Bytes Written" in df.columns:
        _add_byte_conversions(df, "Bytes Written", "Written")
    if "Bytes Compressed" in df.columns:
        _add_byte_conversions(df, "Bytes Compressed", "Compressed")
    if "Est. Bytes to Transfer" in df.columns:
        _add_byte_conversions(df, "Est. Bytes to Transfer", "Est. Transfer")

    return df


def _build_detail_df(conn, task_name: str) -> pd.DataFrame:
    df = file_detail(conn, task_name=task_name, limit=1_000_000, offset=0)
    if df.empty:
        return df

    rename = {
        "task_name": "Task Name",
        "execution_id": "Execution ID",
        "report_type": "Report Type",
        "task_mode": "Task Mode",
        "relative_path": "Relative Path",
        "content_size": "Content Size",
        "item_type": "Item Type",
        "status": "Status",
        "transfer_type": "Transfer Type",
        "event_timestamp": "Event Timestamp",
        "error_code": "Error Code",
        "error_detail": "Error Detail",
    }
    df = df.rename(columns={k: v for k, v in rename.items() if k in df.columns})

    if "Content Size" in df.columns:
        _add_byte_conversions(df, "Content Size", "Size")

    return df


def _auto_width(ws) -> None:
    for col_idx, col_cells in enumerate(ws.iter_cols(min_row=1, max_row=min(ws.max_row, 100)), 1):
        max_len = 0
        for cell in col_cells:
            val = str(cell.value) if cell.value is not None else ""
            max_len = max(max_len, len(val))
        ws.column_dimensions[get_column_letter(col_idx)].width = min(max_len + 3, 40)


def _truncate_sheet_name(name: str) -> str:
    if len(name) <= 31:
        return name
    return name[:28] + "..."


def generate_all_tasks_report(conn) -> tuple[bytes, str]:
    df = _build_summary_df(conn)
    buf = BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        sheet_name = "All Tasks - Summary"
        if df.empty:
            pd.DataFrame({"Info": ["No summary data available"]}).to_excel(
                writer, sheet_name=sheet_name, index=False
            )
        else:
            df.to_excel(writer, sheet_name=sheet_name, index=False)
        _auto_width(writer.sheets[sheet_name])

    filename = f"all_tasks_report_{date.today().isoformat()}.xlsx"
    return buf.getvalue(), filename


def generate_task_report(conn, task_name: str) -> tuple[bytes, str]:
    df_summary = _build_summary_df(conn, task_name=task_name)
    df_detail = _build_detail_df(conn, task_name)

    buf = BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        summary_sheet = _truncate_sheet_name(f"{task_name} - Summary")
        if df_summary.empty:
            pd.DataFrame({"Info": ["No summary data available"]}).to_excel(
                writer, sheet_name=summary_sheet, index=False
            )
        else:
            df_summary.to_excel(writer, sheet_name=summary_sheet, index=False)
        _auto_width(writer.sheets[summary_sheet])

        detail_sheet = _truncate_sheet_name(f"{task_name} - Detailed")
        if df_detail.empty:
            pd.DataFrame({"Info": ["No detailed file records available"]}).to_excel(
                writer, sheet_name=detail_sheet, index=False
            )
        else:
            df_detail.to_excel(writer, sheet_name=detail_sheet, index=False)
        _auto_width(writer.sheets[detail_sheet])

    safe_name = task_name.replace("/", "_").replace("\\", "_")
    filename = f"{safe_name}_report_{date.today().isoformat()}.xlsx"
    return buf.getvalue(), filename
