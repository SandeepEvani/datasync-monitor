"""Task Detail — per-task drill-down with execution history and file breakdown."""

import pandas as pd
import streamlit as st
import plotly.graph_objects as go

from db import (
    get_connection, task_summary, execution_summary,
    file_detail, file_detail_count, error_code_distribution,
    execution_summaries, task_summary_combined,
)
from utils import format_bytes

st.set_page_config(page_title="Task Detail | DataSync Monitor", page_icon="🔍", layout="wide")

C_TRANSFERRED = "#6C9EFF"
C_VERIFIED    = "#4ADE80"
C_FAILED      = "#F87171"
C_SKIPPED     = "#FBBF24"
C_DELETED     = "#A78BFA"

conn = get_connection()
df_tasks = task_summary(conn)
df_tasks_combined = task_summary_combined(conn)

if df_tasks.empty and df_tasks_combined.empty:
    st.info("No data available. Ingest reports first.")
    st.stop()

st.markdown("## Task Detail")

task_list = df_tasks_combined["task_name"].tolist() if not df_tasks_combined.empty else df_tasks["task_name"].tolist()
search_term = st.text_input("Search tasks", placeholder="Type to filter...")
if search_term:
    task_list = [t for t in task_list if search_term.lower() in t.lower()]

if not task_list:
    st.warning("No tasks match your search.")
    st.stop()

selected_task = st.selectbox("Select a task", task_list, index=0)

# ── task-level KPIs ──────────────────────────────────────────────────────────
has_detail = not df_tasks.empty and selected_task in df_tasks["task_name"].values

if has_detail:
    task_row = df_tasks[df_tasks["task_name"] == selected_task].iloc[0]
    st.divider()
    c1, c2, c3, c4, c5, c6 = st.columns(6)
    c1.metric("Executions", int(task_row["executions"]))
    c2.metric("Files Transferred", f"{int(task_row['transferred']):,}")
    c3.metric("Files Verified", f"{int(task_row['verified']):,}")
    c4.metric("Total Failures", f"{int(task_row['total_failed']):,}",
              delta=None if task_row["total_failed"] == 0 else f"{int(task_row['total_failed']):,}",
              delta_color="inverse")
    c5.metric("Files Skipped", f"{int(task_row['skipped']):,}")
    c6.metric("Files Deleted", f"{int(task_row['deleted']):,}")

    ver_rate = (
        task_row["verified"] / task_row["transferred"] * 100
        if task_row["transferred"] > 0 else 0
    )
    st.progress(min(ver_rate / 100, 1.0), text=f"Verification Coverage: {ver_rate:.1f}%")
else:
    df_sum = execution_summaries(conn, task_name=selected_task)
    st.divider()
    st.info("This task has summary reports only — no file-level detailed reports were generated.")
    if not df_sum.empty:
        c1, c2, c3, c4, c5 = st.columns(5)
        c1.metric("Executions", len(df_sum))
        c2.metric("Files Transferred (summary)", f"{int(df_sum['files_transferred'].sum()):,}")
        c3.metric("Files Verified (summary)", f"{int(df_sum['files_verified'].sum()):,}")
        c4.metric("Bytes Written", format_bytes(df_sum["bytes_written"].sum()))
        c5.metric("Bytes Transferred", format_bytes(df_sum["bytes_transferred"].sum()))

st.divider()

# ── execution breakdown ─────────────────────────────────────────────────────
st.markdown("##### Execution History")
df_exec = execution_summary(conn, selected_task) if has_detail else pd.DataFrame()

if not df_exec.empty and len(df_exec) > 1:
    fig_exec = go.Figure()
    for col_name, label, color in [
        ("transferred", "Files Transferred", C_TRANSFERRED),
        ("verified", "Files Verified", C_VERIFIED),
        ("transfer_failed", "Transfer Failures", C_FAILED),
        ("skipped", "Files Skipped", C_SKIPPED),
        ("deleted", "Files Deleted", C_DELETED),
    ]:
        fig_exec.add_trace(go.Bar(
            x=df_exec["execution_id"], y=df_exec[col_name],
            name=label, marker_color=color,
        ))
    fig_exec.update_layout(
        barmode="group",
        xaxis_title="Execution",
        yaxis_title="Record count",
        legend=dict(orientation="h", y=1.1, x=0.5, xanchor="center"),
        margin=dict(t=40, b=60, l=60, r=20),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        height=350,
    )
    st.plotly_chart(fig_exec, use_container_width=True)

if not df_exec.empty:
    display_cols = ["execution_id", "task_mode", "transferred", "verified",
                    "transfer_failed", "skipped", "deleted", "total_records",
                    "first_event", "last_event"]
    st.dataframe(df_exec[[c for c in display_cols if c in df_exec.columns]], use_container_width=True)

# ── summary report details per execution ─────────────────────────────────────
df_summaries = execution_summaries(conn, task_name=selected_task)
if not df_summaries.empty:
    st.divider()
    st.markdown("##### Execution Summary Reports")
    st.caption("Data from AWS DataSync summary reports — includes executions without detailed file-level reports.")

    for _, srow in df_summaries.iterrows():
        exec_id = srow["execution_id"]
        status = srow.get("overall_status", "UNKNOWN")
        status_icon = {"COMPLETED": "✅", "SUCCESS": "✅", "ERROR": "❌", "FAILED": "❌"}.get(
            status, "⚠️"
        )

        with st.expander(f"{status_icon} {exec_id} — {status}", expanded=(status in ("ERROR", "FAILED"))):
            sc1, sc2, sc3, sc4 = st.columns(4)
            sc1.metric("Files Transferred", f"{int(srow.get('files_transferred') or 0):,}")
            sc2.metric("Files Verified", f"{int(srow.get('files_verified') or 0):,}")
            sc3.metric("Files Skipped", f"{int(srow.get('files_skipped') or 0):,}")
            sc4.metric("Files Deleted", f"{int(srow.get('files_deleted') or 0):,}")

            sb1, sb2, sb3, sb4 = st.columns(4)
            sb1.metric("Bytes Written", format_bytes(srow.get("bytes_written")))
            sb2.metric("Bytes Transferred", format_bytes(srow.get("bytes_transferred")))
            sb3.metric("Transfer Status", srow.get("transfer_status") or "N/A")
            sb4.metric("Verify Status", srow.get("verify_status") or "N/A")

            st1, st2, st3, st4 = st.columns(4)
            st1.metric("Start Time", srow.get("start_time") or "N/A")
            st2.metric("End Time", srow.get("end_time") or "N/A")
            st3.metric("Total Time", srow.get("total_time") or "N/A")
            st4.metric("Task Mode", srow.get("task_mode") or "N/A")

            if srow.get("source_location_type") or srow.get("destination_location_type"):
                sl1, sl2 = st.columns(2)
                sl1.caption(f"**Source:** {srow.get('source_location_type', 'N/A')}")
                sl2.caption(f"**Destination:** {srow.get('destination_location_type', 'N/A')}")

            ff_prepare = int(srow.get("files_failed_prepare") or 0)
            ff_transfer = int(srow.get("files_failed_transfer") or 0)
            ff_verify = int(srow.get("files_failed_verify") or 0)
            ff_delete = int(srow.get("files_failed_delete") or 0)
            if ff_prepare + ff_transfer + ff_verify + ff_delete > 0:
                st.markdown("**Failed Files Breakdown**")
                ff1, ff2, ff3, ff4 = st.columns(4)
                ff1.metric("Prepare Failures", f"{ff_prepare:,}")
                ff2.metric("Transfer Failures", f"{ff_transfer:,}")
                ff3.metric("Verify Failures", f"{ff_verify:,}")
                ff4.metric("Delete Failures", f"{ff_delete:,}")

            if srow.get("error_code") or srow.get("error_detail"):
                st.error(
                    f"**Error Code:** {srow.get('error_code', 'N/A')}\n\n"
                    f"**Detail:** {srow.get('error_detail', 'N/A')}"
                )

# ── per-task error codes ─────────────────────────────────────────────────────
df_task_errors = error_code_distribution(conn, selected_task) if has_detail else pd.DataFrame()
if not df_task_errors.empty:
    st.divider()
    st.markdown("##### Error Code Breakdown")
    df_task_errors["error_code"] = df_task_errors["error_code"].astype(str)
    fig_terr = go.Figure(go.Bar(
        x=df_task_errors["error_code"], y=df_task_errors["count"],
        marker_color=C_FAILED,
    ))
    fig_terr.update_layout(
        xaxis_title="Error Code", xaxis_type="category", yaxis_title="Count",
        margin=dict(t=20, b=60, l=60, r=20),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        height=280,
    )
    st.plotly_chart(fig_terr, use_container_width=True)

st.divider()

# ── file-level records ───────────────────────────────────────────────────────
if has_detail:
    st.markdown("##### File Records")

    col_exec_filter, col_type_filter, col_status_filter, col_search = st.columns([1, 1, 1, 2])
    with col_exec_filter:
        exec_options = ["All"] + (df_exec["execution_id"].tolist() if not df_exec.empty else [])
        sel_exec = st.selectbox("Execution", exec_options, key="task_exec_filter")
    with col_type_filter:
        type_options = ["All", "transferred", "verified", "skipped", "deleted"]
        sel_type = st.selectbox("Report Type", type_options, key="task_type_filter")
    with col_status_filter:
        status_options = ["All", "SUCCESS", "FAILED"]
        sel_status = st.selectbox("Status", status_options, key="task_status_filter")
    with col_search:
        file_search = st.text_input("Search file path", key="task_file_search")

    exec_filter = sel_exec if sel_exec != "All" else None
    type_filter = sel_type if sel_type != "All" else None
    status_filter = sel_status if sel_status != "All" else None

    total_count = file_detail_count(conn, selected_task, exec_filter, type_filter, status_filter,
                                     file_search or None)

    PAGE_SIZE = 100
    total_pages = max(1, (total_count + PAGE_SIZE - 1) // PAGE_SIZE)

    st.caption(f"{total_count:,} records")

    page = st.number_input("Page", min_value=1, max_value=total_pages, value=1, key="task_page")
    offset = (page - 1) * PAGE_SIZE

    df_files = file_detail(conn, selected_task, exec_filter, type_filter, status_filter,
                           file_search or None, limit=PAGE_SIZE, offset=offset)

    if df_files.empty:
        st.caption("No records match your filters.")
    else:
        def highlight_rows(row):
            if row.get("status") == "FAILED":
                return ["background-color: rgba(248,113,113,0.15)"] * len(row)
            if row.get("report_type") == "verified":
                return ["background-color: rgba(74,222,128,0.08)"] * len(row)
            return [""] * len(row)

        st.dataframe(
            df_files.style.apply(highlight_rows, axis=1),
            use_container_width=True,
            height=min(len(df_files) * 36 + 40, 600),
        )

        st.caption(f"Page {page} of {total_pages}")

conn.close()
