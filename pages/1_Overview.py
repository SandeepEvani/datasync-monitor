"""Overview dashboard — unified KPIs, management plane, and health indicators."""

import streamlit as st
import plotly.graph_objects as go

from db import (
    get_connection, unified_kpis, unified_task_table,
    tasks_with_multiple_executions, top_failing_tasks,
    error_code_distribution, task_mode_distribution,
    api_task_stats, api_execution_stats, api_tasks_list,
)
from utils import truncate_task_names, format_bytes

st.set_page_config(page_title="Overview | DataSync Monitor", page_icon="📊", layout="wide")

C_TRANSFERRED = "#6C9EFF"
C_VERIFIED    = "#4ADE80"
C_FAILED      = "#F87171"
C_SKIPPED     = "#FBBF24"
C_DELETED     = "#A78BFA"
C_SUCCESS     = "#4ADE80"
C_QUEUED      = "#60A5FA"
C_ACTIVE      = "#34D399"

st.markdown("## Overview")

conn = get_connection()
kpis = unified_kpis(conn)
api_ts = api_task_stats(conn)
api_es = api_execution_stats(conn)

has_data = kpis["total_tasks"] > 0 or api_ts["total_tasks"] > 0

if not has_data:
    st.info("No data ingested yet. Use the sidebar on the home page to poll the API and refresh reports.")
    st.stop()

# ── Management Plane ────────────────────────────────────────────────────────
if api_ts["total_tasks"] > 0:
    st.markdown("##### Management Plane (DataSync API)")
    m1, m2, m3, m4, m5, m6, m7 = st.columns(7)
    m1.metric("Tasks", f"{api_ts['total_tasks']:,}")
    m2.metric("Available", f"{api_ts['available']:,}")
    m3.metric("Running", f"{api_ts['running']:,}")
    m4.metric("Total Executions", f"{api_es['total_executions']:,}")
    m5.metric("Queued", f"{api_es['queued']:,}")
    m6.metric("Succeeded", f"{api_es['succeeded']:,}")
    m7.metric("Failed", f"{api_es['failed']:,}",
              delta=None if api_es["failed"] == 0 else f"{api_es['failed']:,}",
              delta_color="inverse")

    if api_es["active"] > 0:
        active_breakdown = []
        if api_es["queued"]:
            active_breakdown.append(f"{api_es['queued']} queued")
        if api_es["preparing"]:
            active_breakdown.append(f"{api_es['preparing']} preparing")
        if api_es["transferring"]:
            active_breakdown.append(f"{api_es['transferring']} transferring")
        if api_es["verifying"]:
            active_breakdown.append(f"{api_es['verifying']} verifying")
        st.info(f"**{api_es['active']}** active execution(s): {', '.join(active_breakdown)}")

    # Execution status donut
    exec_labels = ["Queued", "Preparing", "Transferring", "Verifying", "Succeeded", "Failed"]
    exec_values = [
        api_es["queued"], api_es["preparing"], api_es["transferring"],
        api_es["verifying"], api_es["succeeded"], api_es["failed"],
    ]
    exec_colors = [C_QUEUED, "#FCD34D", "#FB923C", "#A78BFA", C_SUCCESS, C_FAILED]

    if sum(exec_values) > 0:
        col_exec_donut, col_task_table = st.columns([1, 2])
        with col_exec_donut:
            fig_exec = go.Figure(go.Pie(
                labels=exec_labels, values=exec_values,
                hole=0.55,
                marker=dict(colors=exec_colors),
                textinfo="label+value",
                textposition="outside",
                textfont=dict(size=11, color="#E6EDF3"),
            ))
            fig_exec.update_layout(
                showlegend=False,
                margin=dict(t=20, b=20, l=20, r=20),
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(0,0,0,0)",
                height=300,
            )
            st.plotly_chart(fig_exec, use_container_width=True)

        with col_task_table:
            st.markdown("##### Tasks from API")
            df_api = api_tasks_list(conn)
            if not df_api.empty:
                st.dataframe(df_api, use_container_width=True,
                             height=min(len(df_api) * 36 + 40, 400))

    st.divider()

# ── Unified KPIs (single source of truth) ──────────────────────────────────
st.markdown("##### Data Layer — Unified KPIs")
st.caption(
    f"Detailed reports: {kpis['detail_executions']} executions | "
    f"Summary-only: {kpis['summary_only_executions']} executions"
)

k1, k2, k3, k4, k5, k6 = st.columns(6)
k1.metric("Tasks", f"{kpis['total_tasks']:,}")
k2.metric("Executions", f"{kpis['total_executions']:,}")
k3.metric("Files Transferred", f"{kpis['files_transferred']:,}")
k4.metric("Files Verified", f"{kpis['files_verified']:,}")
k5.metric("Total Failures", f"{kpis['total_failed']:,}",
          delta=None if kpis["total_failed"] == 0 else f"{kpis['total_failed']:,}",
          delta_color="inverse")
k6.metric("Net Data Moved", format_bytes(kpis["net_bytes_moved"]))

st.divider()

# ── status distribution ────────────────────────────────────────────────────
col_donut, col_bar = st.columns([1, 2])

with col_donut:
    st.markdown("##### Record Status Distribution")
    labels = ["Transferred OK", "Transfer Failed", "Verified OK", "Verification Failed", "Skipped", "Deleted"]
    values = [
        kpis["transfer_ok"], kpis["transfer_failed"],
        kpis["verify_ok"], kpis["verify_failed"],
        kpis["files_skipped"], kpis["files_deleted"],
    ]
    colors = [C_TRANSFERRED, C_FAILED, C_VERIFIED, "#FBBF24", C_SKIPPED, C_DELETED]

    if sum(v or 0 for v in values) > 0:
        fig_donut = go.Figure(go.Pie(
            labels=labels, values=values,
            hole=0.55,
            marker=dict(colors=colors),
            textinfo="label+percent",
            textposition="outside",
            textfont=dict(size=11, color="#E6EDF3"),
        ))
        fig_donut.update_layout(
            showlegend=False,
            margin=dict(t=20, b=20, l=20, r=20),
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            height=360,
        )
        st.plotly_chart(fig_donut, use_container_width=True)
    else:
        st.caption("No file-level status data available.")

    df_modes = task_mode_distribution(conn)
    if not df_modes.empty:
        st.markdown("##### Task Mode")
        for _, row in df_modes.iterrows():
            st.caption(f"**{row['task_mode'].capitalize()}**: {int(row['tasks'])} tasks")

with col_bar:
    st.markdown("##### Records per Task (Top 30)")
    df_unified = unified_task_table(conn).head(30)
    if not df_unified.empty:
        short_names = truncate_task_names(df_unified["task_name"].tolist())
        fig_bar = go.Figure()
        for col_name, label, color in [
            ("files_transferred", "Files Transferred", C_TRANSFERRED),
            ("files_verified", "Files Verified", C_VERIFIED),
            ("transfer_failed", "Transfer Failures", C_FAILED),
            ("files_skipped", "Files Skipped", C_SKIPPED),
            ("files_deleted", "Files Deleted", C_DELETED),
        ]:
            if col_name in df_unified.columns:
                fig_bar.add_trace(go.Bar(
                    x=short_names, y=df_unified[col_name],
                    name=label, marker_color=color,
                    customdata=df_unified["task_name"],
                    hovertemplate="%{customdata}<br>" + label + ": %{y:,}<extra></extra>",
                ))
        fig_bar.update_layout(
            barmode="stack",
            xaxis=dict(tickangle=-45, tickfont=dict(size=9)),
            yaxis_title="Record count",
            legend=dict(orientation="h", y=1.12, x=0.5, xanchor="center"),
            margin=dict(t=40, b=100, l=60, r=20),
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            height=400,
        )
        st.plotly_chart(fig_bar, use_container_width=True)

st.divider()

# ── multi-execution tasks & top failures ───────────────────────────────────
col_multi, col_fail = st.columns(2)

with col_multi:
    st.markdown("##### Tasks with Multiple Executions")
    df_multi = tasks_with_multiple_executions(conn)
    if df_multi.empty:
        st.caption("All tasks have a single execution.")
    else:
        df_m = df_multi.head(20).copy()
        df_m["short_name"] = truncate_task_names(df_m["task_name"].tolist())
        fig_multi = go.Figure(go.Bar(
            x=df_m["short_name"], y=df_m["executions"],
            marker_color=C_TRANSFERRED,
            customdata=df_m["task_name"],
            hovertemplate="%{customdata}<br>Executions: %{y:,}<extra></extra>",
        ))
        fig_multi.update_layout(
            xaxis_tickangle=-45,
            yaxis_title="Executions",
            margin=dict(t=20, b=100, l=60, r=20),
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            height=340,
        )
        st.plotly_chart(fig_multi, use_container_width=True)

with col_fail:
    st.markdown("##### Top Failing Tasks")
    df_fail = top_failing_tasks(conn)
    if df_fail.empty:
        st.success("No failures recorded.")
    else:
        df_f = df_fail.copy()
        df_f["short_name"] = truncate_task_names(df_f["task_name"].tolist())
        fig_fail = go.Figure(go.Bar(
            x=df_f["short_name"], y=df_f["failed_records"],
            marker_color=C_FAILED,
            customdata=df_f["task_name"],
            hovertemplate="%{customdata}<br>Failed: %{y:,}<extra></extra>",
        ))
        fig_fail.update_layout(
            xaxis_tickangle=-45,
            yaxis_title="Failed records",
            margin=dict(t=20, b=100, l=60, r=20),
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            height=340,
        )
        st.plotly_chart(fig_fail, use_container_width=True)

# ── error code breakdown ───────────────────────────────────────────────────
df_errors = error_code_distribution(conn)
if not df_errors.empty:
    st.divider()
    st.markdown("##### Error Code Distribution (All Tasks)")
    df_errors["error_code"] = df_errors["error_code"].astype(str)
    fig_err = go.Figure(go.Bar(
        x=df_errors["error_code"], y=df_errors["count"],
        marker_color=C_FAILED,
    ))
    fig_err.update_layout(
        xaxis_title="Error Code", xaxis_type="category",
        yaxis_title="Occurrences",
        margin=dict(t=20, b=60, l=60, r=20),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        height=300,
    )
    st.plotly_chart(fig_err, use_container_width=True)

st.divider()

# ── full task table (unified) ──────────────────────────────────────────────
st.markdown("##### All Tasks (Unified View)")
df_all = unified_task_table(conn)

search = st.text_input("Filter tasks by name", key="overview_search")
if search:
    df_all = df_all[df_all["task_name"].str.contains(search, case=False, na=False)]

if not df_all.empty:
    df_display = df_all.copy()
    df_display["bytes_moved"] = df_display["bytes_moved"].apply(format_bytes)
    df_display["task_mode"] = df_display["task_mode"].str.capitalize()
    df_display.columns = [
        "Task", "Executions", "Transferred", "Transfer Failed",
        "Verified", "Verify Failed", "Skipped", "Deleted",
        "Total Failed", "Data Moved", "Source", "Mode",
    ]

    def _highlight_failed(val):
        if isinstance(val, (int, float)) and val > 0:
            intensity = min(val / 50, 1.0)
            return f"background-color: rgba(248,113,113,{0.1 + intensity * 0.3})"
        return ""

    st.dataframe(
        df_display.style.map(_highlight_failed, subset=["Total Failed"]),
        use_container_width=True,
        height=min(len(df_display) * 36 + 40, 600),
    )

# ── bottom KPIs ────────────────────────────────────────────────────────────
transfer_rate = (
    kpis["transfer_ok"] / kpis["files_transferred"] * 100
    if kpis["files_transferred"] > 0 else 100
)
verification_rate = (
    kpis["verify_ok"] / kpis["files_verified"] * 100
    if kpis["files_verified"] > 0 else 0
)
total_records = kpis["files_transferred"] + kpis["files_verified"] + kpis["files_skipped"] + kpis["files_deleted"]
failure_rate = (
    kpis["total_failed"] / total_records * 100 if total_records > 0 else 0
)

st.divider()
r1, r2, r3, r4 = st.columns(4)
r1.metric("Transfer Success Rate", f"{transfer_rate:.1f}%")
r2.metric("Verification Success Rate", f"{verification_rate:.1f}%")
r3.metric("Overall Failure Rate", f"{failure_rate:.1f}%")
r4.metric("Net Data Moved", format_bytes(kpis["net_bytes_moved"]))

conn.close()
