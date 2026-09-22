"""Overview dashboard — high-level KPIs, status distribution, and health indicators."""

import streamlit as st
import plotly.express as px
import plotly.graph_objects as go

from db import (
    get_connection, global_stats, task_summary,
    tasks_with_multiple_executions, top_failing_tasks,
    error_code_distribution, task_mode_distribution,
)
from utils import truncate_task_names

st.set_page_config(page_title="Overview | DataSync Monitor", page_icon="📊", layout="wide")

C_TRANSFERRED = "#6C9EFF"
C_VERIFIED    = "#4ADE80"
C_FAILED      = "#F87171"
C_SKIPPED     = "#FBBF24"
C_DELETED     = "#A78BFA"
C_SUCCESS     = "#4ADE80"

st.markdown("## Overview")

conn = get_connection()
stats = global_stats(conn)

if stats["total_tasks"] == 0:
    st.info("No data ingested yet. Use the **Refresh Data** button in the sidebar of the home page to pull reports from S3.")
    st.stop()

# ── KPI row ──────────────────────────────────────────────────────────────────
k1, k2, k3, k4, k5, k6 = st.columns(6)
k1.metric("Tasks", f"{stats['total_tasks']:,}")
k2.metric("Executions", f"{stats['total_executions']:,}")
k3.metric("Files Transferred", f"{stats['transferred']:,}")
k4.metric("Files Verified", f"{stats['verified']:,}")
k5.metric("Total Failures", f"{stats['total_failed']:,}",
          delta=None if stats["total_failed"] == 0 else f"{stats['total_failed']:,}",
          delta_color="inverse")
k6.metric("Files Skipped", f"{stats['skipped']:,}")

st.divider()

# ── status distribution & mode breakdown ─────────────────────────────────────
col_donut, col_bar = st.columns([1, 2])

with col_donut:
    st.markdown("##### Record Status Distribution")
    labels = ["Transferred OK", "Transfer Failed", "Verified OK", "Verification Failed", "Skipped", "Deleted"]
    values = [
        stats["transfer_success"], stats["transfer_failed"],
        stats["verify_success"], stats["verify_failed"],
        stats["skipped"], stats["deleted"],
    ]
    colors = [C_TRANSFERRED, C_FAILED, C_VERIFIED, "#FBBF24", C_SKIPPED, C_DELETED]

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

    df_modes = task_mode_distribution(conn)
    if not df_modes.empty:
        st.markdown("##### Task Mode")
        for _, row in df_modes.iterrows():
            st.caption(f"**{row['task_mode'].capitalize()}**: {int(row['tasks'])} tasks")

with col_bar:
    st.markdown("##### Records per Task (Top 30)")
    df_tasks = task_summary(conn).head(30)
    short_names = truncate_task_names(df_tasks["task_name"].tolist())
    fig_bar = go.Figure()
    for col_name, label, color in [
        ("transferred", "Files Transferred", C_TRANSFERRED),
        ("verified", "Files Verified", C_VERIFIED),
        ("transfer_failed", "Transfer Failures", C_FAILED),
        ("skipped", "Files Skipped", C_SKIPPED),
        ("deleted", "Files Deleted", C_DELETED),
    ]:
        if col_name in df_tasks.columns:
            fig_bar.add_trace(go.Bar(
                x=short_names, y=df_tasks[col_name],
                name=label, marker_color=color,
                customdata=df_tasks["task_name"],
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

# ── multi-execution tasks & top failures ─────────────────────────────────────
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

# ── error code breakdown ─────────────────────────────────────────────────────
df_errors = error_code_distribution(conn)
if not df_errors.empty:
    st.divider()
    st.markdown("##### Error Code Distribution (All Tasks)")
    df_errors["error_code"] = df_errors["error_code"].astype(str)
    fig_err = px.bar(
        df_errors, x="error_code", y="count",
        color_discrete_sequence=[C_FAILED],
        labels={"error_code": "Error Code", "count": "Occurrences"},
    )
    fig_err.update_layout(
        xaxis_type="category",
        margin=dict(t=20, b=60, l=60, r=20),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        height=300,
    )
    st.plotly_chart(fig_err, use_container_width=True)

st.divider()

# ── full task table ──────────────────────────────────────────────────────────
st.markdown("##### All Tasks")
df_all = task_summary(conn)

search = st.text_input("Filter tasks by name", key="overview_search")
if search:
    df_all = df_all[df_all["task_name"].str.contains(search, case=False, na=False)]

def _highlight_failed(val):
    if isinstance(val, (int, float)) and val > 0:
        intensity = min(val / 50, 1.0)
        return f"background-color: rgba(248,113,113,{0.1 + intensity * 0.3})"
    return ""

st.dataframe(
    df_all.style.map(_highlight_failed, subset=["total_failed"]),
    use_container_width=True,
    height=min(len(df_all) * 36 + 40, 600),
)

verification_rate = (
    stats["verify_success"] / stats["verified"] * 100 if stats["verified"] > 0 else 0
)
failure_rate = (
    stats["total_failed"] / stats["total_file_records"] * 100 if stats["total_file_records"] > 0 else 0
)

transfer_rate = (
    stats["transfer_success"] / stats["transferred"] * 100 if stats["transferred"] > 0 else 100
)

st.divider()
r1, r2, r3, r4 = st.columns(4)
r1.metric("Transfer Success Rate", f"{transfer_rate:.1f}%")
r2.metric("Verification Success Rate", f"{verification_rate:.1f}%")
r3.metric("Overall Failure Rate", f"{failure_rate:.1f}%")
r4.metric("Total Records", f"{stats['total_file_records']:,}")

conn.close()
