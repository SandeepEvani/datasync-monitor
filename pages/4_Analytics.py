"""Analytics — trends, volume metrics, and file size distribution."""

import streamlit as st
import plotly.graph_objects as go

from db import (
    get_connection, transfer_volume_trend, execution_count_trend,
    failure_count_trend, file_size_stats, avg_file_size_by_task,
    global_stats,
)
from utils import truncate_task_names, format_bytes, best_byte_unit

st.set_page_config(page_title="Analytics | DataSync Monitor", page_icon="📈", layout="wide")

C_TRANSFERRED = "#6C9EFF"
C_VERIFIED    = "#4ADE80"
C_FAILED      = "#F87171"
C_VOLUME      = "#38BDF8"

conn = get_connection()
stats = global_stats(conn)

if stats["total_tasks"] == 0:
    st.info("No data available. Ingest reports first.")
    st.stop()

st.markdown("## Analytics & Trends")
st.caption("Time-series trends, volume metrics, and file size analysis across all DataSync tasks.")


_format_bytes = format_bytes
_best_byte_unit = best_byte_unit


# ── top-level file stats ────────────────────────────────────────────────────
fstats = file_size_stats(conn)

st.divider()
m1, m2, m3, m4, m5, m6 = st.columns(6)
m1.metric("Files Transferred", f"{fstats['total_files']:,}")
m2.metric("Data Transferred", _format_bytes(fstats["total_bytes"]))
m3.metric("Avg File Size", _format_bytes(fstats["avg_size"]))
m4.metric("Median File Size", _format_bytes(fstats["median_size"]))
m5.metric("Smallest File", _format_bytes(fstats["min_size"]))
m6.metric("Largest File", _format_bytes(fstats["max_size"]))

st.divider()

# ── granularity toggle ──────────────────────────────────────────────────────
granularity = st.radio(
    "Time granularity",
    ["Day", "Hour"],
    horizontal=True,
    key="analytics_granularity",
)
gran = granularity.lower()
period_label = "Date" if gran == "day" else "Hour"

# ── fetch trend data ────────────────────────────────────────────────────────
df_vol = transfer_volume_trend(conn, gran)
df_exec = execution_count_trend(conn, gran)
df_fail = failure_count_trend(conn, gran)

# ── data transferred per period ─────────────────────────────────────────────
col_vol, col_files = st.columns(2)

with col_vol:
    st.markdown(f"##### Data Transferred Per {granularity}")
    if df_vol.empty:
        st.caption("No timestamped transfer records.")
    else:
        unit_label, divisor = _best_byte_unit(df_vol["bytes_moved"].max())
        df_vol["scaled"] = df_vol["bytes_moved"].fillna(0) / divisor

        fig_vol = go.Figure(go.Bar(
            x=df_vol["period"], y=df_vol["scaled"],
            marker_color=C_VOLUME,
            hovertemplate=f"%{{x}}<br>%{{y:.2f}} {unit_label}<extra></extra>",
        ))
        fig_vol.update_layout(
            xaxis_title=period_label,
            yaxis_title=f"{unit_label} Transferred",
            margin=dict(t=20, b=60, l=60, r=20),
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            height=340,
        )
        st.plotly_chart(fig_vol, use_container_width=True)

with col_files:
    st.markdown(f"##### Files Transferred Per {granularity}")
    if df_vol.empty:
        st.caption("No timestamped transfer records.")
    else:
        fig_files = go.Figure(go.Bar(
            x=df_vol["period"], y=df_vol["files_transferred"],
            marker_color=C_TRANSFERRED,
            hovertemplate="%{x}<br>%{y:,} files<extra></extra>",
        ))
        fig_files.update_layout(
            xaxis_title=period_label, yaxis_title="Files",
            margin=dict(t=20, b=60, l=60, r=20),
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            height=340,
        )
        st.plotly_chart(fig_files, use_container_width=True)

st.divider()

# ── executions & failures per period ────────────────────────────────────────
col_exec, col_fail = st.columns(2)

with col_exec:
    st.markdown(f"##### Task Executions Per {granularity}")
    if df_exec.empty:
        st.caption("No timestamped execution data.")
    else:
        fig_exec = go.Figure(go.Bar(
            x=df_exec["period"], y=df_exec["executions"],
            marker_color=C_VERIFIED,
            hovertemplate="%{x}<br>%{y:,} executions<extra></extra>",
        ))
        fig_exec.update_layout(
            xaxis_title=period_label, yaxis_title="Executions",
            margin=dict(t=20, b=60, l=60, r=20),
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            height=340,
        )
        st.plotly_chart(fig_exec, use_container_width=True)

with col_fail:
    st.markdown(f"##### Failures Per {granularity}")
    if df_fail.empty:
        st.success("No failures recorded.")
    else:
        fig_fail = go.Figure()
        fig_fail.add_trace(go.Bar(
            x=df_fail["period"], y=df_fail["transfer_failures"],
            marker_color="#F87171", name="Transfer Failures",
            hovertemplate="%{x}<br>%{y:,} transfer failures<extra></extra>",
        ))
        fig_fail.add_trace(go.Bar(
            x=df_fail["period"], y=df_fail["verify_failures"],
            marker_color="#FBBF24", name="Verification Failures",
            hovertemplate="%{x}<br>%{y:,} verification failures<extra></extra>",
        ))
        fig_fail.update_layout(
            barmode="stack",
            xaxis_title=period_label, yaxis_title="Failures",
            legend=dict(orientation="h", y=1.1, x=0.5, xanchor="center"),
            margin=dict(t=40, b=60, l=60, r=20),
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            height=340,
        )
        st.plotly_chart(fig_fail, use_container_width=True)

st.divider()

# ── cumulative trends ───────────────────────────────────────────────────────
if not df_vol.empty:
    st.markdown("##### Cumulative Data Transferred")
    df_cum = df_vol.copy()
    df_cum["cumulative_bytes"] = df_cum["bytes_moved"].fillna(0).cumsum()
    df_cum["cumulative_files"] = df_cum["files_transferred"].cumsum()

    cum_unit, cum_div = _best_byte_unit(df_cum["cumulative_bytes"].max())
    df_cum["cumulative_scaled"] = df_cum["cumulative_bytes"] / cum_div

    col_cum_vol, col_cum_files = st.columns(2)

    with col_cum_vol:
        fig_cum = go.Figure(go.Scatter(
            x=df_cum["period"], y=df_cum["cumulative_scaled"],
            mode="lines+markers", name=f"Cumulative {cum_unit}",
            line=dict(color=C_VOLUME, width=2),
            fill="tozeroy", fillcolor="rgba(56,189,248,0.1)",
            hovertemplate=f"%{{x}}<br>%{{y:.2f}} {cum_unit}<extra></extra>",
        ))
        fig_cum.update_layout(
            xaxis_title=period_label,
            yaxis_title=f"Cumulative ({cum_unit})",
            margin=dict(t=20, b=60, l=60, r=20),
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            height=300,
        )
        st.plotly_chart(fig_cum, use_container_width=True)

    with col_cum_files:
        fig_cum_f = go.Figure(go.Scatter(
            x=df_cum["period"], y=df_cum["cumulative_files"],
            mode="lines+markers", name="Cumulative Files",
            line=dict(color=C_TRANSFERRED, width=2),
            fill="tozeroy", fillcolor="rgba(108,158,255,0.1)",
            hovertemplate="%{x}<br>%{y:,} files<extra></extra>",
        ))
        fig_cum_f.update_layout(
            xaxis_title=period_label, yaxis_title="Cumulative Files",
            margin=dict(t=20, b=60, l=60, r=20),
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            height=300,
        )
        st.plotly_chart(fig_cum_f, use_container_width=True)

    st.divider()

# ── avg file size by task ───────────────────────────────────────────────────
st.markdown("##### Average File Size by Task (Top 30 by Volume)")
df_task_size = avg_file_size_by_task(conn)

if df_task_size.empty:
    st.caption("No file size data available.")
else:
    avg_unit, avg_div = _best_byte_unit(df_task_size["avg_size"].max())
    df_task_size["avg_scaled"] = df_task_size["avg_size"].fillna(0) / avg_div

    short_names = truncate_task_names(df_task_size["task_name"].tolist())
    fig_avg = go.Figure(go.Bar(
        x=short_names,
        y=df_task_size["avg_scaled"],
        marker_color=C_TRANSFERRED,
        text=[_format_bytes(v) for v in df_task_size["avg_size"]],
        textposition="outside",
        textfont=dict(size=9),
        customdata=df_task_size["task_name"],
        hovertemplate="%{customdata}<br>Avg: %{text}<extra></extra>",
    ))
    fig_avg.update_layout(
        xaxis=dict(tickangle=-45, tickfont=dict(size=9)),
        yaxis_title=f"Avg File Size ({avg_unit})",
        margin=dict(t=20, b=120, l=60, r=20),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        height=400,
    )
    st.plotly_chart(fig_avg, use_container_width=True)

    with st.expander("View detailed table"):
        display = df_task_size[["task_name", "files", "avg_size", "total_bytes"]].copy()
        display["avg_size"] = display["avg_size"].apply(_format_bytes)
        display["total_bytes"] = display["total_bytes"].apply(_format_bytes)
        display.columns = ["Task", "Files", "Avg File Size", "Total Volume"]
        st.dataframe(display, use_container_width=True, height=min(len(display) * 36 + 40, 500))

conn.close()
