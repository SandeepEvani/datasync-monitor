"""Hourly Monitor — real-time hourly view with AWS-style time range filters."""

from datetime import datetime, timedelta

import streamlit as st
import plotly.graph_objects as go

import config
from db import (
    get_connection, global_stats,
    hourly_transfer_volume, hourly_verification,
    hourly_execution_count, hourly_failure_breakdown,
    time_range_stats, summary_global_stats,
)
from cloudwatch_client import get_ec2_metrics, get_datasync_agent_metrics, compute_period
from utils import format_bytes, best_byte_unit

st.set_page_config(page_title="Hourly Monitor | DataSync Monitor", page_icon="⏱️", layout="wide")

C_TRANSFERRED = "#6C9EFF"
C_VERIFIED    = "#4ADE80"
C_FAILED      = "#F87171"
C_VERIFY_FAIL = "#FBBF24"
C_VOLUME      = "#38BDF8"
C_SUCCESS     = "#34D399"
C_EXECUTIONS  = "#A78BFA"

conn = get_connection()
stats = global_stats(conn)

if stats["total_tasks"] == 0:
    st.info("No data available. Ingest reports first.")
    st.stop()


_format_bytes = format_bytes
_best_byte_unit = best_byte_unit


# ── header ──────────────────────────────────────────────────────────────────
st.markdown("## Hourly Monitor")
st.caption("Real-time hourly view of transfer activity, failures, and throughput.")

# ── time range controls ─────────────────────────────────────────────────────
st.markdown(
    """<style>
    div[data-testid="stHorizontalBlock"] > div[data-testid="column"] button {
        width: 100%;
    }
    </style>""",
    unsafe_allow_html=True,
)

PRESETS = {
    "1h": timedelta(hours=1),
    "3h": timedelta(hours=3),
    "6h": timedelta(hours=6),
    "12h": timedelta(hours=12),
    "1d": timedelta(days=1),
    "3d": timedelta(days=3),
    "7d": timedelta(days=7),
    "14d": timedelta(days=14),
    "30d": timedelta(days=30),
}

col_presets, col_custom = st.columns([3, 2])

with col_presets:
    st.markdown("**Quick Range**")
    preset_cols = st.columns(len(PRESETS))
    selected_preset = st.session_state.get("hm_preset", "24h")

    for i, (label, _) in enumerate(PRESETS.items()):
        with preset_cols[i]:
            btn_type = "primary" if selected_preset == label else "secondary"
            if st.button(label, key=f"hm_btn_{label}", type=btn_type, use_container_width=True):
                st.session_state["hm_preset"] = label
                st.session_state["hm_use_custom"] = False
                st.rerun()

with col_custom:
    st.markdown("**Custom Range**")
    cc1, cc2 = st.columns(2)
    with cc1:
        custom_start_date = st.date_input("Start", key="hm_start_date")
        custom_start_time = st.time_input("Start time", value=datetime.min.time(), key="hm_start_time")
    with cc2:
        custom_end_date = st.date_input("End", key="hm_end_date")
        custom_end_time = st.time_input("End time", value=datetime.max.replace(microsecond=0).time(), key="hm_end_time")

    if st.button("Apply Custom Range", key="hm_apply_custom", use_container_width=True):
        st.session_state["hm_use_custom"] = True
        st.session_state["hm_preset"] = None
        st.rerun()

# ── compute time window ─────────────────────────────────────────────────────
use_custom = st.session_state.get("hm_use_custom", False)

if use_custom:
    start_dt = datetime.combine(custom_start_date, custom_start_time)
    end_dt = datetime.combine(custom_end_date, custom_end_time)
    range_label = f"{start_dt:%Y-%m-%d %H:%M} → {end_dt:%Y-%m-%d %H:%M}"
else:
    preset_key = st.session_state.get("hm_preset", "1d")
    delta = PRESETS.get(preset_key, timedelta(days=1))
    end_dt = datetime.now(config.IST)
    start_dt = end_dt - delta
    range_label = f"Last {preset_key}"

start_str = start_dt.isoformat()
end_str = end_dt.isoformat()

st.divider()
st.caption(f"Showing: **{range_label}**")

# ── KPIs for the selected time range ────────────────────────────────────────
rs = time_range_stats(conn, start_str, end_str)

def _v(val):
    return val if val is not None else 0

k1, k2, k3, k4, k5, k6, k7 = st.columns(7)
k1.metric("Active Tasks", f"{_v(rs['tasks']):,}")
k2.metric("Executions", f"{_v(rs['executions']):,}")
k3.metric("Files Transferred", f"{_v(rs['files_transferred']):,}")
k4.metric("Transfer Failures", f"{_v(rs['transfer_failed']):,}",
          delta=None if not rs.get("transfer_failed") else f"{rs['transfer_failed']:,}",
          delta_color="inverse")
k5.metric("Files Verified", f"{_v(rs['files_verified']):,}")
k6.metric("Verification Failures", f"{_v(rs['verify_failed']):,}",
          delta=None if not rs.get("verify_failed") else f"{rs['verify_failed']:,}",
          delta_color="inverse")
k7.metric("Data Transferred", _format_bytes(rs.get("bytes_transferred")))

if rs.get("files_transferred") and rs["files_transferred"] > 0:
    transfer_ok = _v(rs.get("transfer_ok"))
    transfer_rate = transfer_ok / rs["files_transferred"] * 100
    st.progress(min(transfer_rate / 100, 1.0), text=f"Transfer Success Rate: {transfer_rate:.1f}%")

st.divider()

# ── fetch hourly data ───────────────────────────────────────────────────────
df_transfers = hourly_transfer_volume(conn, start_str, end_str)
df_verify = hourly_verification(conn, start_str, end_str)
df_exec = hourly_execution_count(conn, start_str, end_str)
df_fail = hourly_failure_breakdown(conn, start_str, end_str)

# ── transfer throughput ─────────────────────────────────────────────────────
col_vol, col_count = st.columns(2)

with col_vol:
    st.markdown("##### Data Throughput")
    if df_transfers.empty:
        st.caption("No transfer data in this time range.")
    else:
        unit, divisor = _best_byte_unit(df_transfers["bytes_moved"].max())
        df_transfers["scaled"] = df_transfers["bytes_moved"].fillna(0) / divisor

        fig_vol = go.Figure()
        fig_vol.add_trace(go.Scatter(
            x=df_transfers["hour"], y=df_transfers["scaled"],
            mode="lines+markers", name=f"{unit}/hr",
            line=dict(color=C_VOLUME, width=2),
            fill="tozeroy", fillcolor="rgba(56,189,248,0.08)",
            hovertemplate=f"%{{x|%b %d %H:%M}}<br>%{{y:.2f}} {unit}<extra></extra>",
        ))
        fig_vol.update_layout(
            xaxis_title="Hour", yaxis_title=f"{unit} / hour",
            xaxis=dict(tickformat="%b %d\n%H:%M"),
            margin=dict(t=20, b=60, l=60, r=20),
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            height=300,
        )
        st.plotly_chart(fig_vol, use_container_width=True)

with col_count:
    st.markdown("##### File Transfer Count")
    if df_transfers.empty:
        st.caption("No transfer data in this time range.")
    else:
        fig_count = go.Figure()
        fig_count.add_trace(go.Bar(
            x=df_transfers["hour"], y=df_transfers["succeeded"],
            marker_color=C_SUCCESS, name="Succeeded",
            hovertemplate="%{x|%b %d %H:%M}<br>%{y:,} succeeded<extra></extra>",
        ))
        fig_count.add_trace(go.Bar(
            x=df_transfers["hour"], y=df_transfers["failed"],
            marker_color=C_FAILED, name="Failed",
            hovertemplate="%{x|%b %d %H:%M}<br>%{y:,} failed<extra></extra>",
        ))
        fig_count.update_layout(
            barmode="stack",
            xaxis_title="Hour", yaxis_title="Files / hour",
            xaxis=dict(tickformat="%b %d\n%H:%M"),
            legend=dict(orientation="h", y=1.1, x=0.5, xanchor="center"),
            margin=dict(t=40, b=60, l=60, r=20),
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            height=300,
        )
        st.plotly_chart(fig_count, use_container_width=True)

st.divider()

# ── failures & verification ────────────────────────────────────────────────
col_fail, col_ver = st.columns(2)

with col_fail:
    st.markdown("##### Failure Breakdown")
    if df_fail.empty:
        st.success("No failures in this time range.")
    else:
        fig_fail = go.Figure()
        fig_fail.add_trace(go.Bar(
            x=df_fail["hour"], y=df_fail["transfer_failures"],
            marker_color=C_FAILED, name="Transfer Failures",
            hovertemplate="%{x|%b %d %H:%M}<br>%{y:,} transfer failures<extra></extra>",
        ))
        fig_fail.add_trace(go.Bar(
            x=df_fail["hour"], y=df_fail["verify_failures"],
            marker_color=C_VERIFY_FAIL, name="Verification Failures",
            hovertemplate="%{x|%b %d %H:%M}<br>%{y:,} verification failures<extra></extra>",
        ))
        fig_fail.update_layout(
            barmode="stack",
            xaxis_title="Hour", yaxis_title="Failures / hour",
            xaxis=dict(tickformat="%b %d\n%H:%M"),
            legend=dict(orientation="h", y=1.1, x=0.5, xanchor="center"),
            margin=dict(t=40, b=60, l=60, r=20),
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            height=300,
        )
        st.plotly_chart(fig_fail, use_container_width=True)

with col_ver:
    st.markdown("##### Verification Activity")
    if df_verify.empty:
        st.caption("No verification data in this time range.")
    else:
        fig_ver = go.Figure()
        fig_ver.add_trace(go.Bar(
            x=df_verify["hour"], y=df_verify["succeeded"],
            marker_color=C_VERIFIED, name="Verified OK",
            hovertemplate="%{x|%b %d %H:%M}<br>%{y:,} verified OK<extra></extra>",
        ))
        fig_ver.add_trace(go.Bar(
            x=df_verify["hour"], y=df_verify["failed"],
            marker_color=C_VERIFY_FAIL, name="Verification Failed",
            hovertemplate="%{x|%b %d %H:%M}<br>%{y:,} verification failed<extra></extra>",
        ))
        fig_ver.update_layout(
            barmode="stack",
            xaxis_title="Hour", yaxis_title="Files / hour",
            xaxis=dict(tickformat="%b %d\n%H:%M"),
            legend=dict(orientation="h", y=1.1, x=0.5, xanchor="center"),
            margin=dict(t=40, b=60, l=60, r=20),
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            height=300,
        )
        st.plotly_chart(fig_ver, use_container_width=True)

st.divider()

# ── executions & tasks per hour ─────────────────────────────────────────────
st.markdown("##### Executions & Active Tasks Per Hour")
if df_exec.empty:
    st.caption("No execution data in this time range.")
else:
    fig_exec = go.Figure()
    fig_exec.add_trace(go.Bar(
        x=df_exec["hour"], y=df_exec["executions"],
        marker_color=C_EXECUTIONS, name="Executions",
        hovertemplate="%{x|%b %d %H:%M}<br>%{y:,} executions<extra></extra>",
    ))
    fig_exec.add_trace(go.Scatter(
        x=df_exec["hour"], y=df_exec["tasks"],
        mode="lines+markers", name="Active Tasks",
        line=dict(color=C_TRANSFERRED, width=2),
        yaxis="y2",
        hovertemplate="%{x|%b %d %H:%M}<br>%{y:,} tasks<extra></extra>",
    ))
    fig_exec.update_layout(
        xaxis_title="Hour",
        xaxis=dict(tickformat="%b %d\n%H:%M"),
        yaxis=dict(title="Executions", side="left"),
        yaxis2=dict(title="Active Tasks", side="right", overlaying="y"),
        legend=dict(orientation="h", y=1.1, x=0.5, xanchor="center"),
        margin=dict(t=40, b=60, l=60, r=60),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        height=320,
    )
    st.plotly_chart(fig_exec, use_container_width=True)

conn.close()

# ── CloudWatch metrics ─────────────────────────────────────────────────────
if config.EC2_INSTANCE_IDS or config.DATASYNC_AGENT_ARNS:
    st.divider()
    st.markdown("## CloudWatch Metrics")

    cw_period = compute_period(start_dt, end_dt)

    if config.EC2_INSTANCE_IDS:
        st.markdown("### EC2 Instance Metrics")
        for instance_id in config.EC2_INSTANCE_IDS:
            st.markdown(f"**Instance: `{instance_id}`**")
            try:
                ec2_m = get_ec2_metrics(instance_id, start_dt, end_dt, cw_period)
            except Exception as e:
                st.error(f"Failed to fetch EC2 metrics: {e}")
                continue

            col_cpu, col_net = st.columns(2)

            with col_cpu:
                st.markdown("##### CPU Utilization")
                df_cpu = ec2_m.get("CPUUtilization")
                if df_cpu is None or df_cpu.empty:
                    st.caption("No CPU data.")
                else:
                    fig_cpu = go.Figure(go.Scatter(
                        x=df_cpu["timestamp"], y=df_cpu["value"],
                        mode="lines", name="CPU %",
                        line=dict(color="#F59E0B", width=2),
                        fill="tozeroy", fillcolor="rgba(245,158,11,0.08)",
                        hovertemplate="%{x|%b %d %H:%M}<br>%{y:.1f}%<extra></extra>",
                    ))
                    fig_cpu.update_layout(
                        yaxis_title="CPU %", yaxis_range=[0, 100],
                        xaxis=dict(tickformat="%b %d\n%H:%M"),
                        margin=dict(t=20, b=60, l=60, r=20),
                        paper_bgcolor="rgba(0,0,0,0)",
                        plot_bgcolor="rgba(0,0,0,0)", height=260,
                    )
                    st.plotly_chart(fig_cpu, use_container_width=True)

            with col_net:
                st.markdown("##### Network I/O")
                df_in = ec2_m.get("NetworkIn")
                df_out = ec2_m.get("NetworkOut")
                has_net = (df_in is not None and not df_in.empty) or (df_out is not None and not df_out.empty)
                if not has_net:
                    st.caption("No network data.")
                else:
                    all_vals = []
                    if df_in is not None and not df_in.empty:
                        all_vals.extend(df_in["value"].tolist())
                    if df_out is not None and not df_out.empty:
                        all_vals.extend(df_out["value"].tolist())
                    net_unit, net_div = _best_byte_unit(max(all_vals) if all_vals else 0)

                    fig_net = go.Figure()
                    if df_in is not None and not df_in.empty:
                        fig_net.add_trace(go.Scatter(
                            x=df_in["timestamp"], y=df_in["value"] / net_div,
                            mode="lines", name="In",
                            line=dict(color="#6C9EFF", width=2),
                            hovertemplate=f"%{{x|%b %d %H:%M}}<br>In: %{{y:.2f}} {net_unit}<extra></extra>",
                        ))
                    if df_out is not None and not df_out.empty:
                        fig_net.add_trace(go.Scatter(
                            x=df_out["timestamp"], y=df_out["value"] / net_div,
                            mode="lines", name="Out",
                            line=dict(color="#4ADE80", width=2),
                            hovertemplate=f"%{{x|%b %d %H:%M}}<br>Out: %{{y:.2f}} {net_unit}<extra></extra>",
                        ))
                    fig_net.update_layout(
                        yaxis_title=f"{net_unit} / period",
                        xaxis=dict(tickformat="%b %d\n%H:%M"),
                        legend=dict(orientation="h", y=1.1, x=0.5, xanchor="center"),
                        margin=dict(t=40, b=60, l=60, r=20),
                        paper_bgcolor="rgba(0,0,0,0)",
                        plot_bgcolor="rgba(0,0,0,0)", height=260,
                    )
                    st.plotly_chart(fig_net, use_container_width=True)

            col_dr, col_dw = st.columns(2)
            with col_dr:
                st.markdown("##### Disk Read")
                df_dr = ec2_m.get("DiskReadBytes")
                if df_dr is None or df_dr.empty:
                    st.caption("No disk read data.")
                else:
                    dr_unit, dr_div = _best_byte_unit(df_dr["value"].max())
                    fig_dr = go.Figure(go.Scatter(
                        x=df_dr["timestamp"], y=df_dr["value"] / dr_div,
                        mode="lines", name="Disk Read",
                        line=dict(color="#A78BFA", width=2),
                        fill="tozeroy", fillcolor="rgba(167,139,250,0.08)",
                        hovertemplate=f"%{{x|%b %d %H:%M}}<br>%{{y:.2f}} {dr_unit}<extra></extra>",
                    ))
                    fig_dr.update_layout(
                        yaxis_title=f"{dr_unit} / period",
                        xaxis=dict(tickformat="%b %d\n%H:%M"),
                        margin=dict(t=20, b=60, l=60, r=20),
                        paper_bgcolor="rgba(0,0,0,0)",
                        plot_bgcolor="rgba(0,0,0,0)", height=260,
                    )
                    st.plotly_chart(fig_dr, use_container_width=True)

            with col_dw:
                st.markdown("##### Disk Write")
                df_dw = ec2_m.get("DiskWriteBytes")
                if df_dw is None or df_dw.empty:
                    st.caption("No disk write data.")
                else:
                    dw_unit, dw_div = _best_byte_unit(df_dw["value"].max())
                    fig_dw = go.Figure(go.Scatter(
                        x=df_dw["timestamp"], y=df_dw["value"] / dw_div,
                        mode="lines", name="Disk Write",
                        line=dict(color="#FB923C", width=2),
                        fill="tozeroy", fillcolor="rgba(251,146,60,0.08)",
                        hovertemplate=f"%{{x|%b %d %H:%M}}<br>%{{y:.2f}} {dw_unit}<extra></extra>",
                    ))
                    fig_dw.update_layout(
                        yaxis_title=f"{dw_unit} / period",
                        xaxis=dict(tickformat="%b %d\n%H:%M"),
                        margin=dict(t=20, b=60, l=60, r=20),
                        paper_bgcolor="rgba(0,0,0,0)",
                        plot_bgcolor="rgba(0,0,0,0)", height=260,
                    )
                    st.plotly_chart(fig_dw, use_container_width=True)

            st.divider()

    if config.DATASYNC_AGENT_ARNS:
        st.markdown("### DataSync Agent Metrics")
        for agent_arn in config.DATASYNC_AGENT_ARNS:
            short_agent = agent_arn.split("/")[-1] if "/" in agent_arn else agent_arn
            st.markdown(f"**Agent: `{short_agent}`**")
            try:
                ds_m = get_datasync_agent_metrics(agent_arn, start_dt, end_dt, cw_period)
            except Exception as e:
                st.error(f"Failed to fetch DataSync agent metrics: {e}")
                continue

            col_bytes, col_files = st.columns(2)

            with col_bytes:
                st.markdown("##### Bytes Transferred")
                df_bt = ds_m.get("BytesTransferred")
                if df_bt is None or df_bt.empty:
                    st.caption("No bytes transferred data.")
                else:
                    bt_unit, bt_div = _best_byte_unit(df_bt["value"].max())
                    fig_bt = go.Figure(go.Bar(
                        x=df_bt["timestamp"], y=df_bt["value"] / bt_div,
                        marker_color=C_VOLUME,
                        hovertemplate=f"%{{x|%b %d %H:%M}}<br>%{{y:.2f}} {bt_unit}<extra></extra>",
                    ))
                    fig_bt.update_layout(
                        yaxis_title=f"{bt_unit} / period",
                        xaxis=dict(tickformat="%b %d\n%H:%M"),
                        margin=dict(t=20, b=60, l=60, r=20),
                        paper_bgcolor="rgba(0,0,0,0)",
                        plot_bgcolor="rgba(0,0,0,0)", height=260,
                    )
                    st.plotly_chart(fig_bt, use_container_width=True)

            with col_files:
                st.markdown("##### Files Transferred")
                df_ft = ds_m.get("FilesTransferred")
                if df_ft is None or df_ft.empty:
                    st.caption("No files transferred data.")
                else:
                    fig_ft = go.Figure(go.Bar(
                        x=df_ft["timestamp"], y=df_ft["value"],
                        marker_color=C_SUCCESS,
                        hovertemplate="%{x|%b %d %H:%M}<br>%{y:,.0f} files<extra></extra>",
                    ))
                    fig_ft.update_layout(
                        yaxis_title="Files / period",
                        xaxis=dict(tickformat="%b %d\n%H:%M"),
                        margin=dict(t=20, b=60, l=60, r=20),
                        paper_bgcolor="rgba(0,0,0,0)",
                        plot_bgcolor="rgba(0,0,0,0)", height=260,
                    )
                    st.plotly_chart(fig_ft, use_container_width=True)

            col_vs, col_vd = st.columns(2)

            with col_vs:
                st.markdown("##### Bytes Verified (Source)")
                df_vs = ds_m.get("BytesVerifiedSource")
                if df_vs is None or df_vs.empty:
                    st.caption("No source verification data.")
                else:
                    vs_unit, vs_div = _best_byte_unit(df_vs["value"].max())
                    fig_vs = go.Figure(go.Scatter(
                        x=df_vs["timestamp"], y=df_vs["value"] / vs_div,
                        mode="lines+markers", name="Source",
                        line=dict(color=C_VERIFIED, width=2),
                        hovertemplate=f"%{{x|%b %d %H:%M}}<br>%{{y:.2f}} {vs_unit}<extra></extra>",
                    ))
                    fig_vs.update_layout(
                        yaxis_title=f"{vs_unit} / period",
                        xaxis=dict(tickformat="%b %d\n%H:%M"),
                        margin=dict(t=20, b=60, l=60, r=20),
                        paper_bgcolor="rgba(0,0,0,0)",
                        plot_bgcolor="rgba(0,0,0,0)", height=260,
                    )
                    st.plotly_chart(fig_vs, use_container_width=True)

            with col_vd:
                st.markdown("##### Bytes Verified (Destination)")
                df_vd = ds_m.get("BytesVerifiedDestination")
                if df_vd is None or df_vd.empty:
                    st.caption("No destination verification data.")
                else:
                    vd_unit, vd_div = _best_byte_unit(df_vd["value"].max())
                    fig_vd = go.Figure(go.Scatter(
                        x=df_vd["timestamp"], y=df_vd["value"] / vd_div,
                        mode="lines+markers", name="Destination",
                        line=dict(color="#6C9EFF", width=2),
                        hovertemplate=f"%{{x|%b %d %H:%M}}<br>%{{y:.2f}} {vd_unit}<extra></extra>",
                    ))
                    fig_vd.update_layout(
                        yaxis_title=f"{vd_unit} / period",
                        xaxis=dict(tickformat="%b %d\n%H:%M"),
                        margin=dict(t=20, b=60, l=60, r=20),
                        paper_bgcolor="rgba(0,0,0,0)",
                        plot_bgcolor="rgba(0,0,0,0)", height=260,
                    )
                    st.plotly_chart(fig_vd, use_container_width=True)

            st.divider()
else:
    st.divider()
    st.caption(
        "Set `DATASYNC_EC2_INSTANCES` and/or `DATASYNC_AGENT_ARNS` environment variables "
        "to enable CloudWatch metrics."
    )
