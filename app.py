"""
DataSync Monitoring Dashboard — V2
===================================
Streamlit + DuckDB monitoring solution for AWS DataSync blob-to-S3 migrations.
Polls the DataSync management plane and ingests S3 JSON task reports.
Single source of truth: detailed reports preferred, summary as fallback.

Launch:  uv run streamlit run app.py
"""

import streamlit as st

import config
from db import (
    get_connection, ingestion_stats, purge_all,
    ingest_chunk_bulk, get_ingested_keys, get_watermark, set_watermark,
    unified_kpis, api_task_stats, api_execution_stats,
    upsert_api_tasks, upsert_api_executions,
)
from s3_client import list_report_keys, download_and_ingest_streaming
from datasync_api import poll_all_executions
from utils import format_bytes

st.set_page_config(
    page_title="DataSync Monitor",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown("""
<style>
    .block-container { padding-top: 2rem; padding-bottom: 1rem; }
    [data-testid="stMetric"] {
        background: #1A1F2B;
        border: 1px solid #2D3748;
        border-radius: 8px;
        padding: 12px 16px;
    }
    [data-testid="stMetricLabel"] { font-size: 0.8rem; color: #9CA3AF; }
    [data-testid="stMetricValue"] { font-size: 1.5rem; font-weight: 600; }
    .sidebar-title {
        font-size: 1.1rem;
        font-weight: 600;
        color: #6C9EFF;
        margin-bottom: 0.5rem;
    }
    .stProgress > div > div { background: #6C9EFF; }
    hr { border-color: #2D3748 !important; }
</style>
""", unsafe_allow_html=True)

# ── sidebar ──────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown('<p class="sidebar-title">⚡ DataSync Monitor</p>', unsafe_allow_html=True)
    st.caption("Azure Blob → S3 Migration Tracker")

    st.divider()

    st.markdown("**S3 Configuration**")
    bucket = st.text_input("S3 Bucket", value=config.S3_BUCKET, key="s3_bucket")
    prefix = st.text_input("Report Prefix", value=config.S3_PREFIX, key="s3_prefix")
    region = st.text_input("AWS Region", value=config.AWS_REGION, key="aws_region")
    profile = st.text_input("AWS Profile (optional)", value=config.AWS_PROFILE or "", key="aws_profile")

    if bucket:
        config.S3_BUCKET = bucket
    if prefix:
        config.S3_PREFIX = prefix
    if region:
        config.AWS_REGION = region
    config.AWS_PROFILE = profile if profile else None

    st.divider()

    st.markdown("**Data Controls**")

    if st.button("📡 Poll DataSync API", use_container_width=True):
        conn = get_connection()
        with st.spinner("Polling DataSync tasks and executions..."):
            try:
                tasks, execs = poll_all_executions()
                upsert_api_tasks(conn, tasks)
                upsert_api_executions(conn, execs)
                st.success(f"Polled {len(tasks)} tasks, {len(execs)} executions.")
            except Exception as e:
                st.error(f"API poll failed: {e}")
        conn.close()

    if st.button("🔄 Refresh Reports (Incremental)", use_container_width=True, type="primary"):
        if not config.S3_BUCKET:
            st.error("Set the S3 bucket name first.")
        else:
            conn = get_connection()
            watermark = get_watermark(conn)

            with st.spinner(
                "Listing S3 objects" + (f" modified after {watermark:%Y-%m-%d %H:%M}" if watermark else "") + "..."
            ):
                report_keys = list_report_keys(config.S3_BUCKET, config.S3_PREFIX, since=watermark)

            already = get_ingested_keys(conn)
            new_keys = [rk for rk in report_keys if rk.key not in already]

            if not new_keys:
                st.success("Already up to date — no new reports.")
            else:
                chunk_size = 500
                n_chunks = (len(new_keys) + chunk_size - 1) // chunk_size
                st.caption(
                    f"Found {len(new_keys)} new report files — "
                    f"ingesting in {n_chunks} chunk(s) of {chunk_size}."
                )

                progress = st.progress(0, text=f"Downloading & ingesting {len(new_keys)} files...")

                def _update_progress(done, total):
                    progress.progress(
                        done / total,
                        text=f"Downloaded & ingested {done:,}/{total:,} files...",
                    )

                def _ingest_chunk(results):
                    return ingest_chunk_bulk(conn, results)

                total_rows, summary_count = download_and_ingest_streaming(
                    config.S3_BUCKET,
                    new_keys,
                    chunk_size=chunk_size,
                    progress_callback=_update_progress,
                    ingest_callback=_ingest_chunk,
                )

                if new_keys:
                    max_ts = max(rk.last_modified for rk in new_keys)
                    set_watermark(conn, max_ts)

                progress.empty()
                st.success(
                    f"Ingested {len(new_keys)} report files "
                    f"({total_rows:,} detail records, {summary_count} summary reports)."
                )

            conn.close()

    if st.button("🗑️ Purge & Reload", use_container_width=True):
        conn = get_connection()
        purge_all(conn)
        conn.close()
        st.warning("All data purged. Click Refresh to re-ingest.")

    st.divider()

    conn = get_connection()
    ing = ingestion_stats(conn)
    wm = get_watermark(conn)
    conn.close()
    st.caption(f"Report files ingested: **{ing['reports_ingested']:,}**")
    if ing["last_ingestion"]:
        st.caption(f"Last refresh: {ing['last_ingestion']:%Y-%m-%d %H:%M IST}")
    if wm:
        st.caption(f"Watermark: {wm:%Y-%m-%d %H:%M IST}")

# ── main content ─────────────────────────────────────────────────────────────
st.markdown(
    """
    # ⚡ DataSync Migration Monitor
    Real-time visibility into your Azure Blob → Amazon S3 data migration pipeline.
    """
)

conn = get_connection()
kpis = unified_kpis(conn)
api_tasks = api_task_stats(conn)
api_execs = api_execution_stats(conn)

has_data = kpis["total_tasks"] > 0 or api_tasks["total_tasks"] > 0

if not has_data:
    st.markdown("---")
    st.markdown(
        """
        ### Getting Started

        1. **Configure** your S3 bucket and report prefix in the sidebar
        2. **Ensure** your AWS credentials are available (environment variables, profile, or IAM role)
        3. Click **Poll DataSync API** to discover tasks and execution status
        4. Click **Refresh Reports** to ingest detailed + summary JSON reports from S3
        5. Navigate to **Overview**, **Task Detail**, or **File Explorer** pages

        #### Expected S3 Report Structure (JSON)
        ```
        s3://<bucket>/<prefix>/<task_name>/
            ├── Detailed-Reports/<task-id>/<exec-id>/
            │   ├── *.json   (Transferred records)
            │   ├── *.json   (Verified records)
            │   ├── *.json   (Skipped records)
            │   └── *.json   (Deleted records)
            └── Summary-Reports/<task-id>/<exec-id>/
                └── *.json   (Execution summary)
        ```

        Both **Enhanced mode** and **Basic mode** report schemas are auto-detected.

        #### Environment Variables
        ```bash
        export DATASYNC_REPORTS_BUCKET="your-bucket-name"
        export DATASYNC_REPORTS_PREFIX="datasync-reports/"
        export AWS_REGION="us-east-1"
        export AWS_PROFILE="your-profile"       # optional
        ```
        """
    )
else:
    # ── Management Plane (from DataSync API) ────────────────────────────────
    if api_tasks["total_tasks"] > 0:
        st.markdown("---")
        st.markdown("#### Management Plane")
        st.caption("Live task & execution state from the DataSync API")

        a1, a2, a3, a4 = st.columns(4)
        a1.metric("Total Tasks (API)", f"{api_tasks['total_tasks']:,}")
        a2.metric("Available", f"{api_tasks['available']:,}")
        a3.metric("Running", f"{api_tasks['running']:,}")
        a4.metric("Unavailable", f"{api_tasks['unavailable']:,}",
                  delta=None if api_tasks["unavailable"] == 0
                  else f"{api_tasks['unavailable']:,}",
                  delta_color="inverse")

        e1, e2, e3, e4, e5, e6 = st.columns(6)
        e1.metric("Total Executions", f"{api_execs['total_executions']:,}")
        e2.metric("Queued", f"{api_execs['queued']:,}")
        e3.metric("Preparing", f"{api_execs['preparing']:,}")
        e4.metric("Transferring", f"{api_execs['transferring']:,}")
        e5.metric("Succeeded", f"{api_execs['succeeded']:,}")
        e6.metric("Failed", f"{api_execs['failed']:,}",
                  delta=None if api_execs["failed"] == 0
                  else f"{api_execs['failed']:,}",
                  delta_color="inverse")

        if api_execs["active"] > 0:
            st.info(f"**{api_execs['active']}** execution(s) currently active")

        if api_tasks.get("last_polled"):
            st.caption(f"Last polled: {api_tasks['last_polled']}")

    # ── Unified KPIs (single source of truth) ──────────────────────────────
    st.markdown("---")
    st.markdown("#### Data Layer (Report Analysis)")
    st.caption(
        f"Source: {kpis['detail_executions']} executions from detailed reports, "
        f"{kpis['summary_only_executions']} from summary-only"
    )

    k1, k2, k3, k4, k5 = st.columns(5)
    k1.metric("Tasks", f"{kpis['total_tasks']:,}")
    k2.metric("Executions", f"{kpis['total_executions']:,}")
    k3.metric("Files Transferred", f"{kpis['files_transferred']:,}")
    k4.metric("Files Verified", f"{kpis['files_verified']:,}")
    k5.metric("Total Failures", f"{kpis['total_failed']:,}",
              delta=None if kpis["total_failed"] == 0 else f"{kpis['total_failed']:,}",
              delta_color="inverse")

    transfer_health = (
        kpis["transfer_ok"] / kpis["files_transferred"] * 100
        if kpis["files_transferred"] > 0 else 100
    )
    ver_rate = (
        kpis["verify_ok"] / kpis["files_verified"] * 100
        if kpis["files_verified"] > 0 else 0
    )

    st.markdown("---")
    h1, h2, h3, h4 = st.columns(4)
    h1.metric("Transfer Success Rate", f"{transfer_health:.1f}%")
    h2.metric("Verification Success Rate", f"{ver_rate:.1f}%")
    h3.metric("Transfer Failures", f"{kpis['transfer_failed']:,}")
    h4.metric("Net Data Moved", format_bytes(kpis["net_bytes_moved"]))

    if transfer_health < 100 and kpis["files_transferred"] > 0:
        st.progress(min(transfer_health / 100, 1.0),
                    text=f"Transfer Success Rate: {transfer_health:.1f}%")

    st.markdown("---")
    st.markdown(
        """
        #### Navigate

        | Page | What it shows |
        |------|---------------|
        | **Overview** | Unified KPIs, status distribution, top failures, management plane |
        | **Task Detail** | Per-task drill-down — execution history, API status, file records |
        | **File Explorer** | Cross-task file search, failure analysis, error code breakdown |
        | **Analytics** | Trends (day/hour), cumulative charts, file size analysis |
        | **Hourly Monitor** | AWS-style time range filters, CloudWatch metrics |
        """
    )

conn.close()
