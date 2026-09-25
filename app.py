"""
DataSync Monitoring Dashboard
=============================
Streamlit + DuckDB monitoring solution for AWS DataSync blob-to-S3 migrations.
Supports both Enhanced and Basic mode JSON task reports automatically.

Launch:  uv run streamlit run app.py
"""

import streamlit as st

import config
from db import (
    get_connection, global_stats, ingestion_stats, purge_all,
    ingest_chunk_bulk, get_ingested_keys, get_watermark, set_watermark,
    summary_global_stats,
)
from s3_client import list_report_keys, download_and_ingest_streaming
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

    if st.button("🔄 Refresh Data (Incremental)", use_container_width=True, type="primary"):
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
stats = global_stats(conn)

if stats["total_tasks"] == 0:
    st.markdown("---")
    st.markdown(
        """
        ### Getting Started

        1. **Configure** your S3 bucket and report prefix in the sidebar
        2. **Ensure** your AWS credentials are available (environment variables, profile, or IAM role)
        3. Click **Refresh Data** to discover and ingest DataSync JSON reports
        4. Navigate to **Overview**, **Task Detail**, or **File Explorer** pages

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
        export DATASYNC_DB_PATH="datasync.duckdb"  # optional
        ```
        """
    )
else:
    st.markdown("---")

    k1, k2, k3, k4, k5 = st.columns(5)
    k1.metric("Tasks", f"{stats['total_tasks']:,}")
    k2.metric("Executions", f"{stats['total_executions']:,}")
    k3.metric("Files Transferred", f"{stats['transferred']:,}")
    k4.metric("Files Verified", f"{stats['verified']:,}")
    k5.metric("Total Failures", f"{stats['total_failed']:,}",
              delta=None if stats["total_failed"] == 0 else f"{stats['total_failed']:,}",
              delta_color="inverse")

    transfer_health = (
        stats["transfer_success"] / stats["transferred"] * 100
        if stats["transferred"] > 0 else 100
    )
    ver_rate = (
        stats["verify_success"] / stats["verified"] * 100
        if stats["verified"] > 0 else 0
    )
    total_bytes = stats["total_bytes"] or 0
    if total_bytes > 1_000_000_000_000:
        bytes_str = f"{total_bytes / 1_000_000_000_000:.2f} TB"
    elif total_bytes > 1_000_000_000:
        bytes_str = f"{total_bytes / 1_000_000_000:.2f} GB"
    elif total_bytes > 1_000_000:
        bytes_str = f"{total_bytes / 1_000_000:.1f} MB"
    else:
        bytes_str = f"{total_bytes:,.0f} B"

    st.markdown("---")
    h1, h2, h3, h4 = st.columns(4)
    h1.metric("Transfer Success Rate", f"{transfer_health:.1f}%")
    h2.metric("Verification Success Rate", f"{ver_rate:.1f}%")
    h3.metric("Transfer Failures", f"{stats['transfer_failed']:,}")
    h4.metric("Data Transferred", bytes_str)

    sum_stats = summary_global_stats(conn)
    if sum_stats.get("total_summaries", 0) > 0:
        st.markdown("---")
        st.caption("**From Summary Reports**")
        s1, s2, s3, s4, s5 = st.columns(5)
        s1.metric("Summary Reports", f"{sum_stats['total_summaries']:,}")
        s2.metric("Files Transferred (summary)", f"{sum_stats['sum_files_transferred']:,}")
        s3.metric("Bytes Written (summary)", format_bytes(sum_stats.get("sum_bytes_written")))
        s4.metric("Bytes Transferred (summary)", format_bytes(sum_stats.get("sum_bytes_transferred")))
        s5.metric("Failed Executions", f"{sum_stats['failed_executions']:,}",
                  delta=None if sum_stats["failed_executions"] == 0
                  else f"{sum_stats['failed_executions']:,}",
                  delta_color="inverse")

    st.markdown("---")
    st.markdown(
        """
        #### Navigate

        | Page | What it shows |
        |------|---------------|
        | **Overview** | High-level KPIs, status distribution, top failures, multi-execution tasks |
        | **Task Detail** | Per-task drill-down — execution history, file counts, per-file records |
        | **File Explorer** | Cross-task file search, failure analysis, error code breakdown |
        """
    )

conn.close()
