"""File Explorer — cross-task file-level search with failure analysis."""

import streamlit as st
import plotly.express as px
import plotly.graph_objects as go

from db import (
    get_connection, file_detail, file_detail_count,
    task_summary, error_code_distribution,
)

st.set_page_config(page_title="File Explorer | DataSync Monitor", page_icon="📂", layout="wide")

C_TRANSFERRED = "#6C9EFF"
C_VERIFIED    = "#4ADE80"
C_FAILED      = "#F87171"
C_SKIPPED     = "#FBBF24"
C_DELETED     = "#A78BFA"

conn = get_connection()

st.markdown("## File Explorer")
st.caption("Search and filter individual file records across all tasks and executions.")

# ── filters ──────────────────────────────────────────────────────────────────
df_tasks = task_summary(conn)
task_names = ["All"] + sorted(df_tasks["task_name"].tolist()) if not df_tasks.empty else ["All"]

col_task, col_type, col_status, col_search = st.columns([1, 1, 1, 2])
with col_task:
    sel_task = st.selectbox("Task", task_names, key="fe_task")
with col_type:
    sel_type = st.selectbox("Report Type", ["All", "transferred", "verified", "skipped", "deleted"], key="fe_type")
with col_status:
    sel_status = st.selectbox("Status", ["All", "SUCCESS", "FAILED"], key="fe_status")
with col_search:
    file_search = st.text_input("Search file path", placeholder="e.g. parquet, .csv, schema", key="fe_search")

task_f = sel_task if sel_task != "All" else None
type_f = sel_type if sel_type != "All" else None
status_f = sel_status if sel_status != "All" else None
search_f = file_search if file_search else None

total = file_detail_count(conn, task_f, None, type_f, status_f, search_f)

st.divider()

# ── quick stats for current filter ───────────────────────────────────────────
if total > 0:
    s1, s2, s3, s4, s5 = st.columns(5)
    s1.metric("Files Transferred", f"{file_detail_count(conn, task_f, None, 'transferred', None, search_f):,}")
    s2.metric("Files Verified", f"{file_detail_count(conn, task_f, None, 'verified', None, search_f):,}")
    s3.metric("Total Failures", f"{file_detail_count(conn, task_f, None, None, 'FAILED', search_f):,}")
    s4.metric("Files Skipped", f"{file_detail_count(conn, task_f, None, 'skipped', None, search_f):,}")
    s5.metric("Files Deleted", f"{file_detail_count(conn, task_f, None, 'deleted', None, search_f):,}")

    st.divider()

# ── paginated results ────────────────────────────────────────────────────────
PAGE_SIZE = 200
total_pages = max(1, (total + PAGE_SIZE - 1) // PAGE_SIZE)

st.caption(f"{total:,} records found")

page = st.number_input("Page", min_value=1, max_value=total_pages, value=1, key="fe_page")
offset = (page - 1) * PAGE_SIZE

df = file_detail(conn, task_f, None, type_f, status_f, search_f, limit=PAGE_SIZE, offset=offset)

if df.empty:
    st.info("No records match the current filters.")
else:
    color_map = {
        "transferred": "background-color: rgba(108,158,255,0.12)",
        "verified": "background-color: rgba(74,222,128,0.12)",
        "skipped": "background-color: rgba(251,191,36,0.12)",
        "deleted": "background-color: rgba(167,139,250,0.12)",
    }

    def row_style(row):
        if row.get("status") == "FAILED":
            return ["background-color: rgba(248,113,113,0.18)"] * len(row)
        bg = color_map.get(row.get("report_type", ""), "")
        return [bg] * len(row)

    st.dataframe(
        df.style.apply(row_style, axis=1),
        use_container_width=True,
        height=min(len(df) * 35 + 40, 700),
    )
    st.caption(f"Page {page} of {total_pages}")

st.divider()

# ── failure analysis section ─────────────────────────────────────────────────
st.markdown("##### Failure Analysis")

failed_count = file_detail_count(conn, task_f, None, None, "FAILED", search_f)

if failed_count == 0:
    st.success("No failures in the current filter scope.")
else:
    st.warning(f"{failed_count:,} failed records in scope.")

    df_errors = error_code_distribution(conn, task_f)
    if not df_errors.empty:
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

    with st.expander(f"View failed records (up to 500)", expanded=False):
        df_failed = file_detail(conn, task_f, None, None, "FAILED", search_f, limit=500)
        display_cols = ["task_name", "execution_id", "report_type", "relative_path",
                        "error_code", "error_detail"]
        st.dataframe(
            df_failed[[c for c in display_cols if c in df_failed.columns]],
            use_container_width=True,
            height=min(len(df_failed) * 35 + 40, 500),
        )

conn.close()
