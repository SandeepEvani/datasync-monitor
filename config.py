import os
from datetime import timezone, timedelta

S3_BUCKET = os.environ.get("DATASYNC_REPORTS_BUCKET", "")
S3_PREFIX = os.environ.get("DATASYNC_REPORTS_PREFIX", "datasync-reports/")
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")
AWS_PROFILE = os.environ.get("AWS_PROFILE", None)
DB_PATH = os.environ.get("DATASYNC_DB_PATH", "datasync_monitor.duckdb")

REPORT_TYPE_KEYS = ["Transferred", "Skipped", "Verified", "Deleted"]

SCHEMA_VERSION = 4

IST = timezone(timedelta(hours=5, minutes=30))

# CloudWatch — EC2 instance IDs and DataSync agent ARNs (comma-separated)
EC2_INSTANCE_IDS = [
    i.strip() for i in os.environ.get("DATASYNC_EC2_INSTANCES", "").split(",") if i.strip()
]
DATASYNC_AGENT_ARNS = [
    a.strip() for a in os.environ.get("DATASYNC_AGENT_ARNS", "").split(",") if a.strip()
]

TASK_NAME_MAX_LEN = 28
