"""CloudWatch metrics client for EC2 instance and DataSync agent monitoring."""

from __future__ import annotations

from datetime import datetime, timedelta

import boto3
import pandas as pd

import config


def _get_cw_client():
    if config.AWS_PROFILE:
        session = boto3.Session(profile_name=config.AWS_PROFILE, region_name=config.AWS_REGION)
    else:
        session = boto3.Session(region_name=config.AWS_REGION)
    return session.client("cloudwatch")


def _fetch_metric(
    client,
    namespace: str,
    metric_name: str,
    dimensions: list[dict],
    start: datetime,
    end: datetime,
    period: int = 300,
    stat: str = "Average",
) -> pd.DataFrame:
    response = client.get_metric_statistics(
        Namespace=namespace,
        MetricName=metric_name,
        Dimensions=dimensions,
        StartTime=start,
        EndTime=end,
        Period=period,
        Statistics=[stat],
    )
    points = response.get("Datapoints", [])
    if not points:
        return pd.DataFrame(columns=["timestamp", "value"])
    df = pd.DataFrame(points)
    df = df.rename(columns={"Timestamp": "timestamp", stat: "value"})
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True).dt.tz_convert(
        "Asia/Kolkata"
    )
    df = df[["timestamp", "value"]].sort_values("timestamp").reset_index(drop=True)
    return df


def get_ec2_metrics(
    instance_id: str,
    start: datetime,
    end: datetime,
    period: int = 300,
) -> dict[str, pd.DataFrame]:
    client = _get_cw_client()
    dims = [{"Name": "InstanceId", "Value": instance_id}]
    metrics = {}
    for metric_name, stat in [
        ("CPUUtilization", "Average"),
        ("NetworkIn", "Sum"),
        ("NetworkOut", "Sum"),
        ("DiskReadBytes", "Sum"),
        ("DiskWriteBytes", "Sum"),
    ]:
        metrics[metric_name] = _fetch_metric(
            client, "AWS/EC2", metric_name, dims, start, end, period, stat,
        )
    return metrics


def get_datasync_agent_metrics(
    agent_arn: str,
    start: datetime,
    end: datetime,
    period: int = 300,
) -> dict[str, pd.DataFrame]:
    client = _get_cw_client()
    dims = [{"Name": "AgentId", "Value": agent_arn}]
    metrics = {}
    for metric_name, stat in [
        ("BytesTransferred", "Sum"),
        ("FilesTransferred", "Sum"),
        ("BytesVerifiedSource", "Sum"),
        ("BytesVerifiedDestination", "Sum"),
    ]:
        metrics[metric_name] = _fetch_metric(
            client, "AWS/DataSync", metric_name, dims, start, end, period, stat,
        )
    return metrics


def compute_period(start: datetime, end: datetime) -> int:
    span = end - start
    if span <= timedelta(hours=3):
        return 60
    if span <= timedelta(hours=12):
        return 300
    if span <= timedelta(days=3):
        return 900
    if span <= timedelta(days=7):
        return 3600
    return 3600
