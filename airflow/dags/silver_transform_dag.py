"""Hourly idempotent Bronze-to-Silver transformation."""

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator


with DAG(
    dag_id="aetherflow_silver_transform",
    schedule="0 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(seconds=5)},
    tags=["aetherflow", "silver", "delta"],
) as dag:
    silver_transform = DockerOperator(
        task_id="silver_transform",
        image="aetherflow-spark:3.5.1-delta3.2.0",
        command=[
            "/opt/spark/bin/spark-submit",
            "--master",
            "spark://spark-master:7077",
            "/opt/aetherflow/scripts/silver_transform.py",
        ],
        docker_url="unix://var/run/docker.sock",
        network_mode="aetherflow_aetherflow-net",
        environment={
            "AWS_ACCESS_KEY_ID": os.environ["S3_ACCESS_KEY"],
            "AWS_SECRET_ACCESS_KEY": os.environ["S3_SECRET_KEY"],
            "S3_ENDPOINT": os.environ.get("S3_ENDPOINT", "http://seaweedfs:8333"),
        },
        mount_tmp_dir=False,
        auto_remove="success",
        user="0:0",
    )
