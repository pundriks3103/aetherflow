"""Scheduled Bronze-to-Silver transformation."""

import os
import sys
from datetime import datetime, timedelta

sys.path.insert(0, "/opt/aetherflow/scripts")

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator

from aetherflow_config import load_config


config = load_config(context="compose")
airflow_config = config["airflow"]

with DAG(
    dag_id=airflow_config["dag_id"],
    schedule=airflow_config["schedule"],
    start_date=datetime(2026, 1, 1),
    catchup=airflow_config["catchup"],
    max_active_runs=airflow_config["max_active_runs"],
    default_args={"retries": airflow_config["retries"], "retry_delay": timedelta(seconds=airflow_config["retry_delay_seconds"])},
    tags=["aetherflow", "silver", "delta"],
) as dag:
    silver_transform = DockerOperator(
        task_id="silver_transform",
        image="aetherflow-spark:3.5.1-delta3.2.0",
        command=["/opt/spark/bin/spark-submit", "--master", config["spark"]["master"], "/opt/aetherflow/scripts/silver_transform.py"],
        docker_url="unix://var/run/docker.sock",
        network_mode="aetherflow_aetherflow-net",
        environment={
            "AWS_ACCESS_KEY_ID": os.environ["S3_ACCESS_KEY"],
            "AWS_SECRET_ACCESS_KEY": os.environ["S3_SECRET_KEY"],
            "AETHERFLOW_CONTEXT": "compose",
            "AETHERFLOW_CONFIG": "/opt/aetherflow/config/aetherflow.yml",
            "S3_ENDPOINT": config["s3a"]["endpoint"],
        },
        mount_tmp_dir=False,
        auto_remove="success",
        user="0:0",
    )
