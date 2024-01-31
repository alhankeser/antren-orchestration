import os
from datetime import datetime, timedelta
from dotenv import load_dotenv
from airflow.models.dag import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)
from airflow.providers.google.cloud.operators.cloud_run import (
    CloudRunExecuteJobOperator,
)

load_dotenv()

PROJECT_ID = os.getenv("GOOGLE_CLOUD_PROJECT_ID")
BUCKET_NAME = os.getenv("CLOUD_STORAGE_BUCKET")
DATASET_NAME = os.getenv("BIGQUERY_DATASET_NAME")
TABLE_NAME = os.getenv("BIGQUERY_TABLE_NAME")

with DAG(
    "antren",
    schedule=timedelta(days=1),
    start_date=datetime(2024, 1, 29),
    catchup=False,
) as dag:
    get_latest_activities = CloudRunExecuteJobOperator(
        task_id="get_latest_activities",
        project_id=PROJECT_ID,
        job_name="app",
        region="europe-west9",
        gcp_conn_id="gcp",
    )

    load_gcs_to_bigquery = GCSToBigQueryOperator(
        task_id="load_gcs_to_bigquery",
        bucket=BUCKET_NAME,
        source_objects=["*.parquet"],
        source_format="PARQUET",
        skip_leading_rows=1,
        destination_project_dataset_table=f"{DATASET_NAME}.{TABLE_NAME}",
        write_disposition="WRITE_TRUNCATE",
        location="europe-west9",
        gcp_conn_id="gcp",
    )

    run_dbt = CloudRunExecuteJobOperator(
        task_id="run_dbt",
        project_id=PROJECT_ID,
        job_name="dbt",
        region="europe-west9",
        gcp_conn_id="gcp",
    )

    get_latest_activities >> load_gcs_to_bigquery >> run_dbt
