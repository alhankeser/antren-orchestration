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
GOOGLE_CLOUD_LOCATION = os.getenv("GOOGLE_CLOUD_LOCATION")
GOOGLE_CLOUD_CONNECTION_ID = os.getenv("GOOGLE_CLOUD_CONNECTION_ID")


with DAG(
    "antren",
    schedule=timedelta(days=1),
    start_date=datetime(2024, 1, 29),
    catchup=False,
) as dag:
    get_latest_activities = CloudRunExecuteJobOperator(
        task_id="get_latest_activities",
        job_name="app",
        project_id=PROJECT_ID,
        region=GOOGLE_CLOUD_LOCATION,
        gcp_conn_id=GOOGLE_CLOUD_CONNECTION_ID,
    )

    load_gcs_to_bigquery = GCSToBigQueryOperator(
        task_id="load_gcs_to_bigquery",
        source_objects=["*.parquet"],
        source_format="PARQUET",
        skip_leading_rows=1,
        destination_project_dataset_table=f"{DATASET_NAME}.{TABLE_NAME}",
        write_disposition="WRITE_TRUNCATE",
        project_id=PROJECT_ID,
        bucket=BUCKET_NAME,
        location=GOOGLE_CLOUD_LOCATION,
        gcp_conn_id=GOOGLE_CLOUD_CONNECTION_ID,
    )

    run_dbt = CloudRunExecuteJobOperator(
        task_id="run_dbt",
        job_name="dbt",
        project_id=PROJECT_ID,
        region=GOOGLE_CLOUD_LOCATION,
        gcp_conn_id=GOOGLE_CLOUD_CONNECTION_ID,
    )

    get_latest_activities >> load_gcs_to_bigquery >> run_dbt
