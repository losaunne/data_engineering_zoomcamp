import os

from datetime import datetime

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from google.cloud import storage
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator, BigQueryUpsertTableOperator
import pyarrow.csv as pv
import pyarrow.parquet as pq

# from data_ingest_script import ingest_callable
from upload_gcs import upload_to_gcs


AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

URL_PREFIX = 'https://d37ci6vzurychx.cloudfront.net/trip-data/' 
FILENAME = 'yellow_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
URL_TEMPLATE = URL_PREFIX + FILENAME
OUTPUT_FILE_TEMPLATE = AIRFLOW_HOME + '/yellow_taxi_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
TABLE_NAME_TEMPLATE = 'yellow_taxi_{{ execution_date.strftime(\'%Y_%m\') }}'
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'trips_data_all')


local_workflow = DAG(
    dag_id="yellow_taxi_dag",
    start_date=datetime(2019, 1, 2),
    end_date=datetime(2020, 12, 31),
    schedule_interval="0 6 2 * *",
    max_active_runs=1,
    catchup=True
)

# NOTE: DAG declaration - using a Context Manager (an implicit way)
with local_workflow:
    # print_params = BashOperator(
    #     task_id="print_params",
    #     bash_command="echo execution date: {{ execution_date }}"
    # )

    download_dataset = BashOperator(
        task_id="download_dataset",
        bash_command=f"curl -sSL {URL_TEMPLATE} > {OUTPUT_FILE_TEMPLATE}"
    )

    local_to_gcs = PythonOperator(
        task_id="local_to_gcs",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": f"raw/{FILENAME}",
            "local_file": f"{OUTPUT_FILE_TEMPLATE}",
        },
    )

    bigquery_create_table = BigQueryCreateExternalTableOperator(
        task_id="bigquery_create_table",
        table_resource={
            "tableReference": {
                "projectId": PROJECT_ID,
                "datasetId": BIGQUERY_DATASET,
                "tableId": FILENAME.replace('.parquet', ''),
            },
            "externalDataConfiguration": {
                "sourceFormat": "PARQUET",
                "sourceUris": [f"gs://{BUCKET}/raw/{FILENAME}"],
                "autodetect": True
            },
        },
    )

    clean_up_file = BashOperator(
        task_id="clean_up_file",
        bash_command=f'rm {OUTPUT_FILE_TEMPLATE}'
    )

    download_dataset >> local_to_gcs >> bigquery_create_table >> clean_up_file
    