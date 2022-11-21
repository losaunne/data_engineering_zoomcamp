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

URL_TEMPLATE = 'https://d37ci6vzurychx.cloudfront.net/misc/taxi+_zone_lookup.csv'
FILENAME = 'zones.csv'
OUTPUT_FILE_TEMPLATE = AIRFLOW_HOME + '/' + FILENAME
TABLE_NAME_TEMPLATE = 'zones_{{ execution_date.strftime(\'%Y_%m\') }}'
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'trips_data_all')


local_workflow = DAG(
    dag_id="zones_dag",
    start_date=days_ago(1),
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
                "tableId": FILENAME.replace('.csv', ''),
            },
            "externalDataConfiguration": {
                "sourceFormat": "CSV",
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
    