import os

from datetime import datetime

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from google.cloud import storage

from upload_gcs import upload_to_gcs

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

def download_upload_dag(dag, url, output_file, gcs_filename):
    with dag:
        download_dataset = BashOperator(
            task_id="download_dataset",
            bash_command=f"curl -sSLf {url} > {output_file}"
        )

        local_to_gcs = PythonOperator(
            task_id="local_to_gcs",
            python_callable=upload_to_gcs,
            op_kwargs={
                "bucket": BUCKET,
                "object_name": 'raw/' + gcs_filename,
                "local_file": f"{output_file}",
            },
        )

        clean_up_file = BashOperator(
            task_id="clean_up_file",
            bash_command=f'rm {output_file}'
        )

        download_dataset >> local_to_gcs >> clean_up_file

yellow_taxi_base_url = 'https://d37ci6vzurychx.cloudfront.net/trip-data/' 
yellow_taxi_filename = 'yellow_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
yellow_taxi_url = yellow_taxi_base_url + yellow_taxi_filename
yellow_taxi_output_file = f'{AIRFLOW_HOME}/{yellow_taxi_filename}'
yellow_taxi_gcs_filename = 'yellow_taxi/{{ execution_date.strftime(\'%Y\') }}/' + yellow_taxi_filename
yellow_taxi_dag = DAG(
    dag_id="yellow_taxi_dag",
    start_date=datetime(2019, 1, 2),
    end_date=datetime(2020, 12, 31),
    schedule_interval="0 6 2 * *",
    max_active_runs=1,
    catchup=True
)
download_upload_dag(yellow_taxi_dag, yellow_taxi_url, yellow_taxi_output_file,
                    yellow_taxi_gcs_filename)

green_taxi_base_url = 'https://d37ci6vzurychx.cloudfront.net/trip-data/' 
green_taxi_filename = 'green_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
green_taxi_url = green_taxi_base_url + green_taxi_filename
green_taxi_output_file = f'{AIRFLOW_HOME}/{green_taxi_filename}'
green_taxi_gcs_filename = 'green_taxi/{{ execution_date.strftime(\'%Y\') }}/' + green_taxi_filename

green_taxi_dag = DAG(
    dag_id="green_taxi_dag",
    start_date=datetime(2019, 1, 2),
    end_date=datetime(2020, 12, 31),
    schedule_interval="0 7 2 * *",
    max_active_runs=1,
    catchup=True
)
download_upload_dag(green_taxi_dag, green_taxi_url, green_taxi_output_file,
                    green_taxi_gcs_filename)                    

fhv_base_url = 'https://d37ci6vzurychx.cloudfront.net/trip-data/' 
fhv_filename = 'fhv_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
fhv_url = yellow_taxi_base_url + yellow_taxi_filename
fhv_output_file = f'{AIRFLOW_HOME}/{fhv_filename}'
fhv_gcs_filename = 'fhv/{{ execution_date.strftime(\'%Y\') }}/' + fhv_filename
fhv_dag = DAG(
    dag_id="fhv_dag",
    start_date=datetime(2019, 1, 2),
    end_date=datetime(2020, 12, 31),
    schedule_interval="0 8 2 * *",
    max_active_runs=1,
    catchup=True
)
download_upload_dag(fhv_dag, fhv_url, fhv_output_file, fhv_gcs_filename)

zones_url = 'https://d37ci6vzurychx.cloudfront.net/misc/taxi+_zone_lookup.csv'
zones_filename = 'zones.csv'
zones_output_file = f'{AIRFLOW_HOME}/{zones_filename}'
zones_dag = DAG(
    dag_id="zones_dag",
    schedule_interval="@once",
    start_date=days_ago(1),
    catchup=False
)
download_upload_dag(zones_dag, zones_url, zones_output_file, zones_filename)
