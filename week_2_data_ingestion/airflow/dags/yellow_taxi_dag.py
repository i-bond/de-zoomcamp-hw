import os
import logging

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from google.cloud import storage
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
import pyarrow.csv as pv
import pyarrow.parquet as pq
from datetime import datetime


PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

URL_PREFIX = 'https://s3.amazonaws.com/nyc-tlc/trip+data'
URL_TEMPLATE = URL_PREFIX + '/yellow_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.csv' # Jinja parametrization
OUTPUT_FILE_TEMPLATE = AIRFLOW_HOME + '/yellow_taxi_{{ execution_date.strftime(\'%Y-%m\') }}.csv' # wc -l output_2021-01.csv - check rows_n
TABLE_NAME_TEMPLATE = 'yellow_taxi_{{ execution_date.strftime(\'%Y_%m\') }}'
# CONVERTED
OUTPUT_FILE_PARQUET = OUTPUT_FILE_TEMPLATE.replace('.csv', '.parquet')
OBJECT_NAME_PARQUET = OUTPUT_FILE_PARQUET.split('/')[-1]

# TODO: pass converted files as xcoms

def format_to_parquet(src_file): # convert .csv to parquet
    if not src_file.endswith('.csv'):
        logging.error("Can only accept source files in CSV format, for the moment")
        return
    table = pv.read_csv(src_file)
    pq.write_table(table, src_file.replace('.csv', '.parquet'))


# NOTE: takes 20 mins, at an upload speed of 800kbps. Faster if your internet has a better upload speed
def upload_to_gcs(bucket, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    :param bucket: GCS bucket name
    :param object_name: target path & file-name
    :param local_file: source path & file-name
    :return:
    """
    print(f"Uploading {object_name} to {bucket} from local {local_file}")
    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    # End of Workaround

    client = storage.Client()
    bucket = client.bucket(bucket)

    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)

default_args = {
    "owner": "airflow",
    "start_date": datetime(2019, 1, 1),
    "end_date":  datetime(2020, 12, 1),
    "depends_on_past": True,
    "retries": 1,
}

# NOTE: DAG declaration - using a Context Manager (an implicit way)
with DAG(
    dag_id="yellow_taxi_dag", # unique id
    schedule_interval="@monthly",
    default_args=default_args,
    catchup=True,
    max_active_runs=1,
    tags=['2-data-ingestion'],
) as dag:
    # download
    # parquetize
    # parquetize
    # upload to GCS
    download_dataset = BashOperator(
        task_id="download_dataset", # unique
        bash_command=f"curl -sSL {URL_TEMPLATE} > {OUTPUT_FILE_TEMPLATE}"
    )

    check_dataset = BashOperator(
        task_id="check_dataset",
        bash_command=f"wc -l {OUTPUT_FILE_TEMPLATE}"
    )

    format_to_parquet = PythonOperator(
        task_id="format_to_parquet_task",
        python_callable=format_to_parquet,
        op_kwargs={
            "src_file": f"{OUTPUT_FILE_TEMPLATE}",
        },
    )

    # TODO: Homework - research and try XCOM to communicate output values between 2 tasks/operators
    load_to_gcs = PythonOperator(
        task_id="local_to_gcs", # keep same as task name
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": f"raw_data/{OBJECT_NAME_PARQUET}",
            "local_file": f"{OUTPUT_FILE_PARQUET}",
        },
    )

download_dataset >> check_dataset >> format_to_parquet >> load_to_gcs