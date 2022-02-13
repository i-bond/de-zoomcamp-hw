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

# AIRFLOW_HOME="/opt/airflow/"
# OUTPUT_FILE_NAME="yellow_taxi_2021-01.csv"

URL_PREFIX = 'https://s3.amazonaws.com/nyc-tlc/trip+data'
URL_TEMPLATE = URL_PREFIX + '/green_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.csv' # Jinja parametrization
CSV_NAME = 'green_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.csv' # wc -l output_2021-01.csv - check rows_n

OBJECT_NAME_PARQUET = CSV_NAME.replace('.csv', '.parquet')
OUTPUT_FILE_PARQUET = AIRFLOW_HOME + "/" + OBJECT_NAME_PARQUET


def format_to_parquet(csv_file): # convert .csv to parquet
    print(f"CSV FILE NAME: {csv_file}")
    if not csv_file.endswith('.csv'):
        logging.error("Can only accept source files in CSV format, for the moment")
        return
    table = pv.read_csv(csv_file)
    pq.write_table(table, csv_file.replace('.csv', '.parquet'))


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
    print('Dataset Uploaded!')


default_args = {
    "owner": "airflow",
    "start_date": datetime(2019, 1, 1),
    "end_date":  datetime(2020, 12, 1),
    "depends_on_past": True,
    "retries": 1,
}


with DAG(
    dag_id="green_taxi_dag", # unique id
    schedule_interval="@monthly",
    default_args=default_args,
    catchup=True,
    max_active_runs=1,
    tags=['4-analytics-engineering'],
) as dag:
    download_dataset = BashOperator(
        task_id="download_dataset_green", # unique
        bash_command=f"curl -sSL {URL_TEMPLATE} > {AIRFLOW_HOME + '/' + CSV_NAME} && " \
                     f"echo {AIRFLOW_HOME + '/' + CSV_NAME}",
    )

    check_row_count = BashOperator(  # bash scripting -> row_count, # use only f"<string>"
        task_id="check_green",
        bash_command=f"ROW_N=( $(wc -l {AIRFLOW_HOME + '/' + CSV_NAME}) ) && " \
                     f"echo $ROW_N",
        do_xcom_push=False,
    )

    format_to_parquet=PythonOperator(
        task_id="format_to_parquet_green",
        python_callable=format_to_parquet,
        op_kwargs={
            "csv_file": f"{AIRFLOW_HOME}/{CSV_NAME}",
        },
    )

    load_to_gcs = PythonOperator(
        task_id="raw_data_green",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": f"raw_data_green/{OBJECT_NAME_PARQUET}",
            "local_file": f"{OUTPUT_FILE_PARQUET}",
        },
    )

    clear_space = BashOperator(
        task_id="clear_space_green",
        bash_command=f"CSV_FILE={AIRFLOW_HOME}/{CSV_NAME} && " \
                     f"PARQUET_FILE={AIRFLOW_HOME}/{OBJECT_NAME_PARQUET} && " \
                     "rm $CSV_FILE $PARQUET_FILE",
        do_xcom_push=False,
    )

download_dataset >> check_row_count >> format_to_parquet >> load_to_gcs >> clear_space



