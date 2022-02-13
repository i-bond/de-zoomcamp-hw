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


# URL_PREFIX = 'https://s3.amazonaws.com/nyc-tlc/trip+data'
# URL_TEMPLATE = URL_PREFIX + '/yellow_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.csv' # Jinja parametrization
# OUTPUT_FILE_NAME = 'yellow_taxi_{{ execution_date.strftime(\'%Y-%m\') }}.csv' # wc -l output_2021-01.csv - check rows_n
# TABLE_NAME = 'yellow_taxi_{{ execution_date.strftime(\'%Y_%m\') }}'


URL_PREFIX = 'https://s3.amazonaws.com/nyc-tlc/trip+data'
URL_TEMPLATE = URL_PREFIX + '/fhv_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.csv' # Jinja parametrization
OUTPUT_FILE_NAME = 'fhv_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.csv' # wc -l output_2021-01.csv - check rows_n


# TODO: pass converted files as xcoms
def format_to_parquet(ti): # convert .csv to parquet
    csv_file = ti.xcom_pull(task_ids="download_dataset_fhv")
    print(f"CSV FILE NAME: {csv_file}")
    # if not src_file.endswith('.csv'):
    #     logging.error("Can only accept source files in CSV format, for the moment")
    #     return
    table = pv.read_csv(csv_file)
    pq.write_table(table, csv_file.replace('.csv', '.parquet'))
    return csv_file.replace('.csv', '.parquet') # will be added to xcoms with default return_value
    # ti.xcom_push(key="parquet_file_fhv", value=)

# NOTE: takes 20 mins, at an upload speed of 800kbps. Faster if your internet has a better upload speed
def upload_to_gcs(ti):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    :param bucket: GCS bucket name
    :param object_name: target path & file-name
    :param local_file: source path & file-name
    :return:
    """
    local_file = ti.xcom_pull(task_ids="format_to_parquet_fhv")
    object_name = "raw_data_fhv/" + local_file.split('/')[-1]
    bucket = BUCKET

    print(f"Uploading {object_name} to {bucket} from local {object_name}")
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
    "start_date": datetime(2019, 1, 1), # 2019 #test - datetime(2020, 4-6, 1)
    "end_date":  datetime(2019, 12, 1),
    "depends_on_past": True,
    "retries": 1,
}


with DAG(
    dag_id="fhv_dag_xcom.", # unique id
    schedule_interval="@monthly",
    default_args=default_args,
    catchup=True,
    max_active_runs=1,
    tags=['2-data-ingestion'],
) as dag:
    download_dataset = BashOperator(
        task_id="download_dataset_fhv", # unique
        bash_command=f"curl -sSL {URL_TEMPLATE} > {AIRFLOW_HOME + '/' + OUTPUT_FILE_NAME} && " \
                     f"echo {AIRFLOW_HOME + '/' + OUTPUT_FILE_NAME}",
    )

    check_row_count = BashOperator( # bash scripting -> row_count, # use only f"<string>"
        task_id="check_dataset_fhv",
        bash_command='CSV_FILE={{ ti.xcom_pull(task_ids="download_dataset_fhv") }} && ' \
                     f"ROW_N=( $(wc -l $CSV_FILE) ) && " \
                     f"echo $ROW_N",
        do_xcom_push=False,
    )

    format_to_parquet = PythonOperator(
        task_id="format_to_parquet_fhv",
        python_callable=format_to_parquet,
    )

    load_to_gcs = PythonOperator(
        task_id="load_to_gcs",
        python_callable=upload_to_gcs,
        # op_kwargs={
        #     "bucket": BUCKET,
        #     "object_name": f"test_folder/{OBJECT_NAME_PARQUET}",
        #     "local_file": f"{OUTPUT_FILE_PARQUET}",
        # },
    )

    clear_space = BashOperator(
        task_id="clear_space_fhv",
        bash_command="CSV_FILE={{ ti.xcom_pull(task_ids='download_dataset_fhv') }} && " \
                     "PARQUET_FILE={{ ti.xcom_pull(task_ids='format_to_parquet_fhv') }} && " \
                     "rm $CSV_FILE $PARQUET_FILE",
        do_xcom_push=False,
    )

download_dataset >> check_row_count >> format_to_parquet >> load_to_gcs >> clear_space



# bash commands
# SOME_O=$(wc -l /opt/airflow/yellow_taxi_2020-05.csv) && echo ${SOME_O[0]}
# arr=($SOME_O)
# echo ${arr[0]}