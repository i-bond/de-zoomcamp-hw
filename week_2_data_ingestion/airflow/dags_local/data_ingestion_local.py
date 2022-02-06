import os

from datetime import datetime

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from ingest_script import ingest_callable


AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/") # create env variable

# taken from .env ALT+J to select multiple "="
PG_HOST = os.getenv('PG_HOST')
PG_USER = os.getenv('PG_USER')
PG_PASSWORD = os.getenv('PG_PASSWORD')
PG_PORT = os.getenv('PG_PORT')
PG_DATABASE = os.getenv('PG_DATABASE')


local_workflow = DAG(
    "LocalIngestionDag",
    schedule_interval="0 6 2 * *", # 6AM 2nd day of every month
    start_date=datetime(2021, 1, 1),
    max_active_runs=1, # consecutive exection of catch up dags
)


URL_PREFIX = 'https://s3.amazonaws.com/nyc-tlc/trip+data' 
URL_TEMPLATE = URL_PREFIX + '/yellow_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.csv' # Jinja parametrization
OUTPUT_FILE_TEMPLATE = AIRFLOW_HOME + '/output_{{ execution_date.strftime(\'%Y-%m\') }}.csv' # wc -l output_2021-01.csv - check rows_n
TABLE_NAME_TEMPLATE = 'yellow_taxi_{{ execution_date.strftime(\'%Y_%m\') }}'


with local_workflow:
    wget_task = BashOperator(
        task_id='wget',
        bash_command=f'curl -sSL {URL_TEMPLATE} > {OUTPUT_FILE_TEMPLATE}' # saved inside the worker container
    )

    ingest_task = PythonOperator( # connect to db from container, USE Docker Network
        task_id="ingest",
        python_callable=ingest_callable,
        op_kwargs=dict(
            user=PG_USER,
            password=PG_PASSWORD,
            host=PG_HOST,
            port=PG_PORT,
            db=PG_DATABASE,
            table_name=TABLE_NAME_TEMPLATE,
            csv_file=OUTPUT_FILE_TEMPLATE
        ),
    )

    wget_task >> ingest_task

