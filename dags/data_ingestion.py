import os
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from ingestion_script import ingest_data

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

# url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet"
URL_PREFIX = "https://d37ci6vzurychx.cloudfront.net/trip-data"

URL_YELLOW_TEMPLATE = URL_PREFIX + "/yellow_tripdata_{{ execution_date.strftime('%Y-%m') }}.parquet"

URL_GREEN_TEMPLATE = URL_PREFIX + "/green_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet"

OUTPUT_TEMPLATE_YELLOW = AIRFLOW_HOME + "/output_yellow{{ execution_date.strftime('%Y-%m') }}.parquet"

OUTPUT_TEMPLATE_GREEN = AIRFLOW_HOME + "/output_green{{ execution_date.strftime(\'%Y-%m\') }}.parquet"

TABLE_YELLOW_TEMPLATE = "yellow_taxi_data"
TABLE_GREEN_TEMPLATE = "green_taxi_data"


workflow = DAG(
    dag_id="IngestionDag",
    start_date=datetime(2023, 1, 1),
    end_date = datetime(2023, 3, 1),
    #the pattern below runs script at 06:00 on the 1st day of each month
    schedule_interval="0 6 1 * *"
    #max_active_runs=1,
    
)
with workflow:
    curl_task_yellow = BashOperator(
        task_id="curl_yellow",
        bash_command=f"curl -sSl {URL_YELLOW_TEMPLATE} > {OUTPUT_TEMPLATE_YELLOW}"
        # bash_command="echo \" {{ ds }} {{ execution_date.strftime('%Y-%m') }} \"",
    )
    curl_task_green = BashOperator(
        task_id="curl_green",
        bash_command=f"curl -sSl {URL_GREEN_TEMPLATE} > {OUTPUT_TEMPLATE_GREEN}"
        # bash_command="echo \" {{ ds }} {{ execution_date.strftime('%Y-%m') }} \"",
    )
    load_task_yellow = PythonOperator(
        task_id="load_yellow",
        python_callable=ingest_data,
        op_kwargs=dict(parquet_file=OUTPUT_TEMPLATE_YELLOW, table_name=TABLE_YELLOW_TEMPLATE),
    )
    load_task_green = PythonOperator(
        task_id="load_green",
        python_callable=ingest_data,
        op_kwargs=dict(parquet_file=OUTPUT_TEMPLATE_GREEN, table_name=TABLE_GREEN_TEMPLATE),
    )

curl_task_yellow >> load_task_yellow
curl_task_green >> load_task_green
