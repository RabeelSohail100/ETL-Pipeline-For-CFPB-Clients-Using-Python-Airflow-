from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

from cfpb_project.scripts.extract_data import extract
from cfpb_project.scripts.transform_data import transform
from cfpb_project.scripts.load_data_gs import load_to_gsheet

default_args = {
    'owner': 'rabeel',
    'start_date': datetime(2025, 2, 24),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='cfpb_etl_dag',
    default_args=default_args,
    schedule_interval="0 12 * * *",  # Runs daily at 12 PM
    catchup=False,
) as dag:

    start = EmptyOperator(task_id="start")

    extract_task = PythonOperator(
        task_id="extract_data_task",
        python_callable=extract
    )

    transform_task = PythonOperator(
        task_id="transform_data_task",
        python_callable=transform
    )

    load_gsheets_task = PythonOperator(
        task_id="load_gsheets_task",
        python_callable=load_to_gsheet
    )

    end = EmptyOperator(task_id="end")

    # DAG Dependencies
    start >> extract_task >> transform_task >> load_gsheets_task >> end
