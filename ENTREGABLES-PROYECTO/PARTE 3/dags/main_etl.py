from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# Importar funciones  del ETL
from etl.utils import ping
from etl.extract import extract
from etl.transform import transform
from etl.great_expectations import great_expectations
from etl.load import load
from etl.analytics import analytics


default_args = {
    "owner": "etl-team",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="main_etl",
    default_args=default_args,
    start_date=datetime(2025, 10, 1),
    schedule_interval=None,   # manual por ahora
    catchup=False,
    description="Pipeline ETL con prueba de ping",
    tags=["etl", "base"],
) as dag:

    t_ping = PythonOperator(
        task_id="ping",
        python_callable=ping,
    )

    t_extract = PythonOperator(
        task_id="extract",
        python_callable=extract,
    )

    t_transform = PythonOperator(
        task_id="transform",
        python_callable=transform,
    )

    t_great_expectations = PythonOperator(
        task_id="great_expectations",
        python_callable=great_expectations,
    )

    t_load = PythonOperator(
        task_id="load",
        python_callable=load,
    )

    t_analytics = PythonOperator(
        task_id="analytics",
        python_callable=analytics,
    )



    # Orden de ejecución
    t_ping >> t_extract >> t_transform >> t_great_expectations >> t_load >> t_analytics
