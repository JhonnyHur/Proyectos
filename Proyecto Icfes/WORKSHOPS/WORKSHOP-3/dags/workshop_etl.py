from __future__ import annotations
from datetime import datetime, timedelta
from pathlib import Path
import sys
from airflow import DAG
from airflow.operators.python import PythonOperator

# Rutas y módulos ETL

sys.path.append("/opt/airflow/etl")
from extract import read_all
from transform import transform
from load import save_csv, save_sqlite
from eda_output import run_eda  # 👈 tu nuevo EDA modular


# Directorios base

DATA = Path("/opt/airflow/data")
STAGE = DATA / "stage"
OUT = DATA / "output"
STAGE.mkdir(parents=True, exist_ok=True)
OUT.mkdir(parents=True, exist_ok=True)

# Configuración DAG

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="workshop_etl",
    description="Pipeline ETL + EDA automático (versión final modular)",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule=None,   # ejecución manual
    catchup=False,
    max_active_runs=1,
    tags=["workshop", "etl", "eda"],
) as dag:

    def _extract(**context):
        tx, cu, pr, fx, cr = read_all()
        tx.to_parquet(STAGE / "tx.parquet", index=False)
        cu.to_parquet(STAGE / "cu.parquet", index=False)
        pr.to_parquet(STAGE / "pr.parquet", index=False)
        fx.to_parquet(STAGE / "fx.parquet", index=False)
        cr.to_parquet(STAGE / "cr.parquet", index=False)
        print("[EXTRACT] Datos guardados en /data/stage")

    def _transform(**context):
        import pandas as pd
        tx = pd.read_parquet(STAGE / "tx.parquet")
        cu = pd.read_parquet(STAGE / "cu.parquet")
        pr = pd.read_parquet(STAGE / "pr.parquet")
        fx = pd.read_parquet(STAGE / "fx.parquet")
        cr = pd.read_parquet(STAGE / "cr.parquet")

        fact, daily = transform(tx, cu, pr, fx, cr)
        fact.to_parquet(STAGE / "fact.parquet", index=False)
        daily.to_parquet(STAGE / "daily.parquet", index=False)
        print("[TRANSFORM] fact.parquet y daily.parquet listos")

    def _load(**context):
        import pandas as pd
        fact = pd.read_parquet(STAGE / "fact.parquet")
        daily = pd.read_parquet(STAGE / "daily.parquet")
        save_csv(fact, daily)
        save_sqlite(fact, daily)
        print("[LOAD] Archivos guardados en /data/output")

    def _eda_output(**context):
        run_eda(OUT)  # 👈 llama directamente a tu eda_output.py
        print("[EDA] Análisis Exploratorio completado.")

    # Definición de tareas y dependencias

    t_extract = PythonOperator(task_id="extract", python_callable=_extract)
    t_transform = PythonOperator(task_id="transform", python_callable=_transform)
    t_load = PythonOperator(task_id="load", python_callable=_load)
    t_eda = PythonOperator(task_id="eda_output", python_callable=_eda_output)

    t_extract >> t_transform >> t_load >> t_eda