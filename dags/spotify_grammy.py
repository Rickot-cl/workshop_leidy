from datetime import datetime, timedelta
import os
import sys
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

sys.path.append("/opt/airflow/scripts")

default_args = {
    "owner": "data_engineer",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


def task_seed_grammys():
    from data_analyze import grammys_db
    grammys_db()

def task_validate_spotify():
    from data_quality import validate_spotify_data
    validate_spotify_data("/opt/airflow/data/datasetspotify.csv")

def task_transform_and_merge():
    from transformation import (
        transform_spotify,
        extract_and_transform_grammys,
        merge_spotify_and_grammys,
    )
    from data_quality import validate_spotify_data

    df_spotify_raw = validate_spotify_data("/opt/airflow/data/spotify.csv")
    df_spotify_clean = transform_spotify(df_spotify_raw)

    df_grammys_clean = extract_and_transform_grammys()

    df_merged = merge_spotify_and_grammys(df_spotify_clean, df_grammys_clean)

    os.makedirs("/opt/airflow/data/processed", exist_ok=True)
    df_merged.to_csv("/opt/airflow/data/processed/final_dataset.csv", index=False)
    print("Datos transformados y combinados.")

def task_load_final_to_postgres():
    import pandas as pd
    from sqlalchemy import create_engine

    db_uri = os.getenv(
        "AIRFLOW_CONN_POSTGRES_DEFAULT",
        "postgresql://airflow:airflow_password@postgres:5432/target_db",
    )
    df_final = pd.read_csv("/opt/airflow/data/processed/final_dataset.csv")

    engine = create_engine(db_uri)
    df_final.to_sql("spotify_grammys_analytics", con=engine, if_exists="replace", index=False)
    print(f"✔ Tabla 'spotify_grammys_analytics' creada en PostgreSQL con {len(df_final)} filas.")

with DAG(
    dag_id="spotify_grammys_etl_pipeline",
    default_args=default_args,
    description="Pipeline ETL automatizado que combina datos de Spotify y Grammys",
    schedule_interval="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["etl", "spotify", "grammys", "pandera", "postgres"],
) as dag:
    
    t1_seed_grammys = PythonOperator(
        task_id="seed_grammys_db",
        python_callable=task_seed_grammys,
    )

    t2_validate_spotify = PythonOperator(
        task_id="validate_spotify_quality",
        python_callable=task_validate_spotify,
    )

    t3_transform_merge = PythonOperator(
        task_id="transform_and_merge_datasets",
        python_callable=task_transform_and_merge,
    )

    t4_load_postgres = PythonOperator(
        task_id="load_final_to_postgres",
        python_callable=task_load_final_to_postgres,
    )

    [t1_seed_grammys, t2_validate_spotify] >> t3_transform_merge >> t4_load_postgres