from airflow import DAG
from datetime import datetime
from airflow.providers.standard.operators.python import PythonOperator


def run_database_sync(**context):
    from datalake.src.jobs.sync.trino_to_postgres_sink import DataSync

    date = datetime.strptime(context["ds"], "%Y-%m-%d").date()
    DataSync(date=date).insert_data_into_postgres()


with DAG(
    dag_id="database_sync",
    start_date=datetime(2026, 3, 1),
    schedule=None,
    catchup=False,
) as dag:
    sync = PythonOperator(task_id="database_sync", python_callable=run_database_sync)

    sync
