from airflow import DAG
from datetime import datetime
from airflow.providers.standard.operators.python import PythonOperator


def run_raw(**context):
    from datalake.src.jobs.moedas.raw.raw_coins import RawCoins

    date = datetime.strptime(context["ds"], "%Y-%m-%d").date()
    RawCoins(date=date).run()


def run_curated(**context):
    from datalake.src.jobs.moedas.curated.curated_coins import CuratedCoins

    date = datetime.strptime(context["ds"], "%Y-%m-%d").date()
    CuratedCoins(date=date).run()


with DAG(
    dag_id="coins_pipeline",
    start_date=datetime(2026, 3, 1),
    schedule=None,
    catchup=False,
) as dag:
    raw = PythonOperator(task_id="raw_crypto", python_callable=run_raw)

    curated = PythonOperator(task_id="curated_crypto", python_callable=run_curated)

    raw >> curated
