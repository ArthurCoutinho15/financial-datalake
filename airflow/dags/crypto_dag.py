from airflow import DAG
from datetime import datetime
from airflow.providers.standard.operators.python import PythonOperator


def run_crypto(**context):
    from datalake.src.jobs.crypto.raw.crypto_job import RawCryptoJob

    date = datetime.strptime(context["ds"], "%Y-%m-%d").date()
    RawCryptoJob(date=date).run()


def run_curated(**context):
    from datalake.src.jobs.crypto.curated.crypto_job import CuratedCryptoJob

    date = datetime.strptime(context["ds"], "%Y-%m-%d").date()
    CuratedCryptoJob(date=date).run()


with DAG(
    dag_id="crypto_pipeline",
    start_date=datetime(2026, 3, 1),
    schedule="@daily",
    catchup=False,
) as dag:
    raw = PythonOperator(task_id="raw_crypto", python_callable=run_crypto)

    curated = PythonOperator(task_id="curated_crypto", python_callable=run_curated)

    raw >> curated
