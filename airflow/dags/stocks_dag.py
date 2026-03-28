from airflow import DAG
from datetime import datetime
from airflow.providers.standard.operators.python import PythonOperator


def run_raw(**context):
    from datalake.src.jobs.stocks.raw.stocks_job import RawStocksJob

    date = datetime.strptime(context["ds"], "%Y-%m-%d").date()

    RawStocksJob(date=date).run()


def run_curated(**context):
    from datalake.src.jobs.stocks.curated.stocks_job import CuratedStocks

    date = datetime.strptime(context["ds"], "%Y-%m-%d").date()

    CuratedStocks(date).run()


with DAG(
    dag_id="stocks_pipeline",
    start_date=datetime(2026, 3, 1),
    schedule="@daily",
    catchup=False,
) as dag:
    raw = PythonOperator(task_id="raw_stocks", python_callable=run_raw)

    curated = PythonOperator(task_id="curated_stocks", python_callable=run_curated)

    raw >> curated
