from airflow import DAG
from datetime import datetime
from airflow.providers.standard.operators.python import PythonOperator


def run_raw_clients(**context):
    from datalake.src.jobs.clients.clients.raw.clients_job import RawClientsJob

    date = datetime.strptime(context["ds"], "%Y-%m-%d").date()

    RawClientsJob(date=date).run()


def run_raw_portfolios(**context):
    from datalake.src.jobs.clients.portfolios.raw.portolios_job import RawPortfoliosJob

    date = datetime.strptime(context["ds"], "%Y-%m-%d").date()

    RawPortfoliosJob(date).run()


def run_raw_positions(**context):
    from datalake.src.jobs.clients.positions.raw.positions_job import RawPositionsJob

    date = datetime.strptime(context["ds"], "%Y-%m-%d").date()

    RawPositionsJob(date).run()


def run_raw_transactions(**context):
    from datalake.src.jobs.clients.transactions.raw.transactions_job import (
        RawTransactionsJob,
    )

    date = datetime.strptime(context["ds"], "%Y-%m-%d").date()

    RawTransactionsJob(date).run()


def run_curated_clients(**context):
    from datalake.src.jobs.clients.clients.curated.clients_job import CuratedClientsJob

    date = datetime.strptime(context["ds"], "%Y-%m-%d").date()

    CuratedClientsJob(date).run()


with DAG(
    dag_id="clients_pipeline",
    start_date=datetime(2026, 3, 1),
    schedule="@daily",
    catchup=False,
) as dag:
    clients = PythonOperator(task_id="raw_clients", python_callable=run_raw_clients)

    portfolios = PythonOperator(
        task_id="raw_portfolios", python_callable=run_raw_portfolios
    )

    positions = PythonOperator(
        task_id="raw_positions", python_callable=run_raw_positions
    )

    transactions = PythonOperator(
        task_id="raw_transactions", python_callable=run_raw_transactions
    )

    curated_clients = PythonOperator(
        task_id="curated_clients", python_callable=run_curated_clients
    )

    [
        clients,
        portfolios,
        positions,
        transactions,
    ] >> curated_clients

    
