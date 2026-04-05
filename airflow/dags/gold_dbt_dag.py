from airflow import DAG
from datetime import datetime
from airflow.providers.standard.operators.bash import BashOperator

with DAG(
    dag_id="dbt_gold_pipeline",
    start_date=datetime(2026, 3, 1),
    schedule="@daily",
    catchup=False,
) as dag:

    dbt_run = BashOperator(
        task_id="dbt_run_gold",
        bash_command="""
        cd /opt/airflow/datalake/analytics && \
        dbt run --select fct_positions --profiles-dir /opt/airflow/datalake/analytics
        """
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="""
        cd /opt/airflow/datalake/analytics && \
        dbt run --select fct_positions --profiles-dir /opt/airflow/datalake/analytics
        """
    )

    dbt_run >> dbt_test