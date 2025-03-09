from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from dags.tasks.collect_data_activobank import run_activobank_etl

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 11, 8),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    'finance_dag',
    default_args = default_args,
    description = 'Fetch and process personal finance data'
)

# collect = PythonOperator(
#     task_id = 'activo_bank_etl',
#     python_callable = run_activobank_etl,
#     dag = dag
# )

transform = PythonOperator(
    task_id = 'activo_bank_etl',
    python_callable = run_activobank_etl,
    dag = dag
)

extract = PythonOperator(
    task_id = 'activo_bank_etl',
    python_callable = run_activobank_etl,
    dag = dag
)

transform >> extract