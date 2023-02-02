from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import datetime as dt
import requests

filename = '/opt/airflow/output/output.txt'


def extract_py(ti):
    data = requests.get('https://api.coincap.io/v2/assets').json()['data']
    ti.xcom_push(key='extract', value=data)


def transform_py(ti):
    data = ti.xcom_pull(key='extract', task_ids='extract')
    data_sorted = sorted(
        data, key=lambda x: x['changePercent24Hr'], reverse=True)
    ti.xcom_push(key='transform', value=data_sorted)


def load_py(ti, **kwargs):
    filename = kwargs['filename']
    data_sorted = ti.xcom_pull(key='transform', task_ids='transform')
    execution_timestamp = dt.datetime.now()
    with open(filename, 'a') as f:
        f.write(f"{data_sorted}\nrun: {execution_timestamp}\n")


default_args = {
    "depends_on_past": False,
    "email": ["example@simple-batch.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": dt.timedelta(seconds=1),
    'execution_timeout': dt.timedelta(seconds=30)
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'sla': timedelta(hours=2),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
}


with DAG(
    "simple-batch",
    default_args=default_args,
    description="Simple Batch ETL data pipeline",
    schedule=dt.timedelta(minutes=1),
    start_date=dt.datetime(2023, 1, 1),
    catchup=False,
    tags=["batch", "simple"],
) as dag:

    extract = PythonOperator(
        task_id='extract',
        python_callable=extract_py
    )

    transform = PythonOperator(
        task_id='transform',
        python_callable=transform_py
    )

    load = PythonOperator(
        task_id='load',
        python_callable=load_py,
        op_kwargs={'filename': filename}
    )

    extract >> transform >> load
