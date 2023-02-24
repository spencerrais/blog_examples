from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import datetime as dt
import requests


def extract_py(ti):
    data = requests.get('https://api.coincap.io/v2/assets').json()['data']
    ti.xcom_push(key='extract', value=data)


def transform_py(ti):
    data = ti.xcom_pull(key='extract', task_ids='extract')
    for item in data:
        item['changePercent24Hr'] = float(item['changePercent24Hr'])
    data_sorted = sorted(
            data, key=lambda x: x['changePercent24Hr'], reverse=True)
    timestamp = dt.datetime.now().isoformat()
    data_sorted = [{**record, "execution_timestamp": timestamp}
                   for record in data_sorted]
    ti.xcom_push(key='transform', value=data_sorted)


def load_py(ti, **kwargs):
    data_sorted = ti.xcom_pull(key='transform', task_ids='transform')
    hook = PostgresHook(postgres_conn_id='data_db')
    conn = hook.get_conn()
    cursor = conn.cursor()
    cols = ('id', 'rank', 'symbol', 'name', 'supply', 'max_supply', 'market_cap_usd',
            'volume_usd_24hr', 'price_usd', 'change_pct_24hr', 'vwap_24hr', 'explorer', 'execution_ts')
    vals = ','.join(['%s'] * len(cols))
    rows = [tuple(d.values()) for d in data_sorted]
    insert_query = f"INSERT INTO assets ({','.join(cols)}) VALUES ({vals})"
    cursor.executemany(insert_query, rows)
    conn.commit()


default_args = {
    "depends_on_past": False,
    "email": ["example@simple-batch.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": dt.timedelta(seconds=1),
    'execution_timeout': dt.timedelta(seconds=30)
}


with DAG(
    "simple-batch-sql",
    default_args=default_args,
    description="Simple Batch ETL data pipeline to Postgres",
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
        python_callable=load_py
    )

    extract >> transform >> load
