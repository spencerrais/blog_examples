import datetime as dt
from airflow import DAG
from airflow.operators.docker_operator import DockerOperator
from docker.types import Mount

default_args = {
    "owner": 'airflow',
    "depends_on_past": False,
    "email": ["example@simple-dbt.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": dt.timedelta(seconds=1),
    'execution_timeout': dt.timedelta(seconds=60)
}

with DAG(
    'docker_operator_demo',
    default_args=default_args,
    description="DockerOperator to run dbt",
    schedule=dt.timedelta(minutes=5),
    start_date=dt.datetime(2023, 1, 1),
    catchup=False,
    tags=["dbt"],
) as dag:

    dbt_run = DockerOperator(
        task_id='docker_command_dbt_run',
        image='dbt_docker',
        container_name='dbt_run',
        api_version='auto',
        auto_remove=True,
        command="bash -c 'dbt run'",
        docker_url="tcp://docker-proxy:2375",
        network_mode="bridge",
        mounts=[Mount(
            source="/home/spencer/dbt/dbt_blog_ex", target="/usr/app", type="bind"),
            Mount(
            source="/home/spencer/.dbt", target="/root/.dbt", type="bind")],
        mount_tmp_dir=False
    )

    dbt_run
