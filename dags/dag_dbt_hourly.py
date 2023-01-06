import os
from datetime import timedelta

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils import timezone

from lib.secrets import get_json_secret_value
from lib.env import get_zenreach_prefix
from lib.slack import task_fail_slack_alert, dag_success_slack_alert

def hours_ago(n):
    """
    Get a datetime object representing `n` hours ago. By default the time is current time.
    """
    today = timezone.utcnow()
    return today - timedelta(hours=n)


ENV_NAME = os.getenv('AIRFLOW_ENV_NAME')

default_args = {
    'owner': 'airflow',
    'start_date': hours_ago(24),
    'on_failure_callback': task_fail_slack_alert(ENV_NAME)
}

## This dag should run every two hours and not interfere with the dag_dbt_daily.
with DAG(dag_id='dbt_hourly', max_active_runs=1, catchup=False, default_args=default_args, schedule_interval='00 1-23/1 * * *') as dag:
    env_prefix = get_zenreach_prefix(ENV_NAME)
    snowflake_creds = get_json_secret_value(env_prefix + 'airflow-snowflake')

    env = {
        "SNOWFLAKE_USERNAME": snowflake_creds['username'],
        "SNOWFLAKE_PASSWORD": snowflake_creds['password']
    }

    dbt_deps = BashOperator(
        task_id='dbt_deps',
        bash_command='/usr/local/airflow/.local/bin/dbt deps --profiles-dir /usr/local/airflow/dags/dbt_analytics --project-dir /usr/local/airflow/dags/dbt_analytics'
    )

    dbt_run = BashOperator(
        task_id='dbt_run',
        env=env,
        bash_command='/usr/local/airflow/.local/bin/dbt run --profiles-dir /usr/local/airflow/dags/dbt_analytics --project-dir /usr/local/airflow/dags/dbt_analytics --models sources.ads staging.ads'
    )

    dbt_test = BashOperator(
        task_id='dbt_test',
        env=env,
        bash_command='/usr/local/airflow/.local/bin/dbt test --profiles-dir /usr/local/airflow/dags/dbt_analytics --project-dir /usr/local/airflow/dags/dbt_analytics --models sources.ads staging.ads'
    )

    dag_success = BashOperator(
        task_id='dag_success',
        bash_command='echo "dag_dbt_bihourly ran successfully!"',
        on_success_callback=dag_success_slack_alert(ENV_NAME)
    )

    dbt_deps >> dbt_run >> dbt_test >> dag_success
