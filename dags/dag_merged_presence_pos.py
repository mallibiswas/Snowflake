import os
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago
from lib.env import get_zenreach_prefix
from lib.secrets import get_json_secret_value
from lib.slack import task_fail_slack_alert, dag_success_slack_alert

ENV_NAME = os.getenv('AIRFLOW_ENV_NAME')

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'on_failure_callback': task_fail_slack_alert
}

with DAG(dag_id='dbt_merged_presence_pos', default_args=default_args, schedule_interval='@daily') as dag:
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

    dbt_seed = BashOperator(
        task_id='dbt_seed',
        env=env,
        bash_command='/usr/local/airflow/.local/bin/dbt seed --profiles-dir /usr/local/airflow/dags/dbt_analytics --project-dir /usr/local/airflow/dags/dbt_analytics'
    )

    dbt_run = BashOperator(
        task_id='dbt_run',
        env=env,
        bash_command='/usr/local/airflow/.local/bin/dbt run --profiles-dir /usr/local/airflow/dags/dbt_analytics --project-dir /usr/local/airflow/dags/dbt_analytics --models \
            mart_merged_presence_pos__contact_details \
            mart_merged_presence_pos__pos_locations \
            mart_merged_presence_pos__pos_sightings \
            mart_merged_presence_pos__enriched_sightings \
            mart_merged_presence_pos__transactions \
            mart_merged_presence_pos__temporospatial_contact_profiles \
            mart_merged_presence_pos__local_crm_contacts \
            mart_merged_presence_pos__pos_customers \
            mart_merged_presence_pos__crm_contact_profiles \
            mart_merged_presence_pos__wifi_pos_presence \
            --full-refresh'
    )

    dbt_test = BashOperator(
        task_id='dbt_test',
        env=env,
        bash_command='/usr/local/airflow/.local/bin/dbt test --profiles-dir /usr/local/airflow/dags/dbt_analytics --project-dir /usr/local/airflow/dags/dbt_analytics --exclude shadowtest seeds'
    )

    dag_success = BashOperator(
        task_id='dag_success',
        bash_command='echo "DAG ran successfully!"',
        on_success_callback=dag_success_slack_alert
    )

    dbt_deps >> dbt_seed >> dbt_run >> dbt_test >> dag_success
