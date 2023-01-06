from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator
from airflow.hooks.base_hook import BaseHook


def task_fail_slack_alert(ENV_NAME):
    def callback(context):
        slack_webhook_token = BaseHook.get_connection(ENV_NAME).password
        slack_msg = """
                :red_circle: Task Failed.
                *Task*: {task}
                *Dag*: {dag}
                *Execution Time*: {exec_date}
                *Log Url*: {log_url}
                """.format(
            ti=context.get('task_instance'),
            task=context.get('task_instance').task_id,
            dag=context.get('task_instance').dag_id,
            exec_date=context.get('execution_date'),
            log_url=context.get('task_instance').log_url)
        failed_alert = SlackWebhookOperator(
            task_id='task_fail_slack_alert',
            username='airflow',
            http_conn_id=ENV_NAME,
            webhook_token=slack_webhook_token,
            message=slack_msg)
        return failed_alert.execute(context=context)
    return callback


def dag_success_slack_alert(ENV_NAME):
    def callback(context):
        slack_webhook_token = BaseHook.get_connection(ENV_NAME).password
        slack_msg = """
                :large_green_circle: DAG Passed.
                *Task*: {task}
                *Dag*: {dag}
                *Execution Time*: {exec_date}
                *Log Url*: {log_url}
                """.format(
            ti=context.get('task_instance'),
            task=context.get('task_instance').task_id,
            dag=context.get('task_instance').dag_id,
            exec_date=context.get('execution_date'),
            log_url=context.get('task_instance').log_url)
        success_alert = SlackWebhookOperator(
            task_id='dag_success_slack_alert',
            username='airflow',
            http_conn_id=ENV_NAME,
            webhook_token=slack_webhook_token,
            message=slack_msg)
        return success_alert.execute(context=context)
    return callback
