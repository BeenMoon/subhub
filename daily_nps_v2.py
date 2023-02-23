import datetime
import timedelta

from airflow import DAG
from airflow.providers.slack.notifications.slack_notifier import send_slack_notification

from plugins import nps_summary
from plugins import build_summary_table

with DAG(
        dag_id="daily_nps_v2",
        description="Create daily NPS",
        schedule="0 9 * * *",
        start_date=datetime(2023, 2, 1),
        default_args={
            'retries': 1,
            'retry_delay': timedelta(minutes=1)
        },
        max_active_tasks=1,
        max_active_runs=1,
        catchup=False,
        on_success_callback=[
            send_slack_notification(
                slack_conn_id='slack_conn_id',
                text="The DAG {{ dag.dag_id }} succeeded",
                channel="#general",
                username="Airflow",
            )
        ],
        on_failure_callback=[
            send_slack_notification(
                slack_conn_id='slack_conn_id',
                text="The DAG {{ dag.dag_id }} failed",
                channel="#general",
                username="Airflow",
            )
        ],
):
    
    create_stage_table = build_summary_table.create_stage(nps_summary, 'redshift_dev_db')
    update_summary_table = build_summary_table.update_summary(nps_summary, 'redshift_dev_db')
    
    create_stage_table >> update_summary_table