from datetime import datetime, timedelta

from airflow import DAG

from plugins import build_summary_table, slack
from config import nps_summary


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
            slack.send_slack_result('success')
        ],
        on_failure_callback=[
            slack.send_slack_result('failure')
        ],
):
    
    create_stage_table = build_summary_table.create_stage(nps_summary, 'redshift_dev_db')
    update_summary_table = build_summary_table.update_summary(nps_summary, 'redshift_dev_db')
    
    create_stage_table >> update_summary_table
