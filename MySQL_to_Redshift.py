from datetime import datetime
from datetime import timedelta

from airflow import DAG
from airflow.providers.amazon.aws.transfers.sql_to_s3 import SqlToS3Operator

from plugins.s3_to_redshift_operator import S3ToRedshiftOperator


dag = DAG(
    dag_id = 'MySQL_to_Redshift',
    start_date = datetime(2023,2,10), # 날짜가 미래인 경우 실행이 안됨
    schedule_interval = '0 9 * * *',  # 적당히 조절
    max_active_runs = 1,
    catchup = False,
    default_args = {
        'retries': 1,
        'retry_delay': timedelta(minutes=3),
    }
)


schema = "wkdansqls"
table = "nps"
s3_bucket = "grepp-data-engineering"
s3_key = schema + "-" + table


mysql_to_s3_nps = SqlToS3Operator(
    task_id = 'mysql_to_s3_nps',
    sql_conn_id = "mysql_conn_id",
    query = "SELECT * FROM prod.nps",
    aws_conn_id = "aws_conn_id",
    s3_bucket = s3_bucket,
    s3_key = s3_key,
    verify = False,
    replace = True,
    pd_kwargs = {index: False},
    dag = dag
)

s3_to_redshift_nps = S3ToRedshiftOperator(
    task_id = 's3_to_redshift_nps',
    aws_conn_id = "aws_conn_id",
    s3_bucket = s3_bucket,
    s3_key = s3_key,
    redshift_conn_id = "redshift_dev_db",
    schema = schema,
    table = table,
    copy_options=['csv'],
    primary_key = "id",
    order_key = "created_at",
    dag = dag
)

mysql_to_s3_nps >> s3_to_redshift_nps
