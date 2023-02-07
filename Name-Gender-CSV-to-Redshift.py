from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime, timedelta
import requests
import logging


def get_Redshift_connection(autocommit = False):
    logging.info("Connection start")
    hook = PostgresHook(postgres_conn_id = 'redshift_dev_db')
    conn = hook.get_conn()
    conn.autocommit = autocommit
    logging.info("Connection done")
    return conn.cursor()


def extract(**context):
    logging.info("Extract started")
    link = context['params']['url']
    execution_date = context['execution_date']
    logging.info(execution_date)
    f = requests.get(link)
    logging.info("Extract done")
    return (f.text)


def transform(**context):
    logging.info("Transform started")
    text = context['ti'].xcom_pull(key = 'return_value', task_ids = 'extract')
    lines = text.split("\n")[1:]  # ignore the first line - header
    logging.info("Transform done")
    return lines


def load(**context):
    logging.info("Load started")
    schema = context['params']['schema']
    table = context['params']['table']
    cur = get_Redshift_connection(autocommit = False)
    lines = context['ti'].xcom_pull(key = 'return_value', task_ids = 'transform')
    sql = f"BEGIN; DELETE FROM {schema}.{table};"
    for line in lines:
        if line != '':
            (name, gender) = line.split(",")
            sql += f"INSERT INTO {schema}.{table} VALUES ('{name}', '{gender}');"
    try:
        cur.execute(sql)
        cur.execute("COMMIT;")
    except:
        cur.execute("ROLLBACK;")
        raise
        logging.info("Load error")
    logging.info(sql)
    logging.info("Load done")
    
    
dag_second_assignment = DAG(
	dag_id = 'Name-Gender-CSV-to-Redshift',
	catchup = False,
	start_date = datetime(2023, 2, 6), # 날짜가 미래인 경우 실행이 안됨
	schedule_interval = '0 0 * * *',
    max_active_runs = 1,
    default_args = {
        'retries': 1,
        'retry_delay': timedelta(minutes=3)}
    )

extract = PythonOperator(
	task_id = 'extract',
	python_callable = extract,
    params = {
        'url': Variable.get('name_gender_url')
    },
	dag = dag_second_assignment)

transform = PythonOperator(
	task_id = 'transform',
	python_callable = transform,
	dag = dag_second_assignment)

load = PythonOperator(
	task_id = 'load',
	python_callable = load,
    params = {
        'schema': 'wkdansqls',
        'table': 'name_gender'
    },
	dag = dag_second_assignment)

extract >> transform >> load