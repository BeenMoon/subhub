from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime
from datetime import timedelta

from airflow import AirflowException

import requests
import logging
import psycopg2

from airflow.exceptions import AirflowException

def get_Redshift_connection():
    hook = PostgresHook(postgres_conn_id = 'redshift_dev_db')
    return hook.get_conn().cursor()


def execSQL(**context):

    schema = context['params']['schema'] 
    table = context['params']['table']
    select_sql = context['params']['sql']

    logging.info("Schema: ", schema)
    logging.info("Table: ", table)
    logging.info("Query: ", select_sql)

    cur = get_Redshift_connection()

    insert_sql = f"""
        INSERT INTO {schema}.{table} (date, nps)
    """
    insert_sql += select_sql
    insert_sql += f"""
        ON CONFLICT (date)
        DO NOTHING;
    """
   
    try:
        logging.info(insert_sql)
        cur.execute(insert_sql)
        cur.execute("COMMIT;")
    except Exception as e:
        cur.execute("ROLLBACK")
        logging.error('Failed to sql. Completed ROLLBACK!')
        raise AirflowException("Error: ", e)


dag = DAG(
    dag_id = "daily_nps",
    start_date = datetime(2023,02,14),
    schedule_interval = '@once',
    catchup = False
)

execsql = PythonOperator(
    task_id = 'execsql',
    python_callable = execSQL,
    params = {
        'schema' : 'wkdansqls',
        'table': 'nps_summary',
        'sql' : """WITH score_prop AS (
                     SELECT DATE(created_at) AS date
                          , score
                          , COUNT(1)/SUM(COUNT(1)) OVER(PARTITION BY date)::FLOAT AS proportion
                     FROM wkdansqls.nps
                     WHERE date = DATE('{{ logical_date }}')
                     GROUP BY date, score
                   )
                   SELECT date
                        , SUM(CASE WHEN score = 0 THEN - accumulation ELSE accumulation END) AS nps
                   FROM (SELECT date
                              , score
                              , SUM(proportion) OVER(PARTITION BY date ORDER BY date, score ROWS BETWEEN CURRENT ROW AND 6 FOLLOWING) AS accumulation
                         FROM score_prop)
                   WHERE score = 0 OR score = 9
                   GROUP BY date"""
    },
    dag = dag
)
