import requests
import logging
import psycopg2

from datetime import datetime
from datetime import timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.exceptions import AirflowException


def get_Redshift_connection():
    hook = PostgresHook(postgres_conn_id = 'redshift_dev_db')
    return hook.get_conn().cursor()

def execSQL(**context):
    schema = context['params']['schema'] 
    table = context['params']['table']
    logical_date = context['logical_date']

    logging.info(schema)
    logging.info(table)

    cur = get_Redshift_connection()
    
    # 스테이징 테이블 생성
    createTemp_sql = f"""
        CREATE TEMP TABLE stage (LIKE {schema}.{table});
    """
    createTemp_sql += f"""
        INSERT INTO stage (date, nps)
        WITH score_prop AS (
            SELECT DATE(created_at) AS date
                 , score
                 , COUNT(1)/SUM(COUNT(1)) OVER(PARTITION BY date)::FLOAT AS proportion
            FROM wkdansqls.nps
            WHERE date = DATE('{logical_date}')
            GROUP BY date, score
            )
        SELECT date
             , SUM(CASE WHEN score = 0 THEN - accumulation ELSE accumulation END) AS nps
        FROM (SELECT date
                   , score
                   , SUM(proportion) OVER(PARTITION BY date ORDER BY date, score ROWS BETWEEN CURRENT ROW AND 6 FOLLOWING) AS accumulation
              FROM score_prop)
        WHERE score = 0 OR score = 9
        GROUP BY date;
    """
    cur.execute(createTemp_sql)
    cur.execute("COMMIT;")
    
    # 트랜잭션: 대상 테이블 업데이트
    try:
        update_sql = """
            BEGIN TRANSACTION;
        """
        update_sql += f"""
            DELETE FROM {schema}.{table}
            USING stage
            WHERE {schema}.{table}.date = stage.date;
        """
        update_sql += f"""
            INSERT INTO {schema}.{table}
            SELECT * FROM stage;
            END TRANSACTION;
        """
        cur.execute(update_sql)
        cur.execute("COMMIT;")
    except Exception as e:
        cur.execute("ROLLBACK;")
        logging.error('Failed to sql. Completed ROLLBACK!')
        raise AirflowException()
    else:
        cur.execute("DROP TABLE stage;")


dag = DAG(
    dag_id = "daily_nps",
    start_date = datetime(2023,2,14),
    schedule_interval = '@once',
    catchup = False
)

execsql = PythonOperator(
    task_id = 'execsql',
    python_callable = execSQL,
    params = {
        'schema': 'wkdansqls',
        'table': 'nps_summary'
    },
    dag = dag
)
