import logging
from requests import get
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook


def get_forecast(**context):
    execution_date = context['execution_date']
    logging.info(execution_date)
    api_key = context['params']['api_key']
    link = f"https://api.openweathermap.org/data/2.5/onecall?lat=37.413294&lon=126.734086&exclude=current,minutely,hourly,alerts&appid={api_key}&units=metric"
    logging.info("Getting forecast start")
    try:
        daily = get(link).json()['daily']
    except Exception as e:
        logging.info("Error during getting forecast")
        print("Error message: ", e)
    else:
        week = []
        for day in daily[1:]:
            date = datetime.fromtimestamp(day['dt']).strftime('%Y-%m-%d')
            temp = day['temp']
            week.append({f'{date}':
                         {'temp': temp['day'], 'min_temp': temp['min'],'max_temp': temp['max']}
                        })
        logging.info("Getting forecast done")
        return week
    

def get_Redshift_connection(autocommit = False):
    logging.info("Connection start")
    hook = PostgresHook(postgres_conn_id = 'redshift_dev_db')
    conn = hook.get_conn()
    conn.autocommit = autocommit
    logging.info("Connection done")
    return conn.cursor()


def load_forecast(**context):
    logging.info("Load forecast start")
    schema = context['params']['schema']
    table = context['params']['table']
    week = context['ti'].xcom_pull(key = 'return_value', task_ids = 'get_forecast')
    
    sql_temp = f"DELETE FROM {schema}.temp_{table};"
    sql_temp += f"""INSERT INTO {schema}.temp_{table}
                    SELECT date, temp, min_temp, max_temp, created_date FROM {schema}.{table};"""
    for day in week:
        key, val = list(day.items())[0]
        sql_temp = f"""INSERT INTO {schema}.temp_{table} (date, temp, min_temp, max_temp)
                       VALUES ('{key}', {val['temp']}, {val['min_temp']}, {val['max_temp']});"""
        
    cur = get_Redshift_connection(autocommit = False)
    try:
        cur.execute(sql_temp)
        cur.execute("COMMIT;")
        logging.info("Copy and load temp done")
    except:
        cur.execute("ROLLBACK;")
        logging.info("Copy and load temp failed")
        raise
    else:
        sql_load = f"DELETE TABLE {schema}.{table};"
        sql_load += f"""INSERT INTO {schema}.{table}
                        SELECT date, temp, min_temp, max_temp, created_date
                        FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY date ORDER BY created_date DESC) ord
                              FROM {schema}.temp_{table})
                        WHERE ord = 1;"""
        try:
            cur.execute(sql_load)
            cur.execute("COMMIT;")
            logging.info("Refresh done")
        except:
            cur.execute("ROLLBACK;")
            logging.info("Refresh failed")
            raise
        finally:
            logging.info("Load forecast done")
    
    
dag_forecast = DAG(
    dag_id = 'ETL_Forecast_to_Redshift_incremental',
    catchup = False,
    start_date = datetime(2023, 2, 6),
    schedule_interval = '0 0 * * *',
    max_active_runs = 1,
    default_args = {
        'retries': 1,
        'retry_delay': timedelta(minutes=3)
        }
    )

get_forecast = PythonOperator(
    task_id = 'get_forecast',
    python_callable = get_forecast,
    params = {
        'api_key': Variable.get('open_weather_api_key')
        },
    dag = dag_forecast
    )

load_forecast = PythonOperator(
    task_id = 'load_forecast',
    python_callable = load_forecast,
    params = {
        'schema': 'wkdansqls',
        'table': 'weather_forecast_incremental'
        },
    dag = dag_forecast
    )

get_forecast >> load_forecast
