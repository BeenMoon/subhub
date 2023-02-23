import logging

from airflow.decorators import task
from airflow.hooks.postgres_hook import PostgresHook
from airflow.exceptions import AirflowException


@task(task_id="create_stage_table")
def create_stage(summary_dict:dict, conn_id:str) -> None:
    schema = summary_dict['schema']
    table = summary_dict['table']
    input_test = summary_dict['input_test']
    
    logging.info(schema)
    logging.info(table)
    
    createTemp_sql = f"""
        CREATE TEMP TABLE stage (LIKE {schema}.{table});
        INSERT INTO stage """
    createTemp_sql += summary_dict['main_sql']
    
    cur = get_redshift_connection(conn_id)
    # input test
    cur.execute(input_test['sql'])
    if input_test['count'] < cur.fetchone():
        logging.error("input test error")
        raise AirflowException(f"Input validation failed: count < {input_test['count']}")
    # run main sql
    cur.execute(createTemp_sql)
    cur.execute("COMMIT;")
    
    logging.info("Created stage table")
    
    
@task(task_id="update_summary_table")
def update_summary(summary_dict:dict, conn_id:str) -> None:
    schema = summary_dict['schema']
    table = summary_dict['table']
    pk = summary_dict['primary_key']
    output_test = summary_dict['output_test']
    
    update_sql = f"""
        BEGIN TRANSACTION;
        DELETE FROM {schema}.{table}
        USING stage
        WHERE {schema}.{table}.{pk} = stage.{pk};
        INSERT INTO {schema}.{table}
        SELECT * FROM stage;
        END TRANSACTION;"""
    
    cur = get_redshift_connection(conn_id)
    
    try:
        # run main sql
        cur.execute(update_sql)
    except:
        cur.execute("ROLLBACK;")
        logging.error("query failed. rollbacked.")
        raise AirflowException("Error occurs during transaction.")
    else:
        # output test
        cur.execute(output_test['sql'])
        if output_test['count'] < cur.fetchone():
            logging.error("output test error")
            raise AirflowException("Output validation failed: count < {output_test['count']}")
        cur.execute("COMMIT;")
        cur.execute("DROP TABLE stage;")
    
    
def get_redshift_connection(conn_id:str):
    hook = PostgresHook(postgres_conn_id=conn_id)
    return hook.get_conn().cursor()