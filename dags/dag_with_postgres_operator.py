from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator


default_args = {
    'owner': 'alex',
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
}


with DAG(
    dag_id='dag_with_postgres_operator_v04',
    default_args=default_args,
    start_date=datetime(2025, 10, 20),
    schedule='0 0 * * *',
) as dag:
    task1 = SQLExecuteQueryOperator(
        task_id='create_postgres_table',
        conn_id='postgres_localhost',
        sql="""
            CREATE TABLE IF NOT EXISTS dag_runs (
                dt TIMESTAMP,
                dag_id VARCHAR(250),
                PRIMARY KEY (dt, dag_id)
            );
        """,
    )

    task2 = SQLExecuteQueryOperator(
        task_id='insert_into_table',
        conn_id='postgres_localhost',
        sql="""
            INSERT INTO dag_runs (dt, dag_id)
            VALUES (CURRENT_TIMESTAMP, '{{ dag.dag_id }}')
            ON CONFLICT (dt, dag_id) DO NOTHING;
        """,
    )


    task1 >> task2
