from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator


default_args = {
    'owner': 'alex',
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
}


def greet(age, ti):
    name = ti.xcom_pull(task_ids='get_name_task')  # pulls the return value from get_name task
    print(f"""Hello World! My name is {name}
        and I'm {age} years old.""")


def get_name():
    return 'Manwe'


with DAG(
    default_args=default_args,
    dag_id='python_operator_dag_v03',
    description='A simple DAG with PythonOperator',
    start_date=datetime(2025, 10, 20),
    schedule='@daily',
) as dag:
    task1 = PythonOperator(
        task_id='greet_task',
        python_callable=greet,
        op_kwargs={'age': 30},
    )

    task2 = PythonOperator(
        task_id='get_name_task',
        python_callable=get_name,
    )

task2 >> task1
