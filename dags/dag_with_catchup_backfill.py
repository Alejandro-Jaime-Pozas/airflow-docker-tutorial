from datetime import datetime, timedelta

from airflow.sdk import dag, task


default_args = {
    'owner': 'alex',
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
}


@dag(
    dag_id='dag_with_taskflow_api_v02',
    start_date=datetime(2025, 10, 20),
    schedule='@daily',
)
def hello_world_etl():

    @task(multiple_outputs=True)
    def get_name():
        return {
            'first_name': 'Ricky',
            'last_name': 'Stinicky',
        }

    @task()
    def get_age():
        return 27

    @task()
    def greet(first_name, last_name, age):
        print(f"Hello World my name is {first_name} {last_name} and I'm {age} years old.")

    name_dict = get_name()
    age = get_age()
    greet(name_dict['first_name'], name_dict['last_name'], age=age)

greet_dag = hello_world_etl()
