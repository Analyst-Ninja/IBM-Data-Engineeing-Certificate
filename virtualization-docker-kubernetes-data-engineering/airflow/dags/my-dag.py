from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator


def say_hi(who):
    return (f"Hi {who})


with DAG('my-dag', start_date=datetime(2024,07,14)) as dag:
    task1 = PythonOperator(
                            task_id = 'first_task',
                            python_callback = say_hi,
                            op_kwargs = {'who' : 'Rohit'}
    )


    task2 = PythonOperator(
                            task_id = 'second_task',
                            python_callback = say_hi,
                            op_kwargs = {'who' : 'back to you!'}
    )

task1 >> task2
