from datetime import timedelta
from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago


# Setting up the default arguments for DAG
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(0),
    'email': ['r.kumar01@hotmail.com'],
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG Definition
dag = DAG(
    'my-first-dag',
    default_args=default_args,
    description='My First DAG for ETL',
    schedule_interval=timedelta(days=1)
)

# Defining tasks

# Task1 --> Extract
extract = BashOperator(
    task_id = 'extract',
    bash_command='cut -d":" -f1,3,6 /etc/passwd > /home/de-ninja/Documents/Courses/IBM-Data-Engineeing-Certificate/ETL_and_pipelines_using_airflow_kafka/module3/ETL1/extracted-data.txt',
    dag=dag
)

# Task2 --> Load
transform_and_load = BashOperator(
    task_id = 'transform_and_load',
    bash_command='cat /home/de-ninja/Documents/Courses/IBM_DE_Certificate/ETL_and_pipelines_using_airflow_kafka/module3/ETL1/extracted-data.txt | tr ":" "," > /home/de-ninja/Documents/Courses/IBM_DE_Certificate/ETL_and_pipelines_using_airflow_kafka/module3/ETL1/transformed-data.csv',
    dag=dag
)

extract >> transform_and_load





