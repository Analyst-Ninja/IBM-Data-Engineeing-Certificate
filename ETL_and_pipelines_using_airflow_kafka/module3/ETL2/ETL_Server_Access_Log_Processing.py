from airflow.models import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago

default_args = {
    'owner' : 'rohit',
    'start_date' : days_ago(0),
    'email' : ['r.kumar01@hotmail.com'],
    'retries' : 1,
    'retry_delay' : timedelta(minutes=5)
}

dag = DAG(
    'ETL-DAG',
    default_args=default_args,
    description='ELT DAG that download and perform ETL to store compressed data',
    schedule_interval=timedelta(days=1)
)

download_data = BashOperator(
    task_id='download',
    bash_command="wget -O /home/de-ninja/Documents/Courses/IBM-Data-Engineeing-Certificate/ETL_and_pipelines_using_airflow_kafka/module3/ETL2/web-server-access-log.txt https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Apache%20Airflow/Build%20a%20DAG%20using%20Airflow/web-server-access-log.txt",
    dag=dag
)

extract_data = BashOperator(
    task_id='extract',
    bash_command="cut -d '#' -f1,4 /home/de-ninja/Documents/Courses/IBM-Data-Engineeing-Certificate/ETL_and_pipelines_using_airflow_kafka/module3/ETL2/web-server-access-log.txt > /home/de-ninja/Documents/Courses/IBM-Data-Engineeing-Certificate/ETL_and_pipelines_using_airflow_kafka/module3/ETL2/extracted-data.txt",
    dag=dag
)

transform_data = BashOperator(
    task_id='transform',
    bash_command="cat /home/de-ninja/Documents/Courses/IBM_DE_Certificate/ETL_and_pipelines_using_airflow_kafka/module3/ETL2/extracted-data.txt | tr '[a-z]' '[A-Z]' > /home/de-ninja/Documents/Courses/IBM_DE_Certificate/ETL_and_pipelines_using_airflow_kafka/module3/ETL2/capitalized.txt",
    dag=dag
)

load_data = BashOperator(
    task_id='load',
    bash_command="zip /home/de-ninja/Documents/Courses/IBM_DE_Certificate/ETL_and_pipelines_using_airflow_kafka/module3/ETL2/log.zip /home/de-ninja/Documents/Courses/IBM_DE_Certificate/ETL_and_pipelines_using_airflow_kafka/module3/ETL2/capitalized.txt",
    dag=dag
)

download_data >> extract_data >> transform_data >> load_data