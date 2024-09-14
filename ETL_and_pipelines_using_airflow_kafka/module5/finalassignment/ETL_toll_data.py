from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

PATH_TO_STAGING = '/home/de-ninja/Documents/Courses/IBM_DE_Certificate/ETL_and_pipelines_using_airflow_kafka/module5/finalassignment/staging/'

# Defining Default Arguements
default_agrs = {
    'owner' : 'rohit',
    'start_date' : days_ago(0),
    'email' : 'r.kumar01@hotmail.com',
    'email_on_failure' : True,
    'email_on_retry' : True,
    'retries' : 1,
    'retry_delays' : timedelta(minutes=5)
}

# Defining DAG
dag = DAG(
    dag_id = 'ETL_toll_data',
    schedule_interval = timedelta(days = 1),
    default_args = default_agrs,
    description = 'Apache Airflow Final Assignment'
)

# Defining Tasks

# Task --> Unzip Data
unzip_data = BashOperator(
    task_id = 'unzip',
    dag = dag,
    bash_command=f"tar -xvf {PATH_TO_STAGING}tolldata.tgz -C {PATH_TO_STAGING}"
)

# Task --> Extract from CSV_data
extract_data_from_csv = BashOperator(
    task_id = 'extract_data_from_csv',
    dag = dag,
    bash_command = f'cut -d "," -f1,2,3,4 {PATH_TO_STAGING}vehicle-data.csv > {PATH_TO_STAGING}csv_data.csv' 
)

# Task --> Extract from TSV_data
extract_data_from_tsv = BashOperator(
    task_id = 'extract_data_from_tsv',
    dag = dag,
    bash_command = f"cut -d $'\t' -f5-7 {PATH_TO_STAGING}tollplaza-data.tsv | tr $'\t' ',' | cut -c1-16 > {PATH_TO_STAGING}tsv_data.csv"
)

# Task --> Extract from fixed width data
extract_data_from_fixed_width = BashOperator(
    task_id = 'extract_data_from_fixed_width',
    dag = dag,
    bash_command = f"rev {PATH_TO_STAGING}payment-data.txt | cut -c1-9 | rev | tr ' ' ',' > {PATH_TO_STAGING}fixed_width_data.csv"
)


# Task --> Consolidating Data
consolidate_data = BashOperator(
    task_id = 'consolidate_data',
    dag = dag,
    bash_command = f"paste -d, {PATH_TO_STAGING}csv_data.csv {PATH_TO_STAGING}tsv_data.csv {PATH_TO_STAGING}fixed_width_data.csv > {PATH_TO_STAGING}extracted_data.csv"
)

# Task --> Transform Data
transform_data = BashOperator(
    task_id = 'transform_data',
    dag = dag,
    bash_command = """awk -F',' 'BEGIN {OFS=","} {$4 = toupper($4); print}' """ + f"{PATH_TO_STAGING}extracted_data.csv > {PATH_TO_STAGING}transformed_data.csv"
)


unzip_data >> [extract_data_from_csv, extract_data_from_tsv, extract_data_from_fixed_width] >> consolidate_data >> transform_data



