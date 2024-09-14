from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.sqlite.operators.sqlite import SQLExecuteQueryOperator
from airflow.utils.dates import days_ago
from datetime import datetime, date, timedelta
import pandas as pd
import sqlite3

PATH_TO_STAGING = '/home/de-ninja/Documents/Courses/IBM_DE_Certificate/ETL_and_pipelines_using_airflow_kafka/module5/ETL_TOLL_with_Python/staging/'


default_args = {
    'owner' : 'rohit',
    'start_date' : days_ago(0),
    'email' : 'r.kumar01@hotmail.com',
    'retries' : 1,
    'retry_delay' : timedelta(minutes=5)
}

dag = DAG(
    dag_id = 'python-etl-toll-data',
    default_args = default_args,
    schedule_interval = timedelta(days=1),
    description = 'Apache Airflow Final Assignment using Python Operator'
)

# Task 1 --> Download Data
download_data = BashOperator(
    task_id = 'download_data',
    dag=dag,
    bash_command=f"wget https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Final%20Assignment/tolldata.tgz -O {PATH_TO_STAGING}tolldata.tgz"
)

# Task 2 --> Extract Data
unzip_data = BashOperator(
    task_id = 'unzip_data',
    dag=dag,
    bash_command=f"tar -xvf {PATH_TO_STAGING}tolldata.tgz -C {PATH_TO_STAGING}"
)

csv_columns = [
    'Rowid',
    'Timestamp',
    'Anonymized_Vehicle_number', 
    'Vehicle_type',
    'Number_of_axles',
    'Vehicle_code'
    ]

tsv_columns = [
    'Rowid',
    'Timestamp',
    'Anonymized_Vehicle_number', 
    'Vehicle_type',
    'Number_of_axles',
    'Tollplaza_id',
    'Tollplaza_code'
    ]
def extract_from_csv(infile, outfile):
    df =  pd.read_csv(
        infile, 
        header=None, 
        names=csv_columns
        ) \
    [
        [
            'Rowid',
            'Timestamp',
            'Anonymized_Vehicle_number', 
            'Vehicle_type'
        ]
    ]

    df.to_csv(outfile, index=False)
    return 0

        
def extract_from_tsv(infile, outfile):
    df =  pd.read_csv(
        infile, 
        header=None, 
        names=tsv_columns,
        delimiter='\t'
        )\
    [
        [
            'Number_of_axles',
            'Tollplaza_id',
            'Tollplaza_code'
        ]
    ]

    df.to_csv(outfile, index=False)
    return 0

def extract_from_fixed_width(infile, outfile):
    with open(outfile, 'w') as w:
        w.write("Type_of_Payment_code,Vehicle_code\n")
        with open(infile, 'r') as r:
            for line in r:
                print(line[-10:].strip().replace(" ",","))
                line_to_be_written = line[-10:].strip().replace(" ",",")+"\n"
                w.write(line_to_be_written)
        r.close()
    w.close()
    return 0

def consolidate(infileList, outfile):
    CSV_df = pd.read_csv(infileList[0])
    TSV_df = pd.read_csv(infileList[1])
    FIXED_WIDTH_FILE_df = pd.read_csv(infileList[2])

    df_concat = pd.concat([CSV_df, TSV_df, FIXED_WIDTH_FILE_df], axis=1)

    df_concat.to_csv(outfile, index=False)
    return 0

def transform(infile, outfile):
    df = pd.read_csv(infile)
    df['Vehicle_type'] = df['Vehicle_type'].str.upper()
    df.to_csv(outfile, index=False)
    return 0


vehicle_data = f"{PATH_TO_STAGING}vehicle-data.csv"
csv_data = f"{PATH_TO_STAGING}csv_data.csv"

# Task 3 --> Extract Data from CSV file
extract_from_CSV = PythonOperator(
    task_id = 'extract_from_CSV_file',
    dag=dag,
    python_callable=extract_from_csv,
    op_kwargs = {
        'infile': vehicle_data,
        'outfile' : csv_data
        }
)

toll_data = f"{PATH_TO_STAGING}tollplaza-data.tsv"
tsv_data = f"{PATH_TO_STAGING}tsv_data.csv"

# Task 4 --> Extract Data from TSV file
extract_from_TSV = PythonOperator(
    task_id = 'extract_from_TSV_file',
    dag=dag,
    python_callable=extract_from_tsv,
    op_kwargs = {
        'infile': toll_data,
        'outfile' : tsv_data
        }
)

payment_data = f"{PATH_TO_STAGING}payment-data.txt"
fixed_width_file_data = f"{PATH_TO_STAGING}fixed_width_data.csv"

# Task 5 --> Extract Data from TSV file
extract_from_fixed_width_file = PythonOperator(
    task_id = 'extract_from_fixed_width_file',
    dag=dag,
    python_callable=extract_from_fixed_width,
    op_kwargs = {
        'infile': payment_data,
        'outfile' : fixed_width_file_data
        }
)

# Task 6 --> Consolidate data from 3 files
consolidated_data = f"{PATH_TO_STAGING}extracted_data.csv"
consolidate_data = PythonOperator(
    task_id = 'consolidate_data',
    dag=dag,
    python_callable=consolidate,
    op_kwargs = {
        'infileList': [csv_data, tsv_data, fixed_width_file_data],
        'outfile' : consolidated_data
        }
)

# Task 7 --> Transform Data
transformed_data = f"{PATH_TO_STAGING}transformed_data.csv"
transform_data = PythonOperator(
    task_id = 'transform_data',
    dag=dag,
    python_callable=transform,
    op_kwargs = {
        'infile': consolidated_data,
        'outfile' : transformed_data
        }
)

sqliteConnection = sqlite3.connect(f'{PATH_TO_STAGING}sql.db')
cursor = sqliteConnection.cursor()

def create_table_function(query):
    cursor.execute(
        """
        DROP TABLE IF EXISTS toll_data;
        """
    )
    cursor.execute(query)

    df = pd.read_csv(f'{PATH_TO_STAGING}transformed_data.csv')
    df.to_sql(if_exists='replace', name='toll_data',index=False,con=sqliteConnection)
    return 0

create_query = """
    CREATE TABLE toll_data 
    (
        rowid INT PRIMARY KEY,
        Timestamp TEXT,
        Anonymized_Vehicle_number BIGINT,
        Vehicle_type TEXT,
        Number_of_axles INT,
        Tollplaza_id INT,
        Tollplaza_code TEXT,
        Type_of_Payment_code TEXT,
        Vehicle_code TEXT
    );
    """

create_table = PythonOperator(
    task_id = 'create_table',
    dag = dag,
    python_callable=create_table_function,
    op_kwargs={'query' : create_query}
)

download_data >> unzip_data >> [extract_from_CSV, extract_from_TSV, extract_from_fixed_width_file ] >> consolidate_data >> transform_data >> create_table
