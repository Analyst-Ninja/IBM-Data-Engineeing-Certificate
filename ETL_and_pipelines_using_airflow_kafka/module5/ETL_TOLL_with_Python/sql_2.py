from sqlalchemy import create_engine
import pandas as pd


db = create_engine("postgresql://postgres:1234@localhost:5432/mydb2")
conn = db.connect() 

df = pd.read_csv('/home/de-ninja/Documents/Courses/IBM_DE_Certificate/ETL_and_pipelines_using_airflow_kafka/module5/ETL_TOLL_with_Python/staging/transformed_data.csv')

# conn.execute("""DROP TABLE IF EXISTS toll_data""")

# conn.execute(
#     """
#     CREATE TABLE mydb2.public.toll_data 
#     (
#         rowid INT PRIMARY KEY,
#         Timestamp TEXT,
#         Anonymized_Vehicle_number BIGINT,
#         Vehicle_type TEXT,
#         Number_of_axles INT,
#         Tollplaza_id INT,
#         Tollplaza_code TEXT,
#         Type_of_Payment_code TEXT,
#         Vehicle_code TEXT
#     );
#     """
# )

create_table_sql = """
    CREATE TABLE mydb2.public.toll_data 
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

with db.connect() as connection:
    connection.execute(create_table_sql)


df.to_sql(name='toll_data', con=conn, index=False, if_exists='replace')

conn.close()