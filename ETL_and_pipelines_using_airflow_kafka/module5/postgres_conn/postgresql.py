from sqlalchemy import create_engine, text
import pandas as pd

# Create a connection to the PostgreSQL database
db = create_engine("postgresql://postgres:1234@localhost:5432/postgres")
conn = db.connect() 

# Read the CSV file into a DataFrame
df = pd.read_csv('/home/de-ninja/Documents/Courses/IBM_DE_Certificate/ETL_and_pipelines_using_airflow_kafka/module5/ETL_TOLL_with_Python/staging/transformed_data.csv')

# Drop the table if it exists
conn.execute(text("DROP TABLE IF EXISTS postgres.public.toll_data;"))

# Create the table
conn.execute(
    text("""
    CREATE TABLE postgres.public.toll_data 
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
    )
)

df.to_sql(name='toll_data', con=conn, index=False, if_exists='replace',schema='public')

conn.commit()

# Close the connection
conn.close()
