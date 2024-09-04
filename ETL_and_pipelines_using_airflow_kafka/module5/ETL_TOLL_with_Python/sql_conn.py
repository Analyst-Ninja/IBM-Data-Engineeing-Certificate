import psycopg2
from sqlalchemy import create_engine
import pandas as pd

conn = psycopg2.connect(
    database="mydb2",  # Use an existing database
    user="postgres",        # Replace with your username
    password="1234",    # Replace with your password
    host="localhost",      # Host (usually localhost)
    port="5432",            # Default port
)
conn.autocommit = True


df = pd.read_csv('/home/de-ninja/Documents/Courses/IBM_DE_Certificate/ETL_and_pipelines_using_airflow_kafka/module5/ETL_TOLL_with_Python/staging/transformed_data.csv')
print(df.head())
# Create a SQLAlchemy engine

# Open a cursor to perform database operations
cur = conn.cursor()
# Execute a command: create datacamp_courses table
cur.execute("""
                SELECT * FROM mydb2.public.users LIMIT 10;
            """)

engine = create_engine('postgresql+psycopg2://postgres:1234@localhost:5432/mydb2')

df.to_sql(name='toll_data', index=False, if_exists='replace', con=engine,schema='public')
# Make the changes to the database persistent
conn.commit()
# Close cursor and communication with the database
cur.close()
conn.close()

