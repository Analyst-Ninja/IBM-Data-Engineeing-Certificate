import sqlite3 
import pandas as pd

table_name = 'instructors'
columns = ['Header','ID','FNAME','LNAME','CITY_CODE',]
df = pd.DataFrame(columns=columns)

data = pd.read_csv('INSTRUCTOR.csv', names=columns)

df = pd.concat([df, data], ignore_index=True) 

conn = sqlite3.connect('instructors.db')

df.to_sql(table_name, conn, if_exists='replace', index=False)

conn.close()