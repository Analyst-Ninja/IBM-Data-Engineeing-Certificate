import requests 
import pandas as pd
import sqlite3 
from bs4 import BeautifulSoup as BS
from datetime import datetime

url = 'https://web.archive.org/web/20230902185326/https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29'
table_name = 'Countries_by_GDP' 
csv_path = 'Countries_by_GDP.csv'
columns = ['Country', 'GDP_USD_millions']
DB_name = 'World_Economies.db'
query_to_run = f"""
                SELECT * from {table_name} 
                WHERE GDP_USD_billions >= 100
                """

# Code for ETL operations on Country-GDP data



def extract(url, table_attribs):
    ''' This function extracts the required
    information from the website and saves it to a dataframe. The
    function returns the dataframe for further processing. '''

    df = pd.DataFrame(columns=table_attribs)

    html_page = requests.get(url=url).text
    data = BS(html_page, 'html.parser')

    tables = data.find_all('tbody')
    rows = tables[2].find_all('tr')

    for i in range(3, len(rows)):
        GDP_in_millions = rows[i].find_all('td')[2].contents[0]
        country_name = rows[i].find_all('a')[0].contents[0]
        if GDP_in_millions.strip() != '—':
            data = {
                'Country' : str(country_name),
                'GDP_USD_millions' : float(str(GDP_in_millions).replace(',',''))
            }
        else:
            data = {
                'Country' : str(country_name),
                'GDP_USD_millions' : 0.0
            }

        df = pd.concat([df, pd.DataFrame(data=data, index=[0])], ignore_index=True)

    return df


def transform(df):
    ''' This function converts the GDP information from Currency
    format to float value, transforms the information of GDP from
    USD (Millions) to USD (Billions) rounding to 2 decimal places.
    The function returns the transformed dataframe.'''

    df['GDP_USD_billions'] = round(df['GDP_USD_millions'] / 1000, 2)

    return df[['Country', 'GDP_USD_billions']]

def load_to_csv(df, csv_path):
    ''' This function saves the final dataframe as a `CSV` file 
    in the provided path. Function returns nothing.'''
    df.to_csv(csv_path)

    return 0

def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final dataframe as a database table
    with the provided name. Function returns nothing.'''

    df.to_sql(table_name, sql_connection,if_exists='replace', index=False)

def run_query(query_statement, sql_connection):
    ''' This function runs the stated query on the database table and
    prints the output on the terminal. Function returns nothing. '''
    print(pd.read_sql(query_statement, sql_connection))



def log_progress(message):
    ''' This function logs the mentioned message at a given stage of the code execution to a log file. Function returns nothing'''

    TS_format = '%y-%h-%d:%H-%M-%S'
    now = datetime.now()

    timestamp = now.strftime(TS_format)

    with open('etl_project_log.txt', mode='+a') as f:
        f.write(timestamp + ' | ' + message + '\n')

''' Here, you define the required entities and call the relevant 
functions in the correct order to complete the project. Note that this
portion is not inside any function.'''


def main():
    
    log_progress('ETL Started.')

    log_progress('Extraction Phase Started.')
    extracted_data = extract(url=url, table_attribs=columns)
    log_progress('Extraction Phase Completed.')

    log_progress('Transformation Phase Started.')
    transformed_data = transform(extracted_data)
    log_progress('Transformation Phase Completed.')

    log_progress('Loading to CSV file.')
    load_to_csv(transformed_data, csv_path=csv_path)
    log_progress('Loading Completed to CSV file.')

    log_progress('Establishing connection to DB.')
    conn = sqlite3.connect(DB_name)
    log_progress(f'Connected to {DB_name}.')

    log_progress('Table creation Started.')
    load_to_db(transformed_data,table_name=table_name,sql_connection=conn)
    log_progress(f'Table created in {DB_name}.')

    log_progress('Running Query.')
    run_query(query_statement = query_to_run, sql_connection = conn)
    log_progress('Query has run successfully.')

    log_progress('Closing DB Connection.')
    conn.close()
    log_progress('Connection Closed.')

    log_progress('ETL Job Completed.')

if __name__ == '__main__':
    main()
    