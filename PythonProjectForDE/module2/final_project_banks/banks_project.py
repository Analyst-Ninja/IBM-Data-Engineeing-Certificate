import sqlite3
import pandas as pd
from datetime import datetime
import requests
from bs4 import BeautifulSoup as BS

LOG_FILE = "code_log.txt"
URL = 'https://web.archive.org/web/20230908091635%20/https://en.wikipedia.org/wiki/List_of_largest_banks'
TABLE_ATTRIBUTES_FOR_EXTRACTION = ["Name", "MC_USD_Billion"]
TABLE_ATTRIBUTES_FOR_TRANSFORMATION = ["Name", "MC_USD_Billion","MC_GBP_Billion", "MC_EUR_Billion", "MC_INR_Billion"]
EXCHANGE_RATE_FILE = 'exchange_rate.csv'
TARGET_CSV_PATH = 'Largest_banks_data.csv'
DB_NAME = 'Banks.db'
TABLE_NAME = 'Largest_banks'

def log_progress(message):
    ''' This function logs the mentioned message of a given stage of the
    code execution to a log file. Function returns nothing'''
    tsFormat = "%Y-%h-%d:%H:%M:%S"
    timestamp = datetime.now().strftime(tsFormat)

    with open(LOG_FILE, mode='+a', encoding='utf-8') as f:
        f.write(f'{timestamp} | {message}\n')

def extract(url, table_attribs):
    ''' This function aims to extract the required
    information from the website and save it to a data frame. The
    function returns the data frame for further processing. '''

    df = pd.DataFrame(columns=table_attribs)

    html_page = requests.get(url).text
    data = BS(html_page, 'html.parser')

    table = data.find_all('tbody')[0]
    rows = table.find_all('tr')

    for row in rows:
        if row.find('td') != None:
            name = row.find_all('td')[1].find_all('a')[1]['title']
            market_cap_in_bill = row.find_all('td')[2].contents[0]

            data_to_add = {
                'Name' : name.strip(),
                'MC_USD_Billion' : int(float(market_cap_in_bill.strip())),
            }

            df = pd.concat([df, pd.DataFrame(data=data_to_add, index=[0])], ignore_index=True)


    return df

def transform(df, csv_path):
    ''' This function accesses the CSV file for exchange rate
    information, and adds three columns to the data frame, each
    containing the transformed version of Market Cap column to
    respective currencies'''

    exchange_rate_df = pd.read_csv(csv_path)
    ER_dict = {}
    for i in exchange_rate_df.values:
        ER_dict[i[0]] = i[1]

    for key,value in ER_dict.items():
        df[f'MC_{key}_Billion'] = round(df['MC_USD_Billion'] * value,2)
    return df

def load_to_csv(df, output_path):
    ''' This function saves the final data frame as a CSV file in
    the provided path. Function returns nothing.'''

    df.to_csv(output_path)


def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final data frame to a database
    table with the provided name. Function returns nothing.'''

    df.to_sql(table_name, sql_connection, if_exists='replace', index = False)

def run_query(query_statement, sql_connection):
    ''' This function runs the query on the database table and
    prints the output on the terminal. Function returns nothing. '''

    print(pd.read_sql(query_statement, sql_connection))


def main():
    ''' Here, you define the required entities and call the relevant
    functions in the correct order to complete the project. Note that this
    portion is not inside any function.'''


    log_progress('ETL Started.')

    log_progress('Extraction Phase Started.')
    extracted_data = extract(URL, TABLE_ATTRIBUTES_FOR_EXTRACTION)
    log_progress('Extraction Phase Completed.')

    log_progress('Transformation Phase Started.')
    transformed_data = transform(extracted_data, EXCHANGE_RATE_FILE)
    log_progress('Transformation Phase Completed.')

    log_progress('Loading to CSV file.')
    load_to_csv(transformed_data, TARGET_CSV_PATH)
    log_progress('Loading Completed to CSV file.')

    log_progress('Establishing connection to DB.')
    CONN = sqlite3.connect(DB_NAME)
    log_progress(f'Connected to {DB_NAME}.')

    log_progress('Table creation Started.')
    load_to_db(transformed_data, CONN, TABLE_NAME)
    log_progress(f'Table created in {DB_NAME}.')

    QUERRY1_TO_RUN = """
                    SELECT * FROM Largest_banks
                    """
    QUERRY2_TO_RUN = """
                    SELECT AVG(MC_GBP_Billion) FROM Largest_banks
                    """
    QUERRY3_TO_RUN = """
                    SELECT Name from Largest_banks LIMIT 5
                    """
    log_progress('Running Query.')
    run_query(QUERRY1_TO_RUN, CONN)
    print('\n')
    run_query(QUERRY2_TO_RUN, CONN)
    print('\n')
    run_query(QUERRY3_TO_RUN, CONN)
    print('\n')

    log_progress('Query has run successfully.')

    log_progress('Closing DB Connection.')
    CONN.close()
    log_progress('Connection Closed.')

    log_progress('ETL Job Completed.')

if __name__ == '__main__':
    main()