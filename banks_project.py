#need to import these libraries:
#bs4
#numpy
#pandas
#requests
#sqlite3
#datetime
# and this download link for the exchange rate file:
# wget https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMSkillsNetwork-PY0221EN-Coursera/labs/v2/exchange_rate.csv

# Code for ETL operations on Country-GDP data
import numpy as np 
import pandas as pd 
import requests
import sqlite3
from datetime import datetime
from bs4 import BeautifulSoup

# Importing the required libraries

def log_progress(message):
    ''' This function logs the mentioned message of a given stage of the
    code execution to a log file. Function returns nothing'''
    timestamp_format = '%Y-%h-%d-%H:%M:%S'  # Year-Monthname-Day-Hour-Minute-Second 
    now = datetime.now()  # get current timestamp 
    timestamp = now.strftime(timestamp_format) 
    with open("./code_log.txt", "a") as f: 
        f.write(timestamp + ' : ' + message + '\n')

def extract(url, table_attribs):
    ''' Extracts the required data from the website and returns a DataFrame. '''
    page = requests.get(url).text
    data = BeautifulSoup(page, 'html.parser')
    df = pd.DataFrame(columns=table_attribs)

    tables = data.find_all('table', {'class': 'wikitable'})
    rows = tables[0].find_all('tr')[1:]

    for row in rows:
        col = row.find_all('td')
        if len(col) >= 3:
            try:
                name = col[1].text.strip()
                mc_usd = float(col[2].text.replace('\n', '').replace(',', '').replace('$', ''))

                data_dict = {
                    "Name": name,
                    "Market Cap (US$ Billion)": mc_usd
                }
                df1 = pd.DataFrame([data_dict])
                df = pd.concat([df, df1], ignore_index=True)
            except ValueError as e:
                print(f"Skipping row due to conversion error: {e}")

    return df


def transform(df, csv_path):
    ''' Placeholder: Transforms the DataFrame. '''
    exchange_df = pd.read_csv('https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMSkillsNetwork-PY0221EN-Coursera/labs/v2/exchange_rate.csv')
    exchange_rate = exchange_df.set_index('Currency').to_dict()['Rate']
    df['MC_GBP_Billion'] = [np.round(x*exchange_rate['GBP'],2) for x in df['Market Cap (US$ Billion)']]
    df['MC_EUR_Billion'] = [np.round(x*exchange_rate['EUR'],2) for x in df['Market Cap (US$ Billion)']]
    df['MC_INR_Billion'] = [np.round(x*exchange_rate['INR'],2) for x in df['Market Cap (US$ Billion)']]


    return df

def load_to_csv(df, output_path):
    ''' Saves the final data frame as a CSV file in the provided path. '''
    transformed_df.to_csv(output_path, index=False)

def load_to_db(df, sql_connection, table_name):
    ''' Saves the final data frame to a database table with the provided name. '''
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)

def run_queries(query_statement, sql_connection):
    ''' Runs the query on the database table and prints the output. '''
    print(f"\nExecuting Query: {query_statement}\n") 
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)

''' Here, you define the required entities and call the relevant
functions in the correct order to complete the project. Note that this
portion is not inside any function.'''

url = 'https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks'
table_attribs = ["Name", "Market Cap (US$ Billion)"]
db_name = 'Banks.db'
table_name = 'Largest_banks'
csv_path = './Largest_banks_data.csv'

log_progress('Preliminaries complete. Initiating ETL process')

data = extract(url, table_attribs)

log_progress('Data extraction complete. DataFrame printed successfully.')

transformed_df = transform(data, csv_path)
#print(transformed_df)
#print('MC_EUR_Billion: ', transformed_df['MC_EUR_Billion'].iloc[4])

log_progress('Data transformation complete. Initiating Loading process')

load_to_csv(transformed_df, csv_path)
log_progress('Data saved to CSV file')

sql_connection = sqlite3.connect(db_name)
log_progress('SQL Connection initiated')

load_to_db(transformed_df, sql_connection, table_name)
log_progress('Data loaded to Database as a table, Executing queries')

query_statement1 = f"SELECT * FROM Largest_banks"
run_queries(query_statement1, sql_connection)
log_progress('Executed query to print the entire table.')

query_statement2 = f"SELECT AVG(MC_GBP_Billion) FROM Largest_banks"
run_queries(query_statement2, sql_connection)
log_progress('Executed query to print the average market capitalization in GBP.')

query_statement3 = f"SELECT Name from Largest_banks LIMIT 5"
run_queries(query_statement3, sql_connection)
log_progress('Executed query to print the names of the top 5 banks.')

log_progress('Process Complete.')
sql_connection.close()