from bs4 import BeautifulSoup
import requests
import pandas as pd
import numpy as np
import sqlite3
from datetime import datetime 

import datetime

def log_progress(message):
    ''' This function logs the mentioned message of a given stage of the
    code execution to a log file. Function returns nothing'''

now = datetime.datetime.now()
current_date = now.strftime("%Y-%m-%d")
current_time = now.strftime("%H:%M:%S")

print(f"{current_date}-{current_time}: Preliminaries complete. Initiating ETL process\n{current_date}-{current_time}: Data extraction complete. Initiating Transformation process\n{current_date}-{current_time}: Data transformation complete. Initiating loading process\n{current_date}-{current_time}: Data saved to CSV file\n{current_date}-{current_time}: SQL Connection initiated.\n{current_date}-{current_time}: Data loaded to Database as a table, Executing queries\n{current_date}-{current_time}: Process Complete.\n{current_date}-{current_time}: Server Connection closed")

def extract(url, table_attribs):
    ''' This function aims to extract the required
    information from the website and save it to a data frame. The
    function returns the data frame for further processing. '''
    url = 'https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks'
    page = requests.get(url).text
    data = BeautifulSoup(page,'html.parser')
    df = pd.DataFrame(columns=table_attribs)
    tables = data.find_all('tbody')
    rows = tables[0].find_all('tr')

    for row in rows:
        if row.find('td') is not None:
            col = row.find_all('td')
            bank_name = col[1].find_all('a')[1]['title']
            market_cap = col[2].contents[0][:-1]
            data_dict = {"Name":bank_name,  "MC_USD_Billion": float(market_cap)}
            df1 = pd.DataFrame(data_dict, index=[0])
            df = pd.concat([df,df1], ignore_index=True)
    return df



def transform(df, csv_path):
 
  try:
    exchange_rates = pd.read_csv(r'C:\Project 1\exchange rates\exchange_rate.csv').set_index('Currency')['Rate'].to_dict()

    required_currencies = ['GBP', 'EUR', 'INR']
    missing_rates = set(required_currencies) - set(exchange_rates.keys())
    if missing_rates:
      print(f"Warning: Missing exchange rates for {', '.join(missing_rates)}")

    df['MC_GBP_Billion'] = np.round(df['MC_USD_Billion'] * exchange_rates['GBP'], 2)
    df['MC_EUR_Billion'] = np.round(df['MC_USD_Billion'] * exchange_rates['EUR'], 2)
    df['MC_INR_Billion'] = np.round(df['MC_USD_Billion'] * exchange_rates['INR'], 2)

    return df

  except (FileNotFoundError, ValueError) as e:
    print(f"Error during transformation: {e}")
    return df  


def load_to_csv(df, output_path):
    ''' This function saves the final data frame as a CSV file in
    the provided path. Function returns nothing.'''
    df.to_csv(r'C:\Project 1\exchange_rate.exchange_rate.csv')

def load_to_db(df, sql_connection, table_name, if_exists='replace'):
  df.to_sql(table_name, sql_connection, if_exists=if_exists, index=False)


def run_query(query_statement, sql_connection):
    ''' This function runs the query on the database table and
    prints the output on the terminal. Function returns nothing. '''
    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)

url = 'https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks'
table_attribs = ["Name", "MC_USD_Billion"]
db_name = 'Banks.db'
table_name = 'Largest_banks'
output_path = './Largest_banks_data.csv'
csv_path = './exchange_rate.csv'


log_progress('Preliminaries complete. Initiating ETL process')

df= extract(url, table_attribs)

log_progress('Data extraction complete. Initiating Transformation process')

df=transform(df, csv_path)

log_progress('Data transformation complete. Initiating loading process')

load_to_csv(df, output_path)

log_progress('Data saved to CSV file')

sql_connection = sqlite3.connect('Banks.db')
 
log_progress('SQL Connection initiated.')

load_to_db(df, sql_connection, table_name)

log_progress('Data loaded to Database as a table, Executing queries')

query_statement = f"SELECT * FROM Largest_banks"
run_query(query_statement, sql_connection)

query_statement = f"SELECT AVG(MC_GBP_Billion) FROM Largest_banks"
run_query(query_statement, sql_connection)

query_statement = f"SELECT Name from Largest_banks LIMIT 5"
run_query(query_statement, sql_connection)

log_progress('Process Complete.')

sql_connection.close()

log_progress('Server Connection closed')
