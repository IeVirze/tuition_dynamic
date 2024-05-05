# Code for ETL operations as per template in Coursera course for ETL operations

#Import the required libraries
import pandas as pd 
import numpy as np 
from datetime import datetime 
from bs4 import BeautifulSoup
import requests
import sqlite3

def extract(url, table_attribs):
    ''' This function extracts the required
    information from the website and saves it to a dataframe. The
    function returns the dataframe for further processing. '''

    

    return df

def transform(df):
    ''' This function (..)
    The function returns the transformed dataframe.'''

    return df

def load_to_csv(df, csv_path):
    ''' This function saves the final dataframe as a `CSV` file 
    in the provided path. Function returns nothing.'''
    df.to_csv(csv_path)

def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final dataframe as a database table
    with the provided name. Function returns nothing.'''
    #create for loop for adding 1 table for each year
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)

def run_query(query_statement, sql_connection):
    ''' This function runs the stated query on the database table and
    prints the output on the terminal. Function returns nothing. '''
    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)

def log_progress(message):
    ''' This function logs the mentioned message at a given stage of the code execution to a log file. Function returns nothing'''
    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second 
    now = datetime.now() # get current timestamp 
    timestamp = now.strftime(timestamp_format) 
    with open("./etl_project_log.txt","a") as f: 
        f.write(timestamp + ' : ' + message + '\n')

''' Here, you define the required entities and call the relevant 
functions in the correct order to complete the project.'''

#The arcived links with data are aquired via the web.archive.org services for free

url_RTU_2024 = 'https://www.rtu.lv/lv/studijas/visas-studiju-programmas' #5 May 2024
url_RTU_2023 = 'https://web.archive.org/web/20230206183459/https://www.rtu.lv/lv/studijas/visas-studiju-programmas' #6 February 2023
url_RTU_2022 = 'https://web.archive.org/web/20220301203232/https://www.rtu.lv/lv/studijas/visas-studiju-programmas' #1 March 2022
url_RTU_2021 = 'https://web.archive.org/web/20210416132938/https://www.rtu.lv/lv/studijas/visas-studiju-programmas' #16 April 2021
url_RTU_2020 = 'https://web.archive.org/web/20200813193321/https://www.rtu.lv/lv/studijas/visas-studiju-programmas' #13 August 2020
url_RTU_2019 = 'https://web.archive.org/web/20190724224506/https://www.rtu.lv/lv/studijas/visas-studiju-programmas' #24 July 2019

csv_path = './tuition_fees'
table_name = 'tuition_2019_to_2024'
db_name = 'tuition_fees.db'
table_attribs = ["Study programe", "Tuition 2019 PLK", "Tuition 2019 PLN", "Tuition 2019 NLN",
                    "Tuition 2020 PLK", "Tuition 2020 PLN", "Tuition 2020 NLN",
                    "Tuition 2021 PLK", "Tuition 2021 PLN", "Tuition 2021 NLN",
                    "Tuition 2022 PLK", "Tuition 2022 PLN", "Tuition 2022 NLN",
                    "Tuition 2023 PLK", "Tuition 2023 PLN", "Tuition 2023 NLN", 
                    "Tuition 2024 PLK", "Tuition 2024 PLN", "Tuition 2024 NLN"]