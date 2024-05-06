# Code for ETL operations as per template in Coursera course for ETL operations

#Import the required libraries
import pandas as pd 
import numpy as np 
from datetime import datetime 
from bs4 import BeautifulSoup
import requests
import sqlite3
import regex as re

def extract(url):
    ''' This function extracts the required
    information from the website and saves it to a dataframe. The
    function returns the dataframe for further processing. '''

            #Define the empty data frames for each year


    page = requests.get(url).text
    data = BeautifulSoup(page,'html.parser')
    df = pd.DataFrame()

        #Access all tables
    tables = data.find_all('tbody')
        #Iterate through all tables that exist in the page (there are 8 tables in total 1 for each faculty)
    for table in tables:
        rows = table.find_all('tr')

        for row in rows:
            col = row.find_all('td')
            if len(col)!=0:
                data_dict = {} #initialize the data_dict for the table
                if col[0].find('a') is not None:
                    data_dict = {"Study programme": col[0].a.contents[0],
                                    "Funded places": col[1].contents[0],
                                    "Tuition PLK": col[2].contents[0],
                                    "Tuition NLK": col[3].contents[0],
                                    "Tuition NLN": col[4].contents[0]}
                                # Check for td with colspan="6"
                elif 'colspan' in col[0].attrs:
                    data_dict = {"Extra Info": col[0].b.contents[0]}
                if data_dict:
                    df1= pd.DataFrame(data_dict, index=[0])
                    df = pd.concat([df,df1], ignore_index=True)          
    return df

def transform(df):
    ''' This function Based on the column Extra info ads a level of the education aquired for each study programme. 
    And removes the column extra info.
    The function returns the transformed dataframe.'''

    #remove the substring

    df["Extra Info"] = df["Extra Info"].replace(r'.*[Bb]akalaura.*', "B", regex=True)
    df["Extra Info"] = df["Extra Info"].replace(r".*[Mm]aģistra.*", "M", regex=True)
    df["Extra Info"] = df["Extra Info"].replace(r"[Dd]oktora.*", "PhD", regex=True)
    df["Extra Info"] = df["Extra Info"].replace(r".*līmeņa.*", "C", regex=True)

# Extract the substrings
    last_info = None
    for i, row in df.iterrows():
        if pd.isna(row['Extra Info']):
            if last_info is not None:
                df.at[i, 'Extra Info'] = last_info
        else:
            last_info = row['Extra Info']
            df = df.drop(i)  # delete the row

    df = df.rename(columns={'Extra Info': last_info})  # rename the column

    return df

def load_to_csv(df, csv_path):
    ''' This function saves the final dataframe as a `CSV` file 
    in the provided path. Function returns nothing.'''
    df.to_csv(csv_path)
    #loaded csv

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

''' Here, you define the required entities and call the relevant 
functions in the correct order to complete the project.'''

#The arcived links with data are aquired via the web.archive.org services for free

urls = {2024: 'https://web.archive.org/web/20240427010003/https://www.rtu.lv/lv/studijas/visas-studiju-programmas', #5 May 2024
    2023:'https://web.archive.org/web/20230206183459/https://www.rtu.lv/lv/studijas/visas-studiju-programmas', #6 February 2023
    2022:'https://web.archive.org/web/20220301203232/https://www.rtu.lv/lv/studijas/visas-studiju-programmas', #1 March 2022
    2021: 'https://web.archive.org/web/20210416132938/https://www.rtu.lv/lv/studijas/visas-studiju-programmas', #16 April 2021
    2020: 'https://web.archive.org/web/20200813193321/https://www.rtu.lv/lv/studijas/visas-studiju-programmas', #13 August 2020
    2019: 'https://web.archive.org/web/20190724224506/https://www.rtu.lv/lv/studijas/visas-studiju-programmas' #24 July 2019}
    }


def main(urls):
    '''The function receives as a parameter all of the RTU urls from web.archive.org and iterates through them while 
    calling the functions for the full ETL pipline actions including loading to the database and .csv file loadings'''
    db_name = 'tuition_fees.db'
    for year, url in urls.items():
        csv = f'./tuition_fees_{year}.csv'
        table_name = f'tuition_fees_{year}'
        #Calls function to start ETL
        df = extract(url)
        #Calls function for transforming the data
        df = transform(df)
        #Calls function for loading the data to .csv files
        load_to_csv(df,csv)
        #Establishes sql connection and calls function to add data tables in .db file
        sql_connection = sqlite3.connect(db_name)
        load_to_db(df,sql_connection,table_name)

main(urls)
