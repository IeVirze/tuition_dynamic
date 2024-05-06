'''Logic to extract salary data for the period of 2019-2024'''

#Import the required libraries
import pandas as pd
import sqlite3
import numpy as np 


#define functions to perform ETL process and get data
#Function functions: 
#Open all .csv files to transform to tables
#from median avg salary - divide in two tables netto and brutto salary for real numbers for comparison
#add to db salaries table netto, brutto avg/median so they can be compared with the tuition fee numbers
#need to find cost of living calculations for the time period 2019-2024 for a single person and a household
#need to modify the brutto count tables: 
    #add the column TOTAL (all age groups)
    #combine both .csv files and take the 2024 march data as data for the year of 2024 (there are no data for full year yet)
    #divide the combined view in two tables - salary ranges per age group per year and salary Total per year
#need to find the data on the most popular age groups for each of the degrees (college, bachelore, masters and phd)


def extract():
