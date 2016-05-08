#! /usr/bin/python

from pandas.io import sql

import MySQLdb
import numpy as np
import pandas as pd

def csv2df(csv_file):
	df = pd.read_csv(csv_file, sep = ',', header = 0)
	df.replace('\"', '')
	return df

def df2sql(dataframe, df_name):
	con = MySQLdb.connect()
	# merely to add data
	dataframe.to_sql(con=con, name=df_name, if_exists='append', flavor='mysql', index=False)