#! /usr/bin/python

from pandas.io import sql

import MySQLdb
import numpy as np
import pandas as pd

def csv2df(csv_file):
	df = pd.read_csv(csv_file, sep = ',', header = 0)
	df.replace('\"', '')
	return df

def df2sql(dataframe, df_name, login, exist_flag='append'):
	con = MySQLdb.connect(host=login['host'], user=login['user'], passwd=login['passwd'], db=login['db'])
	# seems to have no way to tell what types each column should be
	dataframe.to_sql(con=con, name=df_name, flavor='mysql', if_exists=exist_flag, index=False)
	con.close()

def sql2df(df_name, login):
	con = MySQLdb.connect(host=login['host'], user=login['user'], passwd=login['passwd'], db=login['db'])
	# takes in no consideration for what the types should be
	df = pd.read_sql('SELECT * FROM {0}'.format(df_name), con)
	df.replace('\"', '')
	con.close()
	return df