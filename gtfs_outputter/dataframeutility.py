import logging
import MySQLdb
from os import path
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

def optional_field(index, column, dataframe, default='N/A'):
    row = dataframe.iloc[index]
    return row[column] if (column in dataframe.columns and not pd.isnull(row[column])) else default

def can_read_dataframe(df_name, login, is_local, pathname):
    if is_local:
        return path.exists(pathname + df_name + '.csv')
    else:
        try:
            with MySQLdb.connect(host=login['host'], user=login['user'], passwd=login['passwd'], db=login['db']) as con:
                con.execute('SHOW TABLES LIKE \'{0}\''.format(df_name))
                return True if con.fetchall() else False
        except MySQLdb.Error, e:
            try:
                logging.debug('MySQL Error {0}: {1}'.format(e.args[0], e.args[1]))
            except IndexError:
                logging.debug('MySQL Error: {0}'.format(str(e)))
            sys.exit(0)

def read_dataframe(df_name, login, is_local, pathname):
    if is_local:
        with open(pathname + df_name + '.csv', 'rb') as csvfile:
            logging.debug('Reading from local')
            return csv2df(csvfile)
    else:
        logging.debug('Reading from database')
        return sql2df(df_name, login)

def write_dataframe(dataframe, df_name, login, is_local, pathname, refresh):
    if is_local:
        dataframe.to_csv(pathname + df_name + '.csv', sep=',', index=False)
    else:
        df2sql(dataframe, df_name, login=login, exist_flag=('replace' if refresh else 'append'))
