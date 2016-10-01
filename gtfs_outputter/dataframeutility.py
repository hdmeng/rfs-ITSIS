import logging
from os import path
import sys

import pandas as pd


def csv2df(csv_file):
    df = pd.read_csv(csv_file, sep=',', header=0)
    df.replace('\"', '')
    return df


def df2sql(dataframe, df_name, engine, typing):
    dataframe.to_sql(df_name, engine, chunksize=1000, if_exists='append',
                     index=False, dtype=typing)


def sql2df(df_name, engine):
    df = pd.read_sql_table(df_name, engine)
    # df.replace('\"', '')
    return df


def clear_agency(df_name, agency_id, engine):
    engine.execute(
        'delete from {0} where agency_id = {1}'.format(df_name, agency_id))


def optional_field(index, column, dataframe, cast=str, default=None):
    row = dataframe.iloc[index]
    return cast(row[column]) if (column in dataframe.columns and
                                 not pd.isnull(row[column])) else default


def can_read_dataframe(df_name, datapath):
    if datapath['pathname'] is not None:
        return path.exists(datapath['pathname'] + df_name + '.csv')
    else:
        try:
            result = datapath['engine'].execute(
                'show tables like \'{0}\''.format(df_name))
            return True if result.fetchall() else False
        except MySQLdb.Error, e:
            try:
                logging.debug('MySQL Error {0}: {1}'.format(
                    e.args[0], e.args[1]))
            except IndexError:
                logging.debug('MySQL Error: {0}'.format(str(e)))
            sys.exit(1)


def read_dataframe(df_name, datapath):
    if datapath['pathname'] is not None:
        with open(datapath['pathname'] + df_name + '.csv', 'rb') as csvfile:
            logging.debug('Reading from local')
            return csv2df(csvfile)
    else:
        logging.debug('Reading from database')
        return sql2df(df_name, datapath['engine'])


def write_dataframe(dataframe, df_name, datapath, typing, agency_id):
    if datapath['pathname'] is not None:
        dataframe.to_csv(datapath['pathname'] + df_name + '.csv', sep=',', index=False)
    else:
        clear_agency(df_name, agency_id, datapath['engine'])
        df2sql(dataframe, df_name, datapath['engine'], typing)
