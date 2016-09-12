import pandas as pd

import dataframeutility

if __name__ == '__main__':
    pathname = './agencies/{0}/processed/'.format('bart')
    feed = {}
    get_new_feed = False
    table = 'Trip2Pattern'

    with open(pathname + table + '.csv', 'rb') as csvfile:
        feed[table] = dataframeutility.csv2df(csvfile)

    with open(pathname + table + '_1.csv', 'rb') as csvfile:
        feed[table + '_1'] = dataframeutility.csv2df(csvfile)

    with open(pathname + table + '_2.csv', 'rb') as csvfile:
        feed[table + '_2'] = dataframeutility.csv2df(csvfile)

    with open(pathname + table + '_3.csv', 'rb') as csvfile:
        feed[table + '_3'] = dataframeutility.csv2df(csvfile)

    for key, value in feed.items():
        feed[key] = value.sort_values(['01DCM11']).reset_index(drop=True)

    print(feed[table].sort_index(axis=1).equals(feed[table + '_1'].sort_index(axis=1)))
    print(feed[table].sort_index(axis=1).equals(feed[table + '_2'].sort_index(axis=1)))
    print(feed[table].sort_index(axis=1).equals(feed[table + '_3'].sort_index(axis=1)))
