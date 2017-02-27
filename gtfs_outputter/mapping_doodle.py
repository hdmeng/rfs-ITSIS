import code
from datetime import *
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import numpy as np
import pandas as pd
import sqlalchemy as sa
import sys
import time

import dataframeutility
import gtfsutility

def datetimeFromHMS(timestamp):
    """Converts a timestamp string into a datetime instance.

    An hours hand of '99' will be ignored, while any hours hand a general
    24-hour clock will convert the overflow into days.

    Args:
        timestamp: An HH:MM:SS formatted string.

    Returns:
        A datetime instance of the timestamp
    """
    delay = timedelta(seconds=0)
    if int(timestamp[:2]) == 99:
        pass
    elif int(timestamp[:2]) >= 24:
        timestamp = str(int(timestamp[:2]) - 24) + timestamp[2:]
        delay = timedelta(days=1)

    o_time = time.strptime(timestamp, '%H:%M:%S')
    n_time = datetime.now().replace(hour=o_time.tm_hour, minute=o_time.tm_min, second=o_time.tm_sec, microsecond=0)
    return n_time + delay

def main(stop_id, day):
    # set up connection to the MySQL database

    datapath = {}
    pathname = None
    engine = None

    username = 'root'
    password = 'PATH452RFS'
    host = 'localhost'
    database = 'PATHTransit'

    #username = 'root'
    #password = ''
    #host = 'localhost'
    #database = 'GTFS'

    # if not default_login:
    #     username = raw_input('Enter username: ')
    #     password = getpass.getpass()
    #     host = raw_input('Enter host: ')
    #     database = raw_input('Enter database: ')

    engine = sa.create_engine('mysql://{0}:{1}@{2}/{3}'.format(username, password, host, database))

    try:
        conn = engine.connect()
    except sa.exc.OperationalError as e:
        error_code = e.orig.args[0]
        logging.error('unable to reach database' if error_code == 2002 else 'invalid credentials for database')
        sys.exit(1)
    datapath['conn'] = conn
    datapath['pathname'] = pathname
    datapath['engine'] = engine
    datapath['metadata'] = sa.MetaData()

    # read the TransitETABART table

    transit_eta = None
    table_name = 'TransitETA'
    if dataframeutility.can_read_dataframe(table_name, datapath):
        transit_eta = dataframeutility.read_dataframe(table_name, datapath)
    datapath['conn'].close()
    if transit_eta is None or transit_eta.empty:
        sys.exit(1)

    given_day = transit_eta.loc[transit_eta['RecordedDate'] == day]
    stops = given_day.loc[given_day['stop_id'] == stop_id]
    sb_trips = stops.loc[stops['route_dir'] == 0]
    nb_trips = stops.loc[stops['route_dir'] == 1]

    sb_delays = []
    sb_eta = []
    sb_sta = []
    # code.interact(local=locals())
    for i, trip in sb_trips.iterrows():
        sb_delays += [(datetime.combine(trip['RecordedDate'], trip['ETA']) - datetime.combine(trip['RecordedDate'], trip['STA'])).total_seconds() / 60]

        eta_dt = datetime.combine(trip['RecordedDate'], trip['ETA'])
        if len(sb_eta) > 0 and sb_eta[-1] > eta_dt:
            eta_dt += timedelta(days=1)
        sb_eta += [eta_dt]

        sta_dt = datetime.combine(trip['RecordedDate'], trip['STA'])
        if len(sb_sta) > 0 and sb_sta[-1] > sta_dt:
            sta_dt += timedelta(days=1)
        sb_sta += [sta_dt]
    sb_eta = mdates.date2num(sb_eta)
    sb_sta = mdates.date2num(sb_sta)

    nb_delays = []
    nb_eta = []
    nb_sta = []
    for i, trip in nb_trips.iterrows():
        nb_delays += [(datetime.combine(trip['RecordedDate'], trip['ETA']) - datetime.combine(trip['RecordedDate'], trip['STA'])).total_seconds() / 60]

        eta_dt = datetime.combine(trip['RecordedDate'], trip['ETA'])
        if len(nb_eta) > 0 and nb_eta[-1] > eta_dt:
            eta_dt += timedelta(days=1)
        nb_eta += [eta_dt]

        sta_dt = datetime.combine(trip['RecordedDate'], trip['STA'])
        if len(nb_sta) > 0 and nb_sta[-1] > sta_dt:
            sta_dt += timedelta(days=1)
        nb_sta += [sta_dt]
    nb_eta = mdates.date2num(nb_eta)
    nb_sta = mdates.date2num(nb_sta)

    # time_min = datetime.strptime(day + ' 00:00', '%Y-%m-%d %H:%M')
    # time_max = datetime.strptime(day + ' 23:59', '%Y-%m-%d %H:%M')

    locator = mdates.HourLocator()
    formatter = mdates.DateFormatter('%H:%M')
    fig = plt.figure()
    ax = fig.add_subplot(121)
    plt.plot(nb_sta, nb_eta, marker='1')
    plt.plot(nb_sta, nb_sta, marker='2', linestyle='--')
    ax.xaxis.set_major_locator(locator)
    ax.xaxis.set_major_formatter(formatter)
    ax.yaxis.set_major_locator(locator)
    ax.yaxis.set_major_formatter(formatter)
    ax.xaxis_date()
    ax.yaxis_date()
    plt.xlabel('time (min)')
    plt.ylabel('time (min)')
    plt.title('Northbound')
    plt.grid()

    ax = fig.add_subplot(122)
    plt.plot(sb_sta, sb_eta, marker='1')
    plt.plot(sb_sta, sb_sta, marker='2', linestyle='--')
    locator = mdates.HourLocator()
    formatter = mdates.DateFormatter('%H:%M')
    ax.xaxis.set_major_locator(locator)
    ax.xaxis.set_major_formatter(formatter)
    ax.yaxis.set_major_locator(locator)
    ax.yaxis.set_major_formatter(formatter)
    ax.xaxis_date()
    ax.yaxis_date()
    plt.xlabel('time (min)')
    plt.ylabel('time (min)')
    plt.title('Southbound')
    plt.grid()

    plt.gcf().autofmt_xdate()
    plt.tight_layout()
    plt.subplots_adjust(top=0.90)
    plt.suptitle('{0}, {1} STA vs ETA'.format(stop_id, day))
    plt.savefig('{0}_{1}_delays.png'.format(stop_id, day))

    stops.to_csv('{0}_{1}_TA.csv'.format(stop_id, day), index=False)

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print('please provide a stop id')
        sys.exit(1)
    day = str(datetime.now().strftime('%Y-%m-%d')) if len(sys.argv) < 3 else sys.argv[2]
    stop_id = sys.argv[1]
    main(stop_id, day)
