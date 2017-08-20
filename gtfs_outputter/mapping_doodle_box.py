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

def main(route_short_name, stop_id, start_date, end_date):
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

    ed = datetime.strptime(end_date, '%Y-%m-%d')
    sd = datetime.strptime(start_date, '%Y-%m-%d')
    
    given_route = transit_eta.loc[transit_eta['route_short_name'] == route_short_name]
    given_timeframe = given_route.loc[(given_route['RecordedDate'] >= sd) & (given_route['RecordedDate'] <= ed)]
    stops = given_timeframe.loc[given_timeframe['stop_id'] == stop_id]

    sb_week_stas = []
    sb_week_delays = []
    nb_week_stas = []
    nb_week_delays = []

    ed = datetime.strptime(end_date, '%Y-%m-%d')
    sd = datetime.strptime(start_date, '%Y-%m-%d')
    time_diff = (ed - sd).days
    days = []
    for i in range(time_diff + 1):
        d = sd + timedelta(i)
        days.append(d.strftime('%Y-%m-%d'))
    for day in days:
        given_day = stops.loc[stops['RecordedDate'] == day].sort_values('STA')
        sb_trips = given_day.loc[given_day['route_dir'] == 0]
        nb_trips = given_day.loc[given_day['route_dir'] == 1]
        
        sb_delays = []
        sb_eta = []
        sb_sta = []
        sb_eta_previous_day = True
        sb_sta_previous_day = True
        sb_eta_next_day = False
        sb_sta_next_day = False
        # code.interact(local=locals())
        for i, trip in sb_trips.iterrows():
            eta_dt = datetime.combine(trip['RecordedDate'], trip['ETA'])
            if sb_eta_previous_day and eta_dt.hour >= 12:
                eta_dt -= timedelta(days=1)
            else:
                sb_eta_previous_day = False    
            if len(sb_eta) > 0 and sb_eta[-1] > eta_dt and eta_dt.hour == 0:
                sb_eta_next_day = True
            if sb_eta_next_day:
                eta_dt += timedelta(days=1)
            sb_eta += [eta_dt]
            
            sta_dt = datetime.combine(trip['RecordedDate'], trip['STA'])
            if sb_sta_previous_day and sta_dt.hour >= 12:
                sta_dt -= timedelta(days=1)
            else:
                sb_sta_previous_day = False
            if len(sb_sta) > 0 and sb_sta[-1] > sta_dt and sta_dt.hour == 0:
                sb_sta_next_day = True
            if sb_sta_next_day:
                sta_dt += timedelta(days=1)
            sb_sta += [sta_dt]

            sb_delays += [(sb_eta[-1] - sb_sta[-1]).total_seconds() / 60]

            if trip['STA'] not in sb_week_stas:
                sb_week_stas.append(trip['STA'])
            idx = sb_week_stas.index(trip['STA'])
            if not sb_week_delays or len(sb_week_delays) == idx:
                sb_week_delays.append([sb_delays[-1]])
            else:
                sb_week_delays[idx].append(sb_delays[-1])

        #for i, eta in enumerate(sb_eta):
        #    print('{0}, {1}'.format(sb_sta[i], eta))
        sb_eta = mdates.date2num(sb_eta)
        sb_sta = mdates.date2num(sb_sta)
    
        nb_delays = []
        nb_eta = []
        nb_sta = []
        nb_eta_past = False
        nb_sta_past = False
        for i, trip in nb_trips.iterrows():
            eta_dt = datetime.combine(trip['RecordedDate'], trip['ETA'])
            if nb_eta_past:
                eta_dt += timedelta(days=1)
            if len(nb_eta) > 0 and nb_eta[-1] > eta_dt and eta_dt.hour == 0:
                nb_eta_past = True
                eta_dt += timedelta(days=1)
            nb_eta += [eta_dt]

            sta_dt = datetime.combine(trip['RecordedDate'], trip['STA'])
            if nb_sta_past:
                sta_dt += timedelta(days=1)
            elif len(nb_sta) > 0 and nb_sta[-1] > sta_dt and sta_dt.hour == 0:
                nb_sta_past = True
                sta_dt += timedelta(days=1)
            nb_sta += [sta_dt]
            
            nb_delays += [(nb_eta[-1] - nb_sta[-1]).total_seconds() / 60]

            if trip['STA'] not in nb_week_stas:
                nb_week_stas.append(trip['STA'])
            idx = nb_week_stas.index(trip['STA'])
            if not nb_week_delays or len(nb_week_delays) == idx:
                nb_week_delays.append([nb_delays[-1]])
            else:
                nb_week_delays[idx].append(nb_delays[-1])
        #for eta in nb_eta:
        #    print(eta)
        nb_eta = mdates.date2num(nb_eta)
        nb_sta = mdates.date2num(nb_sta)

    whiskerprops = {'linestyle': '-'}
    fig = plt.figure(figsize=(25.6, 9.6), dpi=300)
    ax = fig.add_subplot(121)
    #for idx, sta in enumerate(nb_week_stas):
    #    print('{0}: {1}'.format(sta, nb_week_delays[idx]))
    plt.boxplot(nb_week_delays, whis='range', whiskerprops=whiskerprops)
    xtickNames = plt.setp(ax, xticklabels=nb_week_stas)
    plt.setp(xtickNames, rotation=45, fontsize=8)
    plt.xlabel('time (min)')
    plt.ylabel('min')
    plt.title('Northbound')

    ax = fig.add_subplot(122)
    #for idx, sta in enumerate(sb_week_stas):
    #    print('{0}: {1}'.format(sta, sb_week_delays[idx]))
    plt.boxplot(sb_week_delays, whis='range', whiskerprops=whiskerprops)
    xtickNames = plt.setp(ax, xticklabels=sb_week_stas)
    plt.setp(xtickNames, rotation=45, fontsize=8)
    plt.xlabel('time (min)')
    plt.ylabel('min')
    plt.title('Southbound')

    plt.gcf().autofmt_xdate()
    fig.tight_layout()
    plt.subplots_adjust(top=0.90)
    plt.suptitle('{0}, {1}-{2} STA vs ETA'.format(stop_id, start_date, end_date))
    plt.savefig('{0}_{1}-{2}_box.png'.format(stop_id, start_date, end_date))
    #print('w: {0}, h: {1}'.format(*(fig.get_size_inches() * fig.dpi)))

    stops.to_csv('{0}_{1}-{2}_TA.csv'.format(stop_id, start_date, end_date), index=False)

if __name__ == '__main__':
    if len(sys.argv) < 4:
        print('please provide a route_short_name, stop id, start date, and end date')
        sys.exit(1)
    route_short_name = sys.argv[1]
    stop_id = sys.argv[2]
    start_date = sys.argv[3]
    end_date = sys.argv[4]
    main(route_short_name, stop_id, start_date, end_date)
