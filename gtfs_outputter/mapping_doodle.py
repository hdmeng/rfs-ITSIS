from datetime import datetime, timedelta
import gmplot
import math
import matplotlib.pyplot as plt
from mpl_toolkits.basemap import Basemap
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

# fig = plt.figure()
#
# m = Basemap(projection='merc', llcrnrlat=37.4, urcrnrlat=38.1, llcrnrlon=-122.6, urcrnrlon=-121.8, resolution='i')
#
# m.drawcoastlines()
# m.drawmapboundary(fill_color='aqua')
# m.fillcontinents(color='coral',lake_color='aqua')
#
# # parallels = np.arange(36.5, 38.5, 0.1)
# # m.drawparallels(parallels,labels=[False,True,True,False])
# #
# # meridians = np.arange(236., 240., 0.1)
# # m.drawmeridians(meridians,labels=[True,False,False,True])
#
# static_feed, checksum = gtfsutility.get_static('bart', False)
#
# shape_id = None
# for _, row in static_feed['shapes'].iterrows():
#     if row['shape_id'] != shape_id:
#         if shape_id:
#             x, y = m(lons, lats)
#             m.plot(x, y, marker=None, color='m', linewidth='2')
#         lats = []
#         lons = []
#         shape_id = str(row['shape_id'])
#     lats.append(row['shape_pt_lat'])
#     lons.append(row['shape_pt_lon'])
#
# plt.show()

# set up connection to the MySQL database

datapath = {}
pathname = None
engine = None

username = 'root'
password = 'PATH452RFS'
host = 'localhost'
database = 'PATHTransit'

username = 'root'
password = '3EasSarcasEgot3'
host = 'localhost'
database = 'GTFS'

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

transit_eta_bart = None
table_name = 'TransitETABART'
if dataframeutility.can_read_dataframe(table_name, datapath):
    transit_eta_bart = dataframeutility.read_dataframe(table_name, datapath)
datapath['conn'].close()
if transit_eta_bart is None or transit_eta_bart.empty:
    sys.exit(1)

trip_pattern_shape = None
table_name = 'Trip_pattern_shape'
if dataframeutility.can_read_dataframe(table_name, datapath):
    trip_pattern_shape = dataframeutility.read_dataframe(table_name, datapath)
    trip_pattern_shape = trip_pattern_shape.loc[trip_pattern_shape['agency_id'] == 8]
datapath['conn'].close()
if trip_pattern_shape is None or trip_pattern_shape.empty:
    sys.exit(1)

# prepare Google Map with static BART data

m = gmplot.GoogleMapPlotter(37.75, -122.20, 11)

static_feed, checksum = gtfsutility.get_static('bart', False)
shapes = static_feed['shapes']
stops = static_feed['stops']

shape_id = None
for _, row in shapes.drop_duplicates(['shape_pt_lat', 'shape_pt_lon']).iterrows():
    if row['shape_id'] != shape_id:
        if shape_id:
            m.plot(lats, lons, edge_width=2, edge_alpha=0.5)
        lats = []
        lons = []
        shape_id = str(row['shape_id'])
    lats.append(row['shape_pt_lat'])
    lons.append(row['shape_pt_lon'])
m.plot(lats, lons, edge_width=2, edge_alpha=0.5)

lats = []
lons = []
for _, row in static_feed['stops'].iterrows():
    lats.append(row['stop_lat'])
    lons.append(row['stop_lon'])
m.scatter(lats, lons)

# compute area of delay
# gmplot can't render a third dimension of data (time delay), will subsitute with number of copies of a stop == minutes of delay

comp_shapes = shapes.round(pd.Series([2, 2], index=['shape_pt_lat', 'shape_pt_lon']))
stops = stops.round(pd.Series([2, 2], index=['stop_lat', 'stop_lon']))

lats = []
lons = []
current_run = None
current_pattern_id = None
current_pt_seq = None
current_shape_id = None
next_pt_seq = None
for _, row in transit_eta_bart.iterrows():
    run = row['run']
    pattern_id = row['pattern_id']
    stop_id = row['stop_id']
    stop_row = stops.loc[stops['stop_id'] == stop_id].iloc[0]
    stop_point = (stop_row['stop_lat'], stop_row['stop_lon'])

    if run != current_run or pattern_id != current_pattern_id:
        if current_run:
            # print('changed')
            m.heatmap(lats, lons)

        current_shape_id = trip_pattern_shape.loc[trip_pattern_shape['pattern_id'] == pattern_id].iloc[0]['shape_id']
        point = comp_shapes.loc[(comp_shapes['shape_id'] == current_shape_id) & (comp_shapes['shape_pt_lat'] > (stop_point[0] - 0.01)) & (comp_shapes['shape_pt_lat'] < (stop_point[0] + 0.01)) & (comp_shapes['shape_pt_lon'] > (stop_point[1] - 0.01)) & (comp_shapes['shape_pt_lon'] < (stop_point[1] + 0.01))].iloc[0]
        current_pt_seq = point['shape_pt_sequence']
        next_pt_seq = None
        current_run = run
        current_pattern_id = pattern_id
    else:
        point = comp_shapes.loc[(comp_shapes['shape_id'] == current_shape_id) & (comp_shapes['shape_pt_lat'] > (stop_point[0] - 0.01)) & (comp_shapes['shape_pt_lat'] < (stop_point[0] + 0.01)) & (comp_shapes['shape_pt_lon'] > (stop_point[1] - 0.01)) & (comp_shapes['shape_pt_lon'] < (stop_point[1] + 0.01)) & (comp_shapes['shape_pt_sequence'] > current_pt_seq)].iloc[0]
        next_pt_seq = point['shape_pt_sequence']

        delay_in_min = math.ceil((datetimeFromHMS(str(row['ETA'])) - datetimeFromHMS(str(row['STA']))).total_seconds() / 60.)
        for pt_seq in range(current_pt_seq + 1, next_pt_seq + 1):
            potential_row = shapes.loc[(shapes['shape_id'] == current_shape_id) & (shapes['shape_pt_sequence'] == pt_seq)]
            if potential_row.empty:
                continue
            a_point = potential_row.iloc[0]
            for _ in range(int(delay_in_min)):
                lats.append(a_point['shape_pt_lat'])
                lons.append(a_point['shape_pt_lon'])
        current_pt_seq = next_pt_seq
    # print('{0}, {1}, {2}'.format(row['run'], row['pattern_id'], row['stop_id']))
m.heatmap(lats, lons)

m.draw('doodle.html')
