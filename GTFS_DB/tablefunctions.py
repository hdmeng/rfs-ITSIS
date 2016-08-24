import MySQLdb
import os
import pandas as pd
import numpy as np
import pytz
import MySQLdb
from google.transit import gtfs_realtime_pb2
import urllib
import googlemaps
import datetime
import helper
import csv
import prettytable
from StringIO import StringIO
import sqlalchemy

SQL_USER = "root"
SQL_PWD = "PATH452RFS"

# generate table "Agency"
def agencies(tables, static_feed, arg_agency_id):
    columns = ['agency_id', 'agency_name', 'agency_url', 'agency_timezone', 'agency_lang',
           'agency_phone', 'timezone_name']
    tables['Agency'] = pd.DataFrame(index=np.r_[0:len(static_feed['agency'].index)], columns=columns)
    for i, row in static_feed['agency'].iterrows():
        new_row = tables["Agency"].loc[i] #instantiate a NEW ROW
        new_row['agency_id'] = arg_agency_id
        new_row['agency_name'] = row['agency_name']
        new_row['agency_url'] = row['agency_url']
        timezone = pytz.timezone(row['agency_timezone'])
        new_row['agency_timezone'] = timezone
        new_row['agency_lang'] = helper.optional_field(i, 'agency_lang', static_feed['agency'])
        new_row['agency_phone'] = helper.optional_field(i, 'agency_phone', static_feed['agency'])
        new_row['timezone_name'] = row['agency_timezone']
    helper.write_table(tables, 'Agency')
    print "SUCCESS finished with agencies"

# generate table "Stops"
def stops(tables, static_feed, arg_agency_id):
    columns = ['agency_id', 'stop_id', 'stop_code', 'stop_name', 'stop_desc', 'stop_lat', 'stop_lon',
               'lat_lon', 'stop_url', 'location_type', 'parent_station', 'wheelchair_boarding', 'version']
    # tables['Stops'] = pd.DataFrame(index=np.r_[0:len(static_feed['stops'].index)], columns=columns)
    tables['Stops'] = pd.DataFrame()

    print static_feed['stops'].columns
    for i, row in static_feed['stops'].iterrows():
        new_row = {}
        new_row['agency_id'] = arg_agency_id
        # new_row['stop_id'] = str(row['stop_id'])
        # print "Name of first row: " + row.index[0]
        if row.index[0] == 'stop_id':
            new_row['stop_id'] = row['stop_id']
        else:
            new_row['stop_id'] = row[0]

        new_row['stop_code'] = str(helper.optional_field(i, 'stop_code', static_feed['stops']))
        new_row['stop_name'] = str(row['stop_name'])
        new_row['stop_desc'] = str(helper.optional_field(i, 'stop_desc', static_feed['stops']))
        new_row['stop_lat'] = float(row['stop_lat'])
        new_row['stop_lon'] = float(row['stop_lon'])
        # new_row['stop_url'] = str(helper.optional_field(i, 'stop_url', static_feed['stops']))
        new_row['location_type'] = int(helper.optional_field(i, 'location_type', static_feed['stops'], 0))
        new_row['parent_station'] = int(helper.optional_field(i, 'parent_station', static_feed['stops'], 0))
        new_row['wheelchair_boarding'] = int(helper.optional_field(i, 'wheelchair_boarding', static_feed['stops'], 0))
        tables['Stops'] = tables['Stops'].append(pd.Series(new_row), ignore_index=True)
    helper.write_table(tables, "Stops")
    print "SUCCESS with stops"

def routes(tables, static_feed, arg_agency_id):
    columns = ['agency_id', 'route_short_name', 'route_dir',
               'route_type', 'route_long_name', 'route_desc',
               'route_url', 'route_color', 'route_text_color', 'route_id', 'version']
    tables['Routes'] = pd.DataFrame()
    for i, row in static_feed['routes'].iterrows():
        for direction_id in static_feed['trips'].loc[static_feed['trips']['route_id'] == row['route_id']]['direction_id'].unique():
            new_row = {}
            new_row['agency_id'] = arg_agency_id
            new_row['route_short_name'] = str(helper.optional_field(i, 'route_short_name', static_feed['routes'], static_feed['routes'].iloc[i]['route_long_name']))
            new_row['route_dir'] = direction_id
            new_row['route_type'] = int(row['route_type'])
            new_row['route_long_name'] = str(helper.optional_field(i, 'route_long_name', static_feed['routes'], static_feed['routes'].iloc[i]['route_short_name']))
            new_row['route_desc'] = helper.optional_field(i, 'route_desc', static_feed['routes'])
            new_row['route_url'] = helper.optional_field(i, 'route_url', static_feed['routes'])
            new_row['route_color'] = helper.optional_field(i, 'route_color', static_feed['routes'], default='FFFFFF').upper()
            new_row['route_text_color'] = helper.optional_field(i, 'route_text_color', static_feed['routes'], default='000000').upper()
            new_row['route_id'] = str(row['route_id'])
            new_row['version'] = 1
            tables['Routes'] = tables['Routes'].append(pd.Series(new_row), ignore_index=True)
    helper.write_table(tables, 'Routes')
    print "SUCCESS with routes"

# generate table "Route_stop_seq"
def route_stop_seq(tables, static_feed, arg_agency_id, arg_route_id, trip2pattern):
    count = 0
    columns = ['agency_id', 'route_short_name', 'route_dir',
               'pattern_id', 'stop_id', 'seq', 'is_time_point', 'version', 'trip_id']

    tables['Route_stop_seq'] = pd.DataFrame()
    for i, row in static_feed['routes'].iterrows(): #iterate through the different routes
        if arg_route_id == 'all':
            route_id = row['route_id']
        else :
            route_id = int(arg_route_id)
        print route_id
        patterns = [] #the patterns
        for j, subrow in static_feed['trips'].loc[static_feed['trips']['route_id'] == route_id].iterrows():
            trip_id = subrow['trip_id'] #get the trip_id
            print trip_id
            direction_id = subrow['direction_id'] if 'direction_id' in subrow else 0 #get the direction id in the trip
            trip_id_block = static_feed['stop_times'].loc[static_feed['stop_times']['trip_id'] == trip_id]
            sequence = trip_id_block['stop_id'].tolist()
            if str(sequence) not in patterns:
                patterns += [str(sequence)]
                pattern_num = patterns.index(str(sequence)) + 1
                route_short_name = str(helper.optional_field(i, 'route_long_name', static_feed['routes']))
                pattern_id = "{0}_{1}_{2}".format(route_short_name, direction_id, pattern_num)
                for k, subsubrow in trip_id_block.iterrows():
                    new_row = {}
                    new_row['trip_id'] = trip_id
                    new_row['agency_id'] = arg_agency_id
                    new_row['route_short_name'] = route_short_name
                    new_row['route_dir'] = direction_id
                    new_row['pattern_id'] = pattern_id
                    new_row['stop_id'] = str(subsubrow['stop_id'])
                    new_row['seq'] = subsubrow['stop_sequence']
                    new_row['is_time_point'] = int(helper.optional_field(k, 'timepoint', static_feed['stop_times'], 0))
                    new_row['version'] = 1; #replace later
                    tables["Route_stop_seq"] = tables["Route_stop_seq"].append(pd.Series(new_row), ignore_index=True)
                    count += 1
            trip2pattern[trip_id] = pattern_id
        if arg_route_id != 'all':
            break

    with open('Trip2Pattern.csv', 'wb') as f:
        writer = csv.writer(f)
        writer.writerow(["trip_id", "pattern_id"])
        for key, value in trip2pattern.items():
            writer.writerow([key, value])
    helper.write_table(tables, 'Route_stop_seq')
    print "SUCCESS with Route Stop Seq"


def runPattern(tables, static_feed, arg_agency_id):
    columns = ['agency_id', 'route_short_name', 'start_date', 'end_date',
               'service_id', 'day', 'route_dir', 'run', 'pattern_id', 'trip_headsign', 'trip_id', 'version']
    tables['RunPattern'] =  pd.DataFrame(index=np.r_[0:len(static_feed['trips'].index)], columns=columns)
    run_count = {}

    #open the trip2pattern file
    trip2pattern = helper.csv2df("Trip2Pattern.csv") #load the trip2pattern csv
    #iterate through eveyr trip
    for i, row in static_feed['trips'].iterrows():
        #get this specific row
        new_row = tables["RunPattern"].loc[i]
        new_row['agency_id'] = arg_agency_id
        #getting the name of the route
        j = np.where(static_feed['routes']['route_id'] == row['route_id'])[0][0]
        new_row['route_short_name'] = str(helper.optional_field(j, 'route_short_name', static_feed['routes'], static_feed['routes'].iloc[j]['route_long_name']))
        #use calendar.txt
        new_row['service_id'] = row['service_id']
        calendar = static_feed['calendar'].loc[static_feed['calendar']['service_id'] == row['service_id']].iloc[0]
        new_row['start_date'] = datetime.datetime.strptime(str(calendar['start_date']), "%Y%m%d")
        new_row['end_date'] = datetime.datetime.strptime(str(calendar['end_date']), "%Y%m%d")
        new_row['route_dir'] = int(helper.optional_field(i, 'direction_id', static_feed['trips'], 0))
        new_row['day'] = "{0}{1}{2}{3}{4}{5}{6}".format(calendar['monday'], calendar['tuesday'], calendar['wednesday'], calendar['thursday'], calendar['friday'], calendar['saturday'], calendar['sunday'])
        #calculating the runs...Each run is unqiuely identified by
        #1. route_short_name
        #2. service_id
        #3. route_dir
        if new_row['route_short_name'] not in run_count.keys():
            run_count[new_row['route_short_name']] = {new_row['service_id']: {new_row['route_dir'] : 1}}

        if new_row['service_id'] not in run_count[new_row['route_short_name']].keys():
            run_count[new_row['route_short_name']] = {new_row['service_id']: {new_row['route_dir'] : 1}}

        if new_row['route_dir'] not in run_count[new_row['route_short_name']][new_row['service_id']].keys():
            run_count[new_row['route_short_name']] = {new_row['service_id']: {new_row['route_dir'] : 1}}

        #Todo: order runs by time of first stop in pattern
        new_row['run'] = run_count[new_row['route_short_name']][new_row['service_id']][new_row['route_dir']]
        run_count[new_row['route_short_name']][new_row['service_id']][new_row['route_dir']] += 1 #increment the run because we've seen this before....


        new_row['pattern_id'] = str(trip2pattern[trip2pattern['trip_id'] == row['trip_id']].iloc[0]['pattern_id'])

        # new_row['trip_headsign'] = helper.optional_field(i, 'trip_headsign', static_feed['trips'], static_feed['stop_times'].loc[static_feed['stop_times']['trip_id'] == row['trip_id']]['stop_headsign'].iloc[0])
        new_row['trip_id'] = str(row['trip_id'])
        new_row['version'] = 1
    helper.write_table(tables, 'RunPattern')
    print "SUCCESS with RunPatterns"


'''
def schedules(tables, static_feed, trip_update_feed, alert_feed, vehicle_position_feed, agency_id, trip2pattern):
    try:
        login = {'host': "localhost", 'user': SQL_USER,
                 'passwd': SQL_PWD, 'db': "PATHTransit"}
        run_pattern_df = helper.sql2df('RunPattern', login)
        route_stop_seq_df = helper.sql2df('Route_stop_seq', login)
    except Exception as e:
        print e
    columns = ['agency_id', 'route_short_name', 'start_date', 'end_date', 'day',
               'route_dir', 'run', 'pattern_id', 'seq', 'stop_id',
               'is_time_point', 'pickup_type', 'drop_off_type',
               'arrival_time', 'departure_time', 'stop_headsign', 'trip_id']
    tables["Schedules"] = pd.DataFrame()
    counter = 0

    # trip2pattern = helper.csv2df("Trip2Pattern.csv") #load the trip2pattern csv
    #iterate through the trips
    for a, row in static_feed['trips'].iterrows():
        #find the run_pattern specific to this trip
        run_pattern_trip_specific = run_pattern_df[run_pattern_df['trip_id'] == row['trip_id']]

        #find the route_stop_seq specific to this trip
        # route_stop_seq_trip_specific = route_stop_seq_df[route_stop_seq_df['trip_id']
        #                                                  == row['trip_id']]

        #find the pattern of this trip by opening trip2pattern.csv
        stop_times_df = helper.csv2df("bart/stop_times.txt")
        stops_trip_= helper.cs

        #find the stop_seq by pattern id by opening route_stop_seq





        #find the stop_times_specific to the stop_sequence in the trips
        stop_times_trip_specific = static_feed['stop_times'][static_feed['stop_times']['trip_id'] == row['trip_id']]

        #iterate through the stops of this trip
        for b, subrow in route_stop_seq_trip_specific.iterrows():
            new_row = {}
            new_row['agency_id'] = agency_id
            new_row['route_short_name'] = subrow['route_short_name']
            new_row['start_date'] = run_pattern_trip_specific.iloc[0]['start_date']
            new_row['end_date'] = run_pattern_trip_specific.iloc[0]['end_date']
            new_row['day'] = run_pattern_trip_specific.iloc[0]['day']
            new_row['route_dir'] = subrow['route_dir']
            new_row['run'] = run_pattern_trip_specific.iloc[0]['run']
            new_row['pattern_id'] = subrow['pattern_id']
            new_row['seq'] = subrow['seq']
            new_row['stop_id'] = subrow['stop_id']
            new_row['is_time_point'] = subrow['is_time_point']
            new_row['pickup_type'] = stop_times_trip_specific.iloc[0]['pickup_type']
            new_row['drop_off_type'] = stop_times_trip_specific.iloc[0]['drop_off_type']
            new_row['arrival_time'] = stop_times_trip_specific[stop_times_trip_specific['stop_id'] == subrow['stop_id']].iloc[0]['arrival_time']
            new_row['departure_time'] = stop_times_trip_specific[stop_times_trip_specific['stop_id'] == subrow['stop_id']].iloc[0]['departure_time']
            new_row['stop_headsign'] = stop_times_trip_specific[stop_times_trip_specific['stop_id'] == subrow['stop_id']].iloc[0]['stop_headsign']
            new_row['trip_id'] = row['trip_id']
            tables["Schedules"] = tables["Schedules"].append(pd.Series(new_row), ignore_index=True)
            print counter
            counter += 1
    helper.write_table(tables, "Schedules")
    print "Sucess with Schedules"
'''

def schedules(tables, static_feed, arg_agency_id, trip2pattern):
    try:
        login = {'host': "localhost", 'user': SQL_USER,
                 'passwd': SQL_PWD, 'db': "PATHTransit"}
        run_pattern_df = helper.sql2df('RunPattern', login)
        agency_name = static_feed['agency'].iloc[0]['agency_name']
        trip2pattern = helper.csv2df("Trip2Pattern.csv") #load the trip2pattern csv
        route_stop_seq_df = helper.sql2df('Route_stop_seq', login)


    except Exception as e:
        print e
    columns = ['agency_id', 'route_short_name', 'start_date', 'end_date', 'day',
               'route_dir', 'run', 'pattern_id', 'seq', 'stop_id',
               'is_time_point', 'pickup_type', 'drop_off_type',
               'arrival_time', 'departure_time', 'stop_headsign', 'trip_id']

    tables["Schedules"] = pd.DataFrame()

    pattern_id_memoization = {} #to save calculations and memory
    counter = 0
    #use the unique tripid's to find the the patternid's specific to that trip
    for a, row in static_feed['stop_times'].iterrows():
        print counter
        counter += 1

        #find the pattern_id(What is the point of pattern id? we already have the stops.)
        pattern_id = trip2pattern[trip2pattern['trip_id'] == row['trip_id']]['pattern_id']
        new_row = {}

        #agency_id basic info
        new_row['agency_id'] = arg_agency_id


        #information from pattern_id
        current_trip_id = row['trip_id']

        if current_trip_id in pattern_id_memoization.keys():
            new_row['pattern_id'] = pattern_id_memoization[current_trip_id]
            print current_trip_id
        else: #access trip2pattern.csv to match the trip_id with the pattern
            pattern_id = trip2pattern[trip2pattern['trip_id'] == current_trip_id].iloc[0]['pattern_id']
            pattern_id_memoization[current_trip_id] = pattern_id
            new_row['pattern_id'] = pattern_id_memoization[current_trip_id]

        print "current trip id: " + str(current_trip_id)
        # print route_stop_seq_df
        #get information by matching with route_stop_seq matching by trip_id
        route_stop_seq_stop_time_specific = route_stop_seq_df[route_stop_seq_df['trip_id'] == current_trip_id]
        if not route_stop_seq_stop_time_specific.empty:
            route_stop_seq_stop_time_specific = route_stop_seq_stop_time_specific.iloc[0]
            new_row['route_short_name'] = route_stop_seq_stop_time_specific['route_short_name']
            new_row['route_dir'] = route_stop_seq_stop_time_specific['route_dir']


        #get information using run pattern by matching with trip_id
        run_pattern_stop_time_specific = run_pattern_df[run_pattern_df['trip_id'] == current_trip_id]
        if not run_pattern_stop_time_specific.empty:
            run_pattern_stop_time_specific = run_pattern_stop_time_specific.iloc[0]
            new_row['start_date'] = run_pattern_stop_time_specific['start_date']
            new_row['end_date'] = run_pattern_stop_time_specific['end_date']
            new_row['day'] = run_pattern_stop_time_specific['day']
            new_row['run'] = run_pattern_stop_time_specific['run']


        #all information in stop_times.txt
        new_row['seq'] = row['stop_sequence']
        new_row['stop_id'] = row['stop_id']
        #time point is optional
        new_row['is_time_point'] = helper.optional_field(a, 'time_point', static_feed['stop_times'])
        # new_row['is_time_point'] = row['timepoint']
        new_row['pickup_type'] = helper.optional_field(a, 'pickup_type', static_feed['stop_times'])
        # new_row['pickup_type'] = row['pickup_type']
        # new_row['drop_off_type'] = row['drop_off_type']
        new_row['pickup_type'] = helper.optional_field(a, 'drop_off_type', static_feed['stop_times'])
        new_row['arrival_time'] = row['arrival_time']
        new_row['departure_time'] = row['departure_time']
        # new_row['stop_headsign'] = row['stop_headsign']
        new_row['stop_headsign'] = helper.optional_field(a, 'stop_headsign', static_feed['stop_times'])
        new_row['trip_id'] = row['trip_id']
        tables["Schedules"] = tables["Schedules"].append(pd.Series(new_row), ignore_index=True)

    helper.write_table(tables, "Schedules")
    print "Sucess with Schedules"

def points(tables, static_feed, arg_agency_id, trip2pattern):
    point_id = 0
    #unique lat and lon define a point. Using a map for uniqueness
    point_mapper = {}
    tables['Points'] = pd.DataFrame()
    for a, row in static_feed['shapes'].iterrows():
        if (row['shape_pt_lat'] not in point_mapper.keys()) or (row['shape_pt_lon'] not in point_mapper[row['shape_pt_lat']].keys()):
            new_row = {}
            new_row['agency_id'] = arg_agency_id
            new_row['point_lat'] = row['shape_pt_lat']
            new_row['point_lon'] = row['shape_pt_lon']
            new_row['shape_id'] = row['shape_id']
            new_row['shape_pt_sequence'] = row['shape_pt_sequence']
            new_row['point_id'] = point_id
            tables["Points"] = tables["Points"].append(pd.Series(new_row), ignore_index=True)

            #add the point the point_mapper
            point_mapper[row['shape_pt_lat']] = {row['shape_pt_lon'] : "exists"}
            print point_id
            point_id += 1
        else:
            print 'repeat!'
    helper.write_table(tables, "Points")
    print "Success with Points"



def big_points(tables, static_feed, arg_agency_id, trip2pattern):
    point_id = 0
    big_constant = pow(10, 8)
    #unique lat and lon define a point. Using a map for uniqueness
    point_mapper = {}
    tables['Big_Points'] = pd.DataFrame()
    for a, row in static_feed['shapes'].iterrows():
        if (row['shape_pt_lat'] not in point_mapper.keys()) or (row['shape_pt_lon'] not in point_mapper[row['shape_pt_lat']].keys()):
            new_row = {}
            new_row['agency_id'] = arg_agency_id
            new_row['point_lat'] = int(row['shape_pt_lat'] * big_constant)
            new_row['point_lon'] = int(row['shape_pt_lon'] * big_constant)
            new_row['shape_id'] = row['shape_id']
            new_row['shape_pt_sequence'] = row['shape_pt_sequence']
            new_row['point_id'] = point_id
            tables["Big_Points"] = tables["Big_Points"].append(pd.Series(new_row), ignore_index=True)

            #add the point the point_mapper
            point_mapper[row['shape_pt_lat']] = {row['shape_pt_lon'] : "exists"}
            print point_id
            point_id += 1
        else:
            print 'repeat!'
    helper.write_table(tables, "Big_Points")
    print "Success with Big_Points"

'''
def route_point_seq_old_version(tables, static_feed, trip_update_feed, alert_feed, vehicle_position_feed, agency_id, trip2pattern):
    columns = ['agency_id', 'route_short_name', 'route_dir', 'pattern_id', 'shape_id',
               'point_id', 'seq', 'length', 'heading', 'dist', 'version']
    try:
        #What is shapes_df also had the trips it is a part of?
        #load the Route_Stop_Seq for miscellaneous things
        login = {'host':"localhost", 'user':SQL_USER,
                         'passwd':SQL_PWD, 'db': "PATHTransit"}
        # route_stop_seq_df = helper.sql2df('Route_stop_seq', login)
    except Exception as e:
        print os.getcwd()
        print e

    #finds the sequence of coordinates for a specific trip.
    #what am i having trouble with? getting the shape_id from the table of points.
    tables['Route_Point_Seq'] = pd.DataFrame()
    counter = 0
    for a, row in static_feed['routes'].iterrows(): #iterate through the different routes

        route_id = row['route_id']
        print route_id
        patterns = [] #the patterns
        trips_specific_to_this_route = static_feed['trips'].loc[static_feed['trips']['route_id'] == route_id].iterrows()
        for b, subrow in trips_specific_to_this_route:
            trip_id = subrow['trip_id']
            shape_id = subrow['shape_id'] #get the shape_id of the specific trip
            direction_id = subrow['direction_id'] if 'direction_id' in subrow else 0 #get the direction id in the trip
            shape_id_block = static_feed['shapes'].loc[static_feed['shapes']['shape_id'] == shape_id]
            distanceSinceStart = 0 #keep track of distance since start of the trip
            lastPoint = None
            for c, subsubrow in shape_id_block.iterrows():
                if counter > 200:
                    break
                new_row = {}
                new_row['trip_id'] = trip_id
                new_row['agency_id'] = agency_id
                new_row['route_short_name'] = str(helper.optional_field(a, 'route_long_name', static_feed['routes']))
                new_row['route_dir'] = direction_id
                # new_row['pattern_id'] = pattern_id
                new_row['shape_pt_sequence'] = subsubrow['shape_pt_sequence']
                new_row['shape_id'] =  shape_id

                #geting the point_id is incredibly hard because you have to match the point_id with the
                #current_lon, current_lat with is very technically dificult
                # new_row['point_id']
                #how to calculate the length
                currentLon = subsubrow['shape_pt_lon']
                currentLat = subsubrow['shape_pt_lat']

                if lastPoint != None:
                    new_row['length'] = helper.coordToMiles(currentLat, currentLon, lastPoint['current_lat'], lastPoint['current_lon'])
                    distanceSinceStart += new_row['length']
                    #add the length for the cumumulative distance
                    new_row['dist'] = distanceSinceStart

                    new_row['heading'] = helper.calculate_heading(currentLat, currentLon, lastPoint['current_lat'],lastPoint['current_lon'])
                    lastPoint = {'current_lat': currentLat, 'current_lon': currentLon}
                else: #the case where we just start the way point
                    new_row['length']  = 0 # seq i - seq i distance is 0
                    new_row['dist'] = 0
                    lastPoint = {'current_lat': currentLat, 'current_lon': currentLon}
                    new_row['heading'] = "N/A" #the first point doesn't have any heading

                new_row['version'] = 1
                counter += 1
                print counter

                tables["Route_Point_Seq"] = tables["Route_Point_Seq"].append(pd.Series(new_row), ignore_index=True)

    print tables["Route_Point_Seq"]
    helper.write_table(tables, "Route_Point_Seq")
    print "SUCCESS with Route Point Seq"

'''
def route_point_seq(tables, static_feed, arg_agency_id, trip2pattern):

    try:
        #What is shapes_df also had the trips it is a part of?
        #load the Route_Stop_Seq for miscellaneous things
        login = {'host':"localhost", 'user':SQL_USER,
                         'passwd':SQL_PWD, 'db': "PATHTransit"}
        route_stop_seq_df = helper.sql2df('Route_stop_seq', login)
        big_points_df = helper.sql2df('Big_Points', login)
        points_df = helper.sql2df('Points', login)
    except Exception as e:
        print os.getcwd()
        print e

    counter = 0

    tables['Route_Point_Seq2'] = pd.DataFrame()
    columns = ['agency_id', 'route_short_name', 'route_dir', 'pattern_id', 'shape_id',
               'point_id', 'seq', 'length', 'heading', 'dist', 'version']

    #find a list of unique trips route_stop_seq_df
    unique_trips = route_stop_seq_df['trip_id'].unique()

    #loop through the route_stop_seq_df
    for current_trip_id in unique_trips:

        #from route_stop_stop_seq
        print "current trip id: " + str(current_trip_id)
        route_info_specific_to_trip = route_stop_seq_df[route_stop_seq_df['trip_id'] == current_trip_id].iloc[0]

        #use the current trip_id to find the shape_id associatd with the trip
        current_shape_id = static_feed['trips'][static_feed['trips']['trip_id'] == current_trip_id].iloc[0]['shape_id']
        print "Current_shape_id: " + str(current_shape_id)

        #with the shape_id, you can find a list of way points from shapes.txt
        waypoints_specific_to_shape_id = static_feed['shapes'][static_feed['shapes']['shape_id'] == current_shape_id]
        print "WayPoints: "
        print waypoints_specific_to_shape_id
        distanceSinceStart = 0 #keep track of distance since start of the trip
        lastPoint = None
        for a, row in waypoints_specific_to_shape_id.iterrows():
            counter += 1
            print "counter: "+ str(counter)
            new_row = {}

            #info from route_stop_seq
            new_row['route_short_name'] = route_info_specific_to_trip['route_short_name']
            new_row['route_dir'] = route_info_specific_to_trip['trip_id']
            new_row['pattern_id'] = route_info_specific_to_trip['pattern_id']

            #info from general
            new_row['trip_id'] = current_trip_id
            new_row['agency_id'] = arg_agency_id
            new_row['shape_id'] = current_shape_id

            #info from current waypoints loop
            new_row['shape_pt_sequence'] = row['shape_pt_sequence']
            #geting the point_id is incredibly hard because you have to match the point_id with the
            #current_lon, current_lat with is very technically dificult
            #how to calculate the length
            currentLon = row['shape_pt_lon']
            currentLat = row['shape_pt_lat']

            if lastPoint != None:
                new_row['length'] = helper.coordToMiles(currentLat, currentLon, lastPoint['current_lat'], lastPoint['current_lon'])
                distanceSinceStart += new_row['length']
                #add the length for the cumumulative distance
                new_row['dist'] = distanceSinceStart

                new_row['heading'] = helper.calculate_heading(currentLat, currentLon, lastPoint['current_lat'],lastPoint['current_lon'])
                lastPoint = {'current_lat': currentLat, 'current_lon': currentLon}
            else: #the case where we just start the way point
                new_row['length']  = 0 # seq i - seq i distance is 0
                new_row['dist'] = 0
                lastPoint = {'current_lat': currentLat, 'current_lon': currentLon}
                new_row['heading'] = "N/A" #the first point doesn't have any heading

            #calculate the point_id based on pt_lon, pt_lon
            big_constant = pow(10, 8)
            currentBigLon = int(currentLon * big_constant)
            currentBigLat = int(currentLat * big_constant)
            point_id = big_points_df[(big_points_df['point_lat'] == currentBigLat) &
                                     (big_points_df['point_lon'] == currentBigLon)].iloc[0]['point_id']
            new_row['point_id'] = point_id
            tables['Route_Point_Seq2'] = tables['Route_Point_Seq2'].append(pd.Series(new_row), ignore_index=True)
    print tables['Route_Point_Seq2']
    helper.write_table(tables, "Route_Point_Seq2")
    print "SUCCESS with Route Point Seq 2"

def transfers_old_version(tables, static_feed, arg_agency_id, trip2pattern):
    tables["Transfers"] = pd.DataFrame()
    columns = ['from_agency_id', 'from_id', 'to_agency_id', 'to_id', 'transfer_type',
               'min_transfer_time', 'transfer_dist']
    max_distance = 3  #this is in miles
    #initiate googlemaps for finding minimum transfer time and transfer distance
    gmaps = googlemaps.Client(key='AIzaSyB_yzsaBUOOo3ukoeDvtjg5Q32IGSkBUvU')

    for a, row in static_feed['stops'].iterrows():
        from_id = row['stop_id']

        #create a dataframe of stops that that are not the same pattern id as the from_id
        not_same_pattern = static_feed['stops']



        stops_in_range = helper.find_nearby_stops(from_id, static_feed['stops'], max_distance)
        for a, subrow in stops_in_range.iterrows():
            new_row = {}
            new_row['from_agency_id'] = arg_agency_id
            new_row['from_id'] = from_id
            new_row['to_agency_id'] = 1
            new_row['to_id'] =  subrow['stop_id']
            new_row['tranfer_type'] = 0

            #how to get the minimum transfer time
            result = helper.google_walking_distance_time(row['stop_lat'], row['stop_lon'],
                                                           subrow['stop_lat'], subrow['stop_lon'])
            distance = result['distance']
            time = result['duration']
            print distance
            print time
            new_row['min_transfer_time'] = time
            #distance returned by google maps
            new_row['tranfer_dist'] = distance
            tables["Transfers"] = tables["Transfers"].append(pd.Series(new_row), ignore_index=True)
    helper.write_table(tables, "Transfers")
    print "SUCCESS with Transfers"

def transfers(tables, static_feed, arg_agency_id):
    tables["Transfers"] = pd.DataFrame()
    columns = ['from_agency_id', 'from_id', 'to_agency_id', 'to_id', 'transfer_type',
               'min_transfer_time', 'transfer_dist']
    max_distance = 0.5  #this is in miles
    #initiate googlemaps for finding minimum transfer time and transfer distance
    gmaps = googlemaps.Client(key='AIzaSyB_yzsaBUOOo3ukoeDvtjg5Q32IGSkBUvU')

    #loop through unique patterns in route_stop_seq
    try:
        #What is shapes_df also had the trips it is a part of?
        #load the Route_Stop_Seq for miscellaneous things
        login = {'host':"localhost", 'user':SQL_USER,
                         'passwd':SQL_PWD, 'db': "PATHTransit"}
        route_stop_seq_df = helper.sql2df('Route_stop_seq', login)
    except Exception as e:
        print os.getcwd()
        print e
    counter = 0
    using_google_maps = False

    #get the unique list of patterns
    unique_patterns = route_stop_seq_df['pattern_id'].unique()


    #iterate through the unique list of patterns
    for from_pattern in unique_patterns:  #get one side of the pattern

        #get stop id's associated with the from_pattern
        from_pattern_stops = route_stop_seq_df[route_stop_seq_df['pattern_id'] == from_pattern]['stop_id']

        for to_pattern in unique_patterns: #get the other pattern
            if from_pattern != to_pattern:

                #get stop_id's assoicated with the to_pattern
                to_pattern_stops = route_stop_seq_df[route_stop_seq_df['pattern_id'] == to_pattern]['stop_id']

                #now you have two lists of stops from different patterns, iterate through this list of stops
                for from_pattern_stop in from_pattern_stops:
                    for to_pattern_stop in to_pattern_stops:
                        if arg_agency_id == 11: #if it's TriDelta, coerce from_pattern_stop to Int
                            from_stop_coord = static_feed['stops'][static_feed['stops']['stop_id']
                                                                           == int(from_pattern_stop)].iloc[0]
                            to_stop_coord = static_feed['stops'][static_feed['stops']['stop_id']
                                                                           == int(to_pattern_stop)].iloc[0]
                        elif arg_agency_id == 8: #if it's bart, then leave it as a string
                            from_stop_coord = static_feed['stops'][static_feed['stops']['stop_id']
                                                                           == from_pattern_stop].iloc[0]
                            to_stop_coord = static_feed['stops'][static_feed['stops']['stop_id']
                                                                           == to_pattern_stop].iloc[0]
                        else:
                            from_stop_coord = static_feed['stops'][static_feed['stops']['stop_id']
                                                                           == from_pattern_stop].iloc[0]
                            to_stop_coord = static_feed['stops'][static_feed['stops']['stop_id']
                                                                           == to_pattern_stop].iloc[0]
                        if using_google_maps:
                            distance_time_result = helper.google_walking_distance_time(from_stop_coord['stop_lat'], from_stop_coord['stop_lon'],
                                                                                       to_stop_coord['stop_lat'], to_stop_coord['stop_lon'])
                        else:
                            distance_time_result = {}
                            distance_time_result['distance'] = helper.coordToMiles(from_stop_coord['stop_lat'], from_stop_coord['stop_lon'],
                                                                                       to_stop_coord['stop_lat'], to_stop_coord['stop_lon'])
                            distance_time_result['duration'] = "Time Estimation N/A"

                        if distance_time_result['distance'] < max_distance: #if the distance is within boundaries add a new row
                            print distance_time_result
                            counter += 1

                            print "counter: " + str(counter)
                            new_row = {}
                            new_row['from_agency_id'] = arg_agency_id
                            new_row['from_id'] = from_pattern_stop
                            new_row['to_agency_id'] = 1
                            new_row['to_id'] = to_pattern_stop
                            new_row['tranfer_type'] = 0
                            new_row['min_transfer_time'] = distance_time_result['duration']
                            new_row['transfer_dist'] = distance_time_result['distance']
                            tables["Transfers"] = tables["Transfers"].append(pd.Series(new_row), ignore_index=True)

    helper.write_table(tables, "Transfers")
    print "SUCCESS with Transfers"

def gps_fixes(tables, static_feed, trip_update_feed, alert_feed, vehicle_position_feed, arg_agency_id, trip2pattern):
    columns = ['agency_id', 'veh_id', 'RecordedDate', 'RecordedTime', 'UTC_at_date', 'latitude',
               'longitude', 'speed', 'course']
    tables['gps_fixes'] = pd.DataFrame()
    for entity in trip_update_feed.entity:
        new_row = {}
        new_row['agency_id'] = arg_agency_id
        new_row['veh_id'] = entity.id
        new_row['RecordedDate'] = str(datetime.datetime.now().strftime('%Y-%m-%d'))
        new_row['RecordedTime'] = str(datetime.datetime.now().strftime('%H:%M:%S'))
        new_row['UTC_at_date'] = str(datetime.datetime.now().strftime('%Y-%m-%d'))
        new_row['UTC_at_time'] = str(datetime.datetime.now().strftime('%H:%M:%S'))
        new_row['latitude'] = entity.vehicle.position.latitude
        new_row['longitude'] = entity.vehicle.position.longitude
        new_row['speed'] = entity.vehicle.position.speed
        new_row['course'] = "N/A"
        tables["gps_fixes"] = tables["gps_fixes"].append(pd.Series(new_row), ignore_index=True)
    helper.write_table(tables, "gps_fixes")
    print "SUCCESS with GPS Fixes"


def transit_eta(tables, static_feed, trip_update_feed, alert_feed, vehicle_position_feed, arg_agency_id, trip2pattern):
    columns = ['agency_id', 'RecordedDate', 'RecordedTime', 'veh_id', 'veh_lat', 'veh_lon',
               'veh_speed', 'veh_location_time', 'route_short_name', 'route_dir', 'day', 'run',
               'pattern_id', 'stop_id', 'seq', 'ETA']

    try:
       login = {'host':"localhost", 'user':SQL_USER,
                         'passwd':SQL_PWD, 'db': "PATHTransit"}
       run_pattern_df = helper.sql2df('RunPattern', login)
    except Exception as e:
        print e
    tables['TransitETA'] = pd.DataFrame()
    for entity in trip_update_feed.entity:
        new_row = {}
        new_row['agency_id'] = arg_agency_id
        new_row['RecordedDate'] = str(datetime.datetime.now().strftime('%Y-%m-%d'))
        new_row['RecordedTime'] = str(datetime.datetime.now().strftime('%H:%M:%S'))
        new_row['veh_id'] = entity.id
        new_row['veh_lat'] = entity.vehicle.position.latitude
        new_row['veh_lon'] = entity.vehicle.position.longitude
        new_row['veh_speed'] = entity.vehicle.position.speed

        #VTA and BART are in Bay area so time is the same
        new_row['veh_location_time'] =  str(datetime.datetime.now().strftime('%H:%M:%S'))
        # new_row['route_short_name'] = runPattern_entry['route_short_name']
        # new_row['day'] = runPattern_entry['day']
        # new_row['run'] = runPattern_entry['run']
        # new_row['pattern_id'] = runPattern_entry['pattern_id']
        # new_row['route_dir'] = runPattern_entry['route_dir']
        new_row['stop_id'] = entity.vehicle.stop_id
        new_row['seq'] = entity.vehicle.current_stop_sequence
        tables["TransitETA"] = tables["TransitETA"].append(pd.Series(new_row), ignore_index=True)
    helper.write_table(tables, "TransitETA")
    print "SUCCESS with TransitETA"
