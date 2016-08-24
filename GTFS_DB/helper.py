#!/usr/bin/env python
# -*- coding: utf-8 -*-
from pandas.io import sql
from math import cos, asin, sqrt
import math
import googlemaps
from google.transit import gtfs_realtime_pb2
import MySQLdb
import requests
import pandas as pd
import transit_agencies
from os import path
import os
from StringIO import StringIO
import zipfile
import urllib
import logging
from math import radians, cos, sin, asin, sqrt

SQL_USER = "root"
SQL_PWD = "PATH452RFS"

def csv2df(csv_file):
	df = pd.read_csv(csv_file, sep = ',', header = 0)
	df.replace('\"', '')
	return df

# def df2sql(dataframe, df_name, login, exist_flag='append'):
# 	con = MySQLdb.connect(host=login['host'], user=login['user'], passwd=login['passwd'], db=login['db'])
# 	seems to have no way to tell what types each column should be
	# dataframe.to_sql(con=con, name=df_name, flavor='mysql', if_exists=exist_flag, index=False)
	# con.close()

def sql2df(df_name, login):
	con = MySQLdb.connect(host=login['host'], user=login['user'], passwd=login['passwd'], db=login['db'])
	# takes in no consideration for what the types should be
	df = pd.read_sql('SELECT * FROM {0}'.format(df_name), con)
	df.replace('\"', '')
	con.close()
	return df

#implementation of the haversine formula for coordinates
def coordToM(lat1, lon1, lat2, lon2):
    p = 0.017453292519943295
    a = 0.5 - cos((lat2 - lat1) * p)/2 + cos(lat1 * p) * cos(lat2 * p) * (1 - cos((lon2 - lon1) * p)) / 2
    return 12742 * asin(sqrt(a)) * 1000

def coordToMiles(lat1, lon1, lat2, lon2):
    return (coordToM(lat1, lon1, lat2, lon2) / 1609.344)

def calculate_heading(lat1, lon1, lat2, lon2):
    """
    Calculates the bearing between two points.
    The formulae used is the following:
        θ = atan2(sin(Δlong).cos(lat2),
                  cos(lat1).sin(lat2) − sin(lat1).cos(lat2).cos(Δlong))
    :Parameters:
      - `pointA: The tuple representing the latitude/longitude for the
        first point. Latitude and longitude must be in decimal degrees
      - `pointB: The tuple representing the latitude/longitude for the
        second point. Latitude and longitude must be in decimal degrees
    :Returns:
      The bearing in degrees
    :Returns Type:
      float
    """
    # lat1 = math.radians(pointA[0])
    # lat2 = math.radians(pointB[0])

    diffLong = math.radians(lon2 - lon1)

    x = math.sin(diffLong) * math.cos(lat2)
    y = math.cos(lat1) * math.sin(lat2) - (math.sin(lat1)
            * math.cos(lat2) * math.cos(diffLong))

    initial_bearing = math.atan2(x, y)

    # Now we have the initial bearing but math.atan2 return values
    # from -180° to + 180° which is not what we want for a compass bearing
    # The solution is to normalize the initial bearing as shown below
    initial_bearing = math.degrees(initial_bearing)
    compass_bearing = (initial_bearing + 360) % 360

    return compass_bearing

#this function takes a stop_id and returns a pandas dataframe of nearby stops
# def find_nearby_stops(from_id, stops_df, max_distance):
#     dataframe_of_stops = pd.DataFrame()
#     for a, row in stops_df.iterrows():
#         if from_id != row['stop_id']:
#             print "from_id: " + from_id
#             print "to_id: " + row['stop_id']
#             print "----------------------"
#             from_id_row = stops_df[stops_df['stop_id'] == from_id].iloc[0]
#             lat1, lon1 = from_id_row['stop_lat'], from_id_row['stop_lon']
#             lat2, lon2 = row['stop_lat'], row['stop_lon']
#             distance_between = google_walking_distance_time(lat1, lon1,lat2, lon2)['distance']
#             print "done"
#             if (distance_between < max_distance):
#                 new_row = {}
#                 new_row['stop_id'] = row['stop_id']
#                 new_row['stop_lat'] = row['stop_lat']
#                 new_row['stop_lon'] = row['stop_lon']
#                 dataframe_of_stops = dataframe_of_stops.append(pd.Series(new_row), ignore_index=True)
#     return dataframe_of_stops



def meters_to_miles(meters):
    return meters / 1609.3444

#returns two things
#1. totalDistance(Miles)
#2. totalDuration(seconds) for the trip
def google_walking_distance_time(lat1, lon1, lat2, lon2):
    gmaps = googlemaps.Client(key='AIzaSyB_yzsaBUOOo3ukoeDvtjg5Q32IGSkBUvU')
    routes = gmaps.directions(origin={'lat': lat1, 'lng': lon1},
                          destination={'lat': lat2, 'lng': lon2},
                                        mode="walking", units='imperial')

    legs = routes[0]['legs']
    totalDistance = 0
    totalDuration = 0

    for x in range(0, len(legs)):
        totalDistance += meters_to_miles(legs[x]['distance']['value'])
        totalDuration += legs[x]['duration']['value']
    return {'distance': totalDistance, 'duration': totalDuration}


def optional_field(index, column, dataframe, default='N/A'):
    row = dataframe.iloc[index]
    return row[column] if (column in dataframe.columns and not pd.isnull(row[column])) else default

#Get Static Feed
REQUIRED_GTFS_FILES = ["agency", "stops", "routes", "trips", "stop_times", "calendar"]

def get_static(agency):
    feed = {}

    pathname = "./agencies/" + agency + "/"


    #IF LOCAL FILES EXISTS
    if path.exists(pathname):
        for f in os.listdir(pathname):
            if f[-4:] == ".txt" or f[-4:] == ".csv": #if its a txt file
                with open(pathname + "/" + str(f)) as csvfile:
                    print "f[:-4] " + f[:-4]
                    feed[f[:-4]] = csv2df(csvfile)

        return feed

    #PULL NEW INFORMATION
    request = requests.get(transit_agencies.get(agency, "static"), stream=True)

    if request.status_code != 200:
        print "Error! Did not reach"

    if not path.exists(pathname):
        os.makedirs(pathname)


    #Unzip GTFS static
    buf = request.raw.read()
    zipdata = StringIO()
    zipdata.write(buf)

    with open(pathname + "gtfs.zip", "w") as zipout:
        zipout.write(buf)

    z = zipfile.ZipFile(zipdata)
    z.extractall(pathname)


    # format static feed
    for f in z.namelist():
        with z.open(f) as csvfile:
            feed[f[:-4]] = csv2df(csvfile).rename(columns=lambda s: str(s.decode('ascii', 'ignore')))

    for f in REQUIRED_GTFS_FILES:
        if f not in feed:
            print "Incomplete GTFS dataset"
            return None

    z.close()

    return feed


#Get Realtime feed
def get_realtime(agency, mode):
    URL = transit_agencies.get(agency, mode)
    if URL == None or len(URL) == 0:
        logging.debug("**********************")
        return None
    response = urllib.urlopen(URL)
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(response.read())
    return feed



def write_table(tables, name):
    db = MySQLdb.connect(host="localhost", user="root",
                         passwd="PATH452RFS", db="PATHTransit")
    # db = MySQLdb.connect(host="http://52.53.208.65", user="root",
    #     passwd="PATH452RFS", db="TrafficTransit")
    tables[name].to_sql(con=db, flavor='mysql', name=name, if_exists="replace")
    # tables[name].to_sql(con=db, flavor='mysql', name=name, if_exists="replace")
