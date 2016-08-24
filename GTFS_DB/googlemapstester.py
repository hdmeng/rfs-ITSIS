import googlemaps
import pandas as pd
import os
import json
import helper

try:
    #What is shapes_df also had the trips it is a part of?
    stops_df = pd.read_csv("agencies/bart/stops.txt")
except Exception as e:
    print os.getcwd()
    print e

gmaps = googlemaps.Client(key='AIzaSyB_yzsaBUOOo3ukoeDvtjg5Q32IGSkBUvU')
stop1 = stops_df.iloc[0]
stop2 = stops_df.iloc[1]


print "stop 1: " + stop1['stop_id']
print "stop 2: " + stop2['stop_id']


routes = gmaps.directions(origin={'lat': stop1['stop_lat'], 'lng': stop1['stop_lon']},
                          destination={'lat': stop2['stop_lat'], 'lng': stop2['stop_lon']},
                                       mode="walking", units='imperial')

legs = routes[0]['legs']
totalDistance = 0
totalDuration = 0


print legs[0]['distance']

for x in range(0, len(legs)):
    totalDistance += helper.meters_to_miles(legs[x]['distance']['value'])
    totalDuration += legs[x]['duration']['value']


print 15953 / 1609.344
print "totalDistance:" + str(totalDistance)
print "totalDuration:" + str(totalDuration)




