import math

import googlemaps
from geopy import distance


def get_distance(a_lat, a_lon, b_lat, b_lon):
    return distance.vincenty((a_lat, a_lon), (b_lat, b_lon)).meters

# https://gist.github.com/jeromer/2005586
# TODO(erchpito) is this w.r.t. north?


def get_heading(a_lat, a_lon, b_lat, b_lon):
    lat1 = math.radians(a_lat)
    lat2 = math.radians(b_lat)

    diffLong = math.radians(b_lon - a_lon)

    x = math.sin(diffLong) * math.cos(lat2)
    y = math.cos(lat1) * math.sin(lat2) - (math.sin(lat1) * math.cos(lat2) *
                                           math.cos(diffLong))

    initial_bearing = math.atan2(x, y)

    initial_bearing = math.degrees(initial_bearing)
    compass_bearing = (initial_bearing + 360) % 360

    return compass_bearing


def get_distance_and_time(a_lat, a_lon, b_lat, b_lon):
    # TODO(erchpito) get rid of this, find out how to inject
    gmaps = googlemaps.Client(key='AIzaSyB_yzsaBUOOo3ukoeDvtjg5Q32IGSkBUvU')

    directions_result = gmaps.directions(
        (a_lat, a_lon), (b_lat, b_lon), mode="walking", units='metric')

    legs = directions_result[0]['legs']
    totalDistance = 0
    totalDuration = 0

    for leg in legs:
        totalDistance += leg['distance']['value']
        totalDuration += leg['duration']['value']
    return (totalDistance, totalDuration)
