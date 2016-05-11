#! /usr/bin/python

from datetime import date, timedelta, datetime
from google.transit import gtfs_realtime_pb2
from os import path
from pandas.io import sql
from StringIO import StringIO

import code
import csv
import getpass
import logging
import MySQLdb
import numpy as np
import os
import pandas as pd
import pymysql
import pytz
import sys
import time
import warnings

import df_helper
import gtfs_helper
import transit_agencies

def interpret(agency, static_feed, trip_update_feed, alert_feed, vehicle_position_feed, checksum, refresh, login, local):

	pathname = "./agencies/" + agency + "/processed/"
	if not path.exists(pathname):
		os.makedirs(pathname)

	for fn, df in static_feed.iteritems():
		logging.debug("%s\n%s\n%s\n", fn, "----------" * 8, df)
	logging.debug("Trip Update Timestamp: %s", trip_update_feed.header.timestamp if trip_update_feed else None)
	logging.debug("Alert Timestamp: %s", alert_feed.header.timestamp if alert_feed else None)
	logging.debug("Vehicle Position Timestamp: %s", vehicle_position_feed.header.timestamp if vehicle_position_feed else None)

	def optional_field(index, column, dataframe, default='N/A'):
		row = dataframe.iloc[index]
		return row[column] if (column in dataframe.columns and not pd.isnull(row[column])) else default

	def can_read_table(table):
		if local:
			return path.exists(pathname + table + '.csv')
		else:
			try:
				with MySQLdb.connect(host=login['host'], user=login['user'], passwd=login['passwd'], db=login['db']) as con:
					con.execute('SHOW TABLES LIKE \'{0}\''.format(table))
					return True if con.fetchall() else False
			except MySQLdb.Error, e:
				try:
					logging.debug('MySQL Error {0}: {1}'.format(e.args[0], e.args[1]))
				except IndexError:
					logging.debug('MySQL Error: {0}'.format(str(e)))
				sys.exit(0)

	def read_table(table):
		if local:
			with open(pathname + table + '.csv', 'rb') as csvfile:
				return df_helper.csv2df(csvfile)
		else:
			logging.debug('Read from database')
			return df_helper.sql2df(table, login)

	def write_table(table):
		if local:
			tables[table].to_csv(pathname + table + '.csv', sep = ',', index = False)
		else:
			df_helper.df2sql(tables[table], table, login=login, exist_flag=('replace' if refresh else 'append'))

	# check if newer timestamp
	# process entity

	tables = {}
	trip2pattern = {}
	trip2vehicle = {}
	agency_id = transit_agencies.get(agency, "id")

	# Static Feed
	# ---- Task 1 ----
	# Agency
	# int agency_id -> 'agency_id' int(10) unsigned
	# required string agency_name  -> 'agency_name' varchar(255)
	# required string agency_url -> 'agency_url' varchar(255)
	# required string agency_timezone -> 'agency_timezone' smallint(6)
	# optional string agency_lang -> 'agency_lang' varchar(255)
	# optional string agency_phone -> 'agency_phone' varchar(255)
	# required string agency_timezone -> 'timezone_name' varchar(45)
	# PRIMARY KEY ('agency_id')
	# KEY ('agency_timezone')

	if can_read_table('Agency') and not refresh:
		tables['Agency'] = read_table('Agency')
	else:
		columns = ['agency_id', 'agency_name', 'agency_url', 'agency_timezone', 'agency_lang', 'agency_phone', 'timezone_name']
		tables['Agency'] = pd.DataFrame(index=np.r_[0:len(static_feed['agency'].index)], columns=columns)
		for i, row in static_feed['agency'].iterrows():
			new_row = tables["Agency"].loc[i]
			new_row['agency_id'] = agency_id
			new_row['agency_name'] = row['agency_name']
			new_row['agency_url'] = row['agency_url']
			timezone = pytz.timezone(row['agency_timezone'])
			offset = datetime.now(timezone).strftime('%z')
			new_row['agency_timezone'] = int(offset) # figure out a way to get the offset without regard to DST
			new_row['agency_lang'] = optional_field(i, 'agency_lang', static_feed['agency'])
			new_row['agency_phone'] = optional_field(i, 'agency_phone', static_feed['agency'])
			new_row['timezone_name'] = row['agency_timezone']
		write_table('Agency')
	logging.debug("%s\n%s\n%s\n", 'Agnecy', "----------" * 8, tables['Agency'])


	# Routes
	# int agency_id -> 'agency_id' int(10) unsigned
	# required string route_short_name -> 'route_short_name' varchar(255)
	# optional int direction_id -> 'route_dir' int(10) unsigned
	# required int route_type -> 'route_type' int(10) unsigned
	# required string route_long_name -> 'route_long_name' varchar(255) default 'N/A'
	# optional string route_desc -> 'route_desc' varchar(255) default 'N/A'
	# optional string route_url -> 'route_url' varchar(255) default 'N/A'
	# optional string route_color -> 'route_color' varchar(255) default 'FFFFFF',
	# optional string route_text_color -> 'route_text_color' varchar(255) default '000000'
	# required string route_id -> 'route_id' varchar(255) default '000000'
	# zipfile MD5 -> 'version' varchar(255)

	# if path.exists(pathname + 'Routes.csv') and not refresh:
	# 	with open(pathname + 'Routes.csv', 'rb') as csvfile:
	# 		tables['Routes'] = df_helper.csv2df(csvfile)
	# else:
	# 	columns = ['agency_id', 'route_short_name', 'route_dir', 'route_type', 'route_long_name', 'route_desc', 'route_url', 'route_color', 'route_text_color', 'route_id', 'version']
	# 	tables['Routes'] = pd.DataFrame()
	# 	for i, row in static_feed['routes'].iterrows():
	# 		for direction_id in static_feed['trips'].loc[static_feed['trips']['route_id'] == row['route_id']]['direction_id'].unique():
	# 			new_row = {}
	# 			new_row['agency_id'] = agency_id
	# 			new_row['route_short_name'] = str(optional_field(i, 'route_short_name', static_feed['routes'], static_feed['routes'].iloc[i]['route_long_name']))
	# 			new_row['route_dir'] = direction_id
	# 			new_row['route_type'] = int(row['route_type'])
	# 			new_row['route_long_name'] = str(optional_field(i, 'route_long_name', static_feed['routes'], static_feed['routes'].iloc[i]['route_short_name']))
	# 			new_row['route_desc'] = optional_field(i, 'route_desc', static_feed['routes'])
	# 			new_row['route_url'] = optional_field(i, 'route_url', static_feed['routes'])
	# 			new_row['route_color'] = optional_field(i, 'route_color', static_feed['routes'], default='FFFFFF').upper()
	# 			new_row['route_text_color'] = optional_field(i, 'route_text_color', static_feed['routes'], default='000000').upper()
	# 			new_row['route_id'] = str(row['route_id'])
	# 			new_row['version'] = checksum
	# 			tables["Routes"] = tables['Routes'].append(pd.Series(new_row), ignore_index=True)
	# 	tables['Routes'].to_csv(pathname + 'Routes' + ".csv", sep = ',', index = False)
	# logging.debug("%s\n%s\n%s\n", 'Routes', "----------" * 8, tables['Routes'])

	# # # Stops
	# # int agency_id -> 'agency_id' int(10) unsigned
	# # required string stop_id -> 'stop_id' bigint(20) unsigned
	# # optional string stop_code -> 'stop_code' varchar(255) default 'N/A'
	# # required string stop_name -> 'stop_name' varchar(255)
	# # optional string stop_desc -> 'stop_desc' varchar(255) default 'N/A'
	# # required float stop_lat -> 'stop_lat' double
	# # required float stop_lon -> 'stop_lon' double
	# # stop_lat and stop_lon -> 'lat_lon' point
	# # optional string stop_url -> `stop_url` varchar(255) default 'N/A'
	# # optional int location_type -> 'location_type' int(10) unsigned default '0'
	# # optional int parent_station -> 'parent_station' bigint(20) unsigned default '0'
	# # optional int wheelchair_boarding -> 'wheelchair_boarding' int(10) unsigned default '0'
	# # zipfile MD5 -> 'version' varchar(255)

	# if path.exists(pathname + 'Stops.csv') and not refresh:
	# 	with open(pathname + 'Stops.csv', 'rb') as csvfile:
	# 		tables['Stops'] = df_helper.csv2df(csvfile)
	# else:
	# 	columns = ['agency_id', 'stop_id', 'stop_code', 'stop_name', 'stop_desc', 'stop_lat', 'stop_lon', 'lat_lon', 'stop_url', 'location_type', 'parent_station', 'wheelchair_boarding', 'version']
	# 	tables['Stops'] = pd.DataFrame(index=np.r_[0:len(static_feed['stops'].index)], columns=columns)
	# 	for i, row in static_feed['stops'].iterrows():
	# 		new_row = tables["Stops"].loc[i]
	# 		new_row['agency_id'] = agency_id
	# 		new_row['stop_id'] = str(row['stop_id'])
	# 		new_row['stop_code'] = str(optional_field(i, 'stop_code', static_feed['stops']))
	# 		new_row['stop_name'] = str(row['stop_name'])
	# 		new_row['stop_desc'] = str(optional_field(i, 'stop_desc', static_feed['stops']))
	# 		new_row['stop_lat'] = float(row['stop_lat'])
	# 		new_row['stop_lon'] = float(row['stop_lon'])
	# 		new_row['lat_lon'] = 0 # some calculations, ignore until using MySQL
	# 		new_row['stop_url'] = str(optional_field(i, 'stop_url', static_feed['stops']))
	# 		new_row['location_type'] = int(optional_field(i, 'location_type', static_feed['stops'], 0))
	# 		new_row['parent_station'] = int(optional_field(i, 'parent_station', static_feed['stops'], 0))
	# 		new_row['wheelchair_boarding'] = int(optional_field(i, 'wheelchair_boarding', static_feed['stops'], 0))
	# 		new_row['version'] = checksum
	# 	tables['Stops'].to_csv(pathname + 'Stops' + ".csv", sep = ',', index = False)
	# logging.debug("%s\n%s\n%s\n", 'Stops', "----------" * 8, tables['Stops'])

	# # # Route_stop_seq
	# # int agency_id -> 'agency_id' int(10) unsigned
	# # required string route_short_name -> 'route_short_name' varchar(255)
	# # optional int direction_id -> 'route_dir' int(10) unsigned
 # 	# route_short_name + route_dir + seq -> 'pattern_id' varchar(255)
 # 	#  `stop_id` int(10) unsigned NOT NULL,
 # 	#  `seq` int(10) unsigned NOT NULL,
 # 	#  `is_time_point` int(10) unsigned NOT NULL Default 0,
 # 	# zipfile MD5 -> 'version' varchar(255)

 # 	if path.exists(pathname + 'Route_stop_seq.csv') and not refresh:
	# 	with open(pathname + 'Route_stop_seq.csv', 'rb') as csvfile:
	# 		tables['Route_stop_seq'] = df_helper.csv2df(csvfile)
	# 	with open(pathname + 'Trip2Pattern.csv', 'rb') as f:
	# 		reader = csv.reader(f)
	# 		trip2pattern = dict(reader)
 # 	else:
	#  	columns = ['agency_id', 'route_short_name', 'route_dir', 'pattern_id', 'stop_id', 'seq', 'is_time_point', 'version']
	# 	tables['Route_stop_seq'] = pd.DataFrame()
	# 	for i, row in static_feed['routes'].iterrows():
	# 		route_id = row['route_id']
	# 		patterns = []
	# 		for j, subrow in static_feed['trips'].loc[static_feed['trips']['route_id'] == route_id].iterrows():
	# 			trip_id = subrow['trip_id']
	# 			direction_id = subrow['direction_id'] if 'direction_id' in subrow else 0
	# 			trip_id_block = static_feed['stop_times'].loc[static_feed['stop_times']['trip_id'] == trip_id]
	# 			sequence = trip_id_block['stop_id'].tolist()
	# 			if str(sequence) not in patterns:
	# 				patterns += [str(sequence)]
	# 			pattern_num = patterns.index(str(sequence)) + 1
	# 			route_short_name = str(optional_field(i, 'route_short_name', static_feed['routes'], static_feed['routes'].iloc[i]['route_long_name']))
	# 			pattern_id = "{0}_{1}_{2}".format(route_short_name, direction_id, pattern_num)
	# 			for k, subsubrow in trip_id_block.iterrows():
	# 				new_row = {}
	# 				new_row['agency_id'] = agency_id
	# 				new_row['route_short_name'] = route_short_name
	# 				new_row['route_dir'] = direction_id
	# 				new_row['pattern_id'] = pattern_id
	# 				pattern_id = new_row['pattern_id']
	# 				new_row['stop_id'] = str(subsubrow['stop_id'])
	# 				new_row['seq'] = subsubrow['stop_sequence']
	# 				new_row['is_time_point'] = int(optional_field(k, 'timepoint', static_feed['stop_times'], 0))
	# 				new_row['version'] = checksum
	# 				tables["Route_stop_seq"] = tables["Route_stop_seq"].append(pd.Series(new_row), ignore_index=True)
	# 			trip2pattern[trip_id] = pattern_id
	# 		for sequence in patterns:
	#  			logging.debug("{0}: {1} = {2}".format(route_id, patterns.index(sequence) + 1, sequence))
	# 	tables['Route_stop_seq'].to_csv(pathname + 'Route_stop_seq' + ".csv", sep = ',', index = False)
	# 	with open(pathname + 'Trip2Pattern.csv', 'wb') as f:
	# 		writer = csv.writer(f)
	# 		for key, value in trip2pattern.items():
	# 			writer.writerow([key, value])
	# logging.debug("%s\n%s\n%s\n", 'Route_stop_seq', "----------" * 8, tables['Route_stop_seq'])

	# # # RunPattern
	# # `agency_id` int(10) unsigned NOT NULL,
	# # `route_short_name` varchar(255) NOT NULL,
	# # `start_date` date NOT NULL,
	# # `end_date` date NOT NULL,
	# # `service_id` varchar(255) NOT NULL,
	# # `day` char(7) NOT NULL,
	# # `route_dir` int(10) unsigned NOT NULL,
	# # `run` int(10) unsigned NOT NULL,
	# # `pattern_id` varchar(255) NOT NULL,
	# # `trip_headsign` varchar(255) NOT NULL,
	# # `trip_id` bigint(20) unsigned NOT NULL,
	# # `version` varchar(255) NOT NULL,

	# if path.exists(pathname + 'RunPattern.csv') and not refresh:
	# 	with open(pathname + 'RunPattern.csv', 'rb') as csvfile:
	# 		tables['RunPattern'] = df_helper.csv2df(csvfile)
	# else:
	# 	columns = ['agency_id', 'route_short_name', 'start_date', 'end_date', 'service_id', 'day', 'route_dir', 'run', 'pattern_id', 'trip_headsign', 'trip_id', 'version']
	# 	# code.interact(local=locals())
	# 	tables['RunPattern'] = pd.DataFrame(index=np.r_[0:len(static_feed['trips'].index)], columns=columns)
	# 	day_count = {}
	# 	for i, row in static_feed['trips'].iterrows():
	# 		new_row = tables["RunPattern"].loc[i]
	# 		new_row['agency_id'] = agency_id
	# 		j = np.where(static_feed['routes']['route_id'] == row['route_id'])[0][0]
	# 		new_row['route_short_name'] = str(optional_field(j, 'route_short_name', static_feed['routes'], static_feed['routes'].iloc[j]['route_long_name']))
	# 		new_row['service_id'] = row['service_id']
	# 		calendar = static_feed['calendar'].loc[static_feed['calendar']['service_id'] == row['service_id']].iloc[0]
	# 		new_row['start_date'] = datetime.strptime(str(calendar['start_date']), "%Y%m%d")
	# 		new_row['end_date'] = datetime.strptime(str(calendar['end_date']), "%Y%m%d")
	# 		new_row['day'] = "{0}{1}{2}{3}{4}{5}{6}".format(calendar['monday'], calendar['tuesday'], calendar['wednesday'], calendar['thursday'], calendar['friday'], calendar['saturday'], calendar['sunday'])
	# 		if new_row['day'] not in day_count:
	# 			day_count[new_row['day']] = 1
	# 		new_row['route_dir'] = int(optional_field(i, 'direction_id', static_feed['trips'], 0))
	# 		new_row['run'] = day_count[new_row['day']]
	# 		day_count[new_row['day']] += 1
	# 		new_row['pattern_id'] = trip2pattern[row['trip_id']]
	# 		new_row['trip_headsign'] = optional_field(i, 'trip_headsign', static_feed['trips'], static_feed['stop_times'].loc[static_feed['stop_times']['trip_id'] == row['trip_id']]['stop_headsign'].iloc[0])
	# 		new_row['trip_id'] = str(row['trip_id'])
	# 		new_row['version'] = checksum
	# 	tables['RunPattern'].to_csv(pathname + 'RunPattern' + ".csv", sep = ',', index = False)
	# logging.debug("%s\n%s\n%s\n", 'RunPattern', "----------" * 8, tables['RunPattern'])

	# # # Schedules
	# # tables["Schedules"] = pd.DataFrame()

	# # # Route_point_seq
	# # tables["Route_point_seq"] = pd.DataFrame()

	# # # Points
	# # tables["Points"] = pd.DataFrame()

	# # # Fare (not required for now)
	# # tables["Fare"] = pd.DataFrame()

	# # # Calendar_dates
	# # tables["Calendar_dates"] = pd.DataFrame()

	# # # ---- Task 2 ----
	# # # Transfers
	# # tables["Transfers"] = pd.DataFrame()

	# # # ---- Task 3 ----
	# # code.interact(local=locals())
	# # # gps_fixes
	# columns = ['agency_id', 'veh_id', 'RecordedDate', 'RecordedTime', 'UTC_at_date', 'UTC_at_time', 'latitude', 'longitude', 'speed', 'course']
	# tables['gps_fixes'] = pd.DataFrame()
	# for entity in vehicle_position_feed.entity:
	# 	update = gtfs_helper.VehiclePosition(entity.vehicle)
	# 	trip = update.get_trip_descriptor()
	# 	position = update.get_position()
	# 	vehicle = update.get_vehicle_descriptor()
	# 	misc = update.get_update_fields()

	# 	new_row = {}
	# 	new_row['agency_id'] = agency_id
	# 	new_row['veh_id'] = int(vehicle['id'] if (vehicle and 'id' in vehicle) else -1)
	# 	new_row['RecordedDate'] = str(datetime.now().strftime('%Y-%m-%d'))
	# 	new_row['RecordedTime'] = str(datetime.now().strftime('%H:%M:%S'))
	# 	timestamp = datetime.datetime.fromtimestamp(int(misc['timestamp'])) if (misc and 'timestamp' in misc) else -1
	# 	new_row['UTC_at_date'] = str(timestamp.strftime('%Y-%m-%d') if (timestamp == -1) else 'N/A')
	# 	new_row['UTC_at_time'] = str(timestamp.strftime('%H:%M:%S') if (timestamp == -1) else 'N/A')
	# 	new_row['latitude'] = float(position['latitude'] if (position and 'latitude' in position) else -1)
	# 	new_row['longitude'] = float(position['longitude'] if (position and 'longitude' in position) else -1)
	# 	new_row['speed'] = float(position['speed'] if (position and 'speed' in position) else -1)
	# 	new_row['course'] = float(position['bearing'] if (position and 'bearing' in position) else -1)
	# 	tables["gps_fixes"] = tables["gps_fixes"].append(pd.Series(new_row), ignore_index=True)
	# 	if (trip and 'trip_id' in trip) and (vehicle and 'id' in vehicle):
	# 		trip2vehicle[trip['trip_id']] = vehicle['id']
	
	# tables['gps_fixes'].to_csv(pathname + 'gps_fixes' + ".csv", sep = ',', index = False)
	# logging.debug("%s\n%s\n%s\n", 'gps_fixes', "----------" * 8, tables['gps_fixes'])

	# # # TransitETA
	# # agency_id: int(10) unsigned NOT NULL <- gps_fixes
	# # RecordedDate: date NOT NULL
	# # RecordedTime: time NOT NULL
	# # veh_id: int(11) NOT NULL
	# # veh_lat: dobule NOT NULL
	# # veh_lon: double NOT NULL
	# # veh_speed: double unsigned NOT NULL
	# # veh_location_time: bigint(20) NOT NULL
	# # route_short_name: varchar(255) NOT NULL
	# # route_dir: int(10) unsigned NOT NULL
	# # day: char(7) NOT NULL
	# # run: int(10) NOT NULL
	# # pattern_id: varchar(255) NOT NULL
	# # stop_id: int(10) unsigned NOT NULL
	# # seq: int(10) unsigned NOT NULL
	# # ETA: time NOT NULL

	# columns = ['agency_id', 'RecordedDate', 'RecordedTime', 'veh_id', 'veh_lat', 'veh_lon', 'veh_speed', 'veh_location_time', 'route_short_name', 'route_dir', 'day', 'run', 'pattern_id', 'stop_id', 'seq', 'ETA']
	# tables['TransitETA'] = pd.DataFrame()
	# # code.interact(local=locals())
	# # exit()
	# for entity in trip_update_feed.entity:
	# 	update = gtfs_helper.TripUpdate(entity.trip_update)
	# 	descriptor = update.get_trip_descriptor()
	# 	stop_updates = update.get_stop_time_updates()

	# 	trip_id = descriptor['trip_id']
	# 	runPattern_entry = tables['RunPattern'].loc[tables['RunPattern']['trip_id'] == '10SFO10'].iloc[0]
	# 	route_short_name = runPattern_entry['route_short_name']
	# 	route_dir = runPattern_entry['route_dir']
	# 	day = runPattern_entry['day']
	# 	run = runPattern_entry['run']
	# 	pattern_id = runPattern_entry['pattern_id']
	# 	trip_id_block = static_feed['stop_times'].loc[static_feed['stop_times']['trip_id'] == trip_id]
	# 	vehicle_id = int(trip2vehicle[trip_id] if trip2vehicle and trip_id else -1)
	# 	if vehicle_id != -1:
	# 		vehicle_entry = tables['gps_fixes'].loc[tables['gps_fixes']['veh_id'] == vehicle_id].iloc[0]
	# 		vehicle_lat = vehicle_entry['latitude']
	# 		vehicle_lon = vehicle_entry['longitude']
	# 		vehicle_speed = vehicle_entry['speed']
	# 		s = datetime.datetime.strptime(vehicle_entry['UTC_at_date'] + ' ' + vehicle_entry['UTC_at_time'], '%Y-%m-%d %H:%M:%S')
	# 		vehicle_location_time = time.mktime(s.timetuple())
	# 	else:
	# 		vehicle_lat = -1
	# 		vehicle_lon = -1
	# 		vehicle_speed = -1
	# 		vehicle_location_time = -1
	# 	code.interact(local=locals())
	# 	exit()

	# 	i = 0
	# 	while i < len(stop_updates):
	# 		stop_update = stop_updates[i]
	# 		if 'departure' in stop_update or 'arrival' in stop_update:
	# 			stop_seq = stop_update['stop_sequence']
	# 			stop_seq_til = stop_updates[i + 1]['stop_sequence'] if i + 1 < len(stop_updates) else len(trip_id_block) + 1

	# 			time_diff = stop_update['departure' if 'departure' in stop_update else 'arrival']

	# 			if 'delay' in time_diff:
	# 				delay = timedelta(seconds=time_diff['delay'])
	# 			else: # it would seem this is stop_sequence specific
	# 				delay_time = datetime.datetime.fromtimestamp(int(time_diff['time']))
	# 				schedule_time = datetimeFromHMS(trip_id_block[trip_id_block['stop_sequence'] == stop_seq].iloc[0]['departure_time'])
	# 				delay = delay_time - schedule_time

	# 			while stop_seq < stop_seq_til:
	# 				stop = trip_id_block[trip_id_block['stop_sequence'] == stop_seq].iloc[0]
	# 				new_row = {}
	# 				new_row['agency_id'] = agency_id
	# 				new_row['RecordedDate'] = str(datetime.now().strftime('%Y-%m-%d'))
	# 				new_row['RecordedTime'] = str(datetime.now().strftime('%H:%M:%S'))
	# 				new_row['veh_id'] = int(vehicle_id if vehicle_id != -1 else -1)
	# 				new_row['veh_lat'] = float(vehicle_lat if vehicle_lat != -1 else -1)
	# 				new_row['veh_lon'] = float(vehicle_lon if vehicle_lon != -1 else -1)
	# 				new_row['veh_speed'] = float(vehicle_speed if vehicle_speed != -1 else -1)
	# 				new_row['veh_location_time'] = int(vehicle_location_time if vehicle_location_time != -1 else -1)
	# 				new_row['route_short_name'] = str(route_short_name)
	# 				new_row['route_dir'] = int(route_dir)
	# 				new_row['day'] = str(day)
	# 				new_row['run'] = str(run)
	# 				new_row['pattern_id'] = str(pattern_id)
	# 				new_row['stop_id'] = str(stop['stop_id']) # int?
	# 				new_row['seq'] = stop_seq
	# 				new_row['ETA'] = str((datetimeFromHMS(stop['departure_time']) + delay).strftime('%H:%M:%S'))
	# 				tables["TransitETA"] = tables["TransitETA"].append(pd.Series(new_row), ignore_index=True)
	# 				stop_seq += 1
	# 		i += 1
	
	# tables['TransitETA'].to_csv(pathname + 'TransitETA' + ".csv", sep = ',', index = False)
	# logging.debug("%s\n%s\n%s\n", 'TransitETA', "----------" * 8, tables['TransitETA'])

def datetimeFromHMS(timestamp):
	if int(timestamp[:2]) == 99:
		pass
		delay = timedelta(seconds=0)
	elif int(timestamp[:2]) >= 24:
		timestamp = str(int(timestamp[:2]) - 24) + timestamp[2:]
		delay = timedelta(days=1)
	else:
		delay = timedelta(seconds=0)
	o_time = time.strptime(timestamp, '%H:%M:%S')
	n_time = datetime.now().replace(hour=o_time.tm_hour, minute=o_time.tm_min, second=o_time.tm_sec, microsecond=0)
	return n_time + delay

def main(argv):
	logging.basicConfig(format='%(asctime)s %(levelname)s: %(message)s')

	agencies = []
	login={'host': 'localhost', 'user': '', 'passwd': '', 'db': ''}
	refresh = False
	local = False

	if '-a' in argv:
		agencies = transit_agencies.agency_dict.keys()
	if '-d' in argv:
		logging.getLogger().setLevel(logging.DEBUG)
	if '-db' in argv:
		db = argv[argv.index('-db') + 1]
		if db[0] != '-':
			login['db'] = db
	if '-h' in argv:
		host = argv[argv.index('-h') + 1]
		if host[0] != '-':
			login['host'] = host
	if '-i' in argv:
		logging.getLogger().setLevel(logging.INFO)
	if '-l' in argv:
		local = True
	if '-n' in argv:
		refresh = True
	if '-u' in argv:
		user = argv[argv.index('-u') + 1]
		if user[0] != '-':
			login['user'] = user

	for key, value in login.iteritems():
		if value == '':
			if key == 'passwd':
				login[key] = getpass.getpass()
			else:
				login[key] = raw_input("{0}: ".format(key))

	try:
		with MySQLdb.connect(host=login['host'], user=login['user'], passwd=login['passwd']) as con:
			con.execute('CREATE DATABASE IF NOT EXISTS {0}'.format(login['db']))
	except MySQLdb.Error, e:
		try:
			logging.debug('MySQL Error {0}: {1}'.format(e.args[0], e.args[1]))
		except IndexError:
			logging.debug('MySQL Error: {0}'.format(str(e)))
		sys.exit(0)

	# try:
	# 	con = MySQLdb.connect(host=login['host'], user=login['user'], passwd=login['passwd'])
	# 	cur = con.cursor()
	# 	cur.execute('CREATE DATABASE IF NOT EXISTS {0}'.format(login['db']))
	# except MySQLdb.Error, e:
	# 	try:
	# 		logging.debug('MySQL Error {0}: {1}'.format(e.args[0], e.args[1]))
	# 	except IndexError:
	# 		logging.debug('MySQL Error: {0}'.format(str(e)))
	# 	sys.exit(0)
	# finally:
	# 	if cur:
	# 		cur.close()
	# 	if con:
	# 		con.close()

	if not agencies:
		# check program conditions
		if len(argv) == 1:
			logging.error("Not enough arguments")
			return
		agency = argv[1]
		if not transit_agencies.isValidAgency(agency):
			logging.error("Need to provide a valid Transit Agency")
			return
		agencies = [agency]

	# access GTFS data
	for agency in agencies:
		logging.debug("Processing agency: {0}".format(agency))
		static_feed, checksum = gtfs_helper.get_static(agency, refresh)
		trip_update_feed = gtfs_helper.get_realtime(agency, mode="trip_update")
		alert_feed = gtfs_helper.get_realtime(agency, mode="alert")
		vehicle_position_feed = gtfs_helper.get_realtime(agency, mode="vehicle_position")

		# process GTFS data
		interpret(agency, static_feed, trip_update_feed, alert_feed, vehicle_position_feed, checksum, refresh, login, local)

if __name__ == "__main__":
	main(sys.argv)
	


	