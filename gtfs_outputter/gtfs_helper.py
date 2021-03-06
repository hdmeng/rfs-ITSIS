#! /usr/bin/python

from datetime import date, timedelta, datetime
from google.transit import gtfs_realtime_pb2
from os import path
from StringIO import StringIO

import hashlib
import logging
import os
import requests
import urllib
import warnings
import zipfile

import df_helper
import transit_agencies

REQUIRED_GTFS_FILES = ["agency", "stops", "routes", "trips", "stop_times", "calendar"]
OPTONAL_GTFS_FILES = ["calendar_dates", "fare_attributes", "fare_rules", "shapes", "frequencies", "transfers", "feed_info"]

class TripUpdate(object):
	def __init__(self, trip_update):
		self.td = trip_update.trip
		self.stus = trip_update.stop_time_update

		self.td_fv = self.process_trip_descriptor()
		self.stus_fv = self.process_stop_time_updates()

	def get_trip_descriptor(self):
		return self.td_fv

	def get_stop_time_updates(self):
		return self.stus_fv

	def process_trip_descriptor(self):
		fields = ["trip_id", "route_id", "start_time", "start_date", "schedule_relationship"]
		td_fv = {}
		for f in fields:
			if self.td.HasField(f):
				td_fv[f] = getattr(self.td, f)
				if isinstance(td_fv[f], unicode):
					td_fv[f] = str(td_fv[f])
				if type(td_fv[f]) == str and td_fv[f].isdigit():
					td_fv[f] = int(td_fv[f])
		if "schedule_relationship" not in td_fv.viewkeys():
			td_fv["schedule_relationship"] = gtfs_realtime_pb2.TripDescriptor.SCHEDULED

		logging.debug(td_fv)

		return td_fv

	def process_stop_time_updates(self):
		stus_fv = []

		for stu in self.stus:
			stus_fv += [self.process_stop_time_update(stu)]

		# logging.debug(stus_fv)

		return stus_fv

	def process_stop_time_update(self, stu):
		fields = ["stop_sequence", "stop_id", "arrival", "departure", "schedule_relationship"]
		stu_fv = {}
		for f in fields:
			if (f == "arrival" or f == "departure") and stu.HasField(f):
				stu_fv[f] = self.process_stop_time_event(getattr(stu, f))
			elif stu.HasField(f):
				stu_fv[f] = getattr(stu, f)
				if isinstance(stu_fv[f], unicode):
					stu_fv[f] = str(stu_fv[f])
		if "schedule_relationship" not in stu_fv.viewkeys():
			stu_fv["schedule_relationship"] = gtfs_realtime_pb2.TripUpdate.StopTimeUpdate.SCHEDULED

		logging.debug(stu_fv)

		return stu_fv

	def process_stop_time_event(self, ste):
		fields = ["delay", "time", "uncertainty"]
		ste_fv = {}
		for f in fields:
			if ste.HasField(f):
				ste_fv[f] = getattr(ste, f)

		return ste_fv

class VehiclePosition(object):
	def __init__(self, vehicle_position):
		self.trip = vehicle_position.trip
		self.position = vehicle_position.position
		self.vehicle = vehicle_position.vehicle

		fields = ["current_stop_sequence", "stop_id", "current_status", "timestamp", "congestion_level"]
		self.update_dict = {}
		for f in fields:
			if vehicle_position.HasField(f):
				update_dict[f] = getattr(vehicle_position, f)
				if isinstance(update_dict[f], unicode):
					update_dict[f] = str(update_dict[f])

		logging.debug(update_dict)

		self.trip_dict = self.process_trip_descriptor()
		self.position_dict = self.process_position()
		self.vehicle_dict = self.process_vehicle_descriptor()

	def get_trip_descriptor(self):
		return self.trip_dict

	def get_position(self):
		return self.position_dict

	def get_vehicle_descriptor(self):
		return self.vehicle_dict

	def get_update_fields(self):
		return self.update_dict

	def process_trip_descriptor(self):
		fields = ["trip_id", "route_id", "start_time", "start_date", "schedule_relationship"]
		trip_dict = {}
		for f in fields:
			if self.trip.HasField(f):
				trip_dict[f] = getattr(self.trip, f)
				if isinstance(trip_dict[f], unicode):
					trip_dict[f] = str(trip_dict[f])
				if type(trip_dict[f]) == str and trip_dict[f].isdigit():
					trip_dict[f] = int(trip_dict[f])
		if "schedule_relationship" not in trip_dict.viewkeys():
			trip_dict["schedule_relationship"] = gtfs_realtime_pb2.TripDescriptor.SCHEDULED

		logging.debug(trip_dict)

		return trip_dict

	def process_position(self):
		fields = ['latitude', 'longitude', 'bearing', 'odometer', 'speed']
		position_dict = {}
		for f in fields:
			if self.position.HasField(f):
				position_dict[f] = getattr(self.position, f)
				if isinstance(position_dict[f], unicode):
					position_dict[f] = str(position_dict[f])
				if type(position_dict[f]) == str and position_dict[f].isdigit():
					position_dict[f] = int(position_dict[f])

		logging.debug(position_dict)

		return position_dict

	def process_vehicle_descriptor(self):
		fields = ['id', 'label', 'license_plate']
		vehicle_dict = {}
		for f in fields:
			if self.vehicle.HasField(f):
				vehicle_dict[f] = getattr(self.vehicle, f)
				if isinstance(vehicle_dict[f], unicode):
					vehicle_dict[f] = str(vehicle_dict[f])
				if type(vehicle_dict[f]) == str and vehicle_dict[f].isdigit():
					vehicle_dict[f] = float(vehicle_dict[f])

		logging.debug(vehicle_dict)

		return vehicle_dict

# apparently hashlib.md5() is broken or deprecated, an alternative is hashlib.sha256()
def hasher(file, checksum, blocksize=65536):
	buf = file.read(blocksize)
	while len(buf) > 0:
		checksum.update(buf)
		buf = file.read(blocksize)
	output = checksum.digest()
	logging.debug("Checksum: {0}".format(output))
	return output

def get_realtime(agency, mode):
	URL = transit_agencies.get(agency, mode)
	if URL == None:
		return None
	response = urllib.urlopen(URL)
	feed = gtfs_realtime_pb2.FeedMessage()
	feed.ParseFromString(response.read())
	return feed

def get_static(agency, refresh):
	pathname = "./agencies/" + agency + "/"
	feed = {}
	get_new_feed = False

	if path.exists(pathname + "raw_csv/") and not refresh:

		with open(pathname + "gtfs.zip", "r") as zipout:
			checksum = hasher(zipout, hashlib.md5()) # did not work

		# read csv files
		for f in os.listdir(pathname + "raw_csv/"):
			if f[-4:] == ".csv":
				with open(pathname + "raw_csv/" + f, 'rb') as csvfile:
					feed[f[:-4]] = df_helper.csv2df(csvfile)

		# if required GTFS data is missing, request new data anyway
		for f in REQUIRED_GTFS_FILES:
			if f not in feed:
				get_new_feed = True

		# if feed_end_date has passed, request new data anyway
		if not get_new_feed:
			feed_end_date = feed["feed_info"].feed_end_date[0] if "feed_info" in feed else feed["calendar"].end_date[0]
			feed_end_date = datetime.strptime(str(feed_end_date),'%Y%m%d')
			current_date = datetime.now()
			logging.info("feed_end_date = %s", feed_end_date.strftime("%Y%m%d"))
			logging.info("current_date = %s", current_date.strftime("%Y%m%d"))
			get_new_feed = feed_end_date < current_date

		if not get_new_feed:
			logging.debug("Read from local")
			return feed, checksum

	# request GTFS-Static
	request = requests.get(transit_agencies.get(agency, "static"), stream = True)

	# if unsuccessful
	if request.status_code != 200:
		return None

	if not path.exists(pathname + "raw/"):
		os.makedirs(pathname + "raw/")

	# unzip GTFS static
	buf = request.raw.read()
	zipdata = StringIO()
	zipdata.write(buf)

	with open(pathname + "gtfs.zip", "w") as zipout:
		zipout.write(buf)
	with open(pathname + "gtfs.zip", "r") as zipout:
		checksum = hasher(zipout, hashlib.md5())

	z = zipfile.ZipFile(zipdata)
	z.extractall(pathname + "raw/")
	
	# format static feed
	for f in z.namelist():
		with z.open(f) as csvfile:
			feed[f[:-4]] = df_helper.csv2df(csvfile).rename(columns = lambda s: str(s.decode('ascii', 'ignore')))
	for f in REQUIRED_GTFS_FILES:
		if f not in feed:
			logging.error("Incomplete GTFS dataset")
			return None

	z.close()

	# write csv files
	if not path.exists(pathname + "raw_csv/"):
		os.makedirs(pathname + "raw_csv/")
	for fn, df in feed.iteritems():
		df.to_csv(pathname + "raw_csv/" + fn + ".csv", sep = ',', index = False)

	logging.debug("Read from online")
	return feed, checksum
	


	