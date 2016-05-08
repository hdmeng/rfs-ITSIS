#! /usr/bin/python

agency_dict = {
	"bart": [0, 'http://www.bart.gov/sites/default/files/docs/google_transit_20160208_v1.zip',
		'http://api.bart.gov/gtfsrt/alerts.aspx',
		'http://api.bart.gov/gtfsrt/tripupdate.aspx',
		'http://api.transitime.org/api/v1/key/5ec0de94/agency/bart/command/gtfs-rt/vehiclePositions'], 
	"tri_delta": [11, 'http://70.232.147.132/rtt/public/utility/gtfs.aspx',
		'http://70.232.147.132/rtt/public/utility/gtfsrealtime.aspx/alert',
		'http://70.232.147.132/rtt/public/utility/gtfsrealtime.aspx/tripupdate'], 
	"vta": [10, 'http://www.vta.org/sfc/servlet.shepherd/document/download/069A0000001NUea', 
		'', 
		'http://api.transitime.org/api/v1/key/5ec0de94/agency/vta/command/gtfs-rt/tripUpdates', 
		'http://api.transitime.org/api/v1/key/5ec0de94/agency/vta/command/gtfs-rt/vehiclePositions'], 
	}

# BART: 1 Hz
# Tri Delta: 1/30 Hz

def get(agency, field):
	if field == "name":
		return agency
	elif field == "id":
		return agency_dict[agency][0]
	elif field == "static":
		return agency_dict[agency][1]
	elif field == "alert" and len(agency_dict[agency]) > 2:
		return agency_dict[agency][2]
	elif field == "trip_update" and len(agency_dict[agency]) > 3:
		return agency_dict[agency][3]
	elif field == "vehicle_position" and len(agency_dict[agency]) > 4:
		return agency_dict[agency][4]
	else:
		return None

def isValidAgency(agency):
	return agency in agency_dict.keys()


# AC Transit (AC)
# ACT
# County Connection
# Dumbarton Express
# FAST
# GGT
# Marin Transit (MT)
# Muni (SFM)
# Petaluma Transit (PT)
# Rio Vista Delta Breeze (RVDB)
# SamTrans (SMT)
# VTA (VTA)
# CityBus ()
# SolTrans ()
# SC Transit ()
# Tri Delta Transit ()
# UC Transit ()
# City Coach ()
# VINE (VINE)
# WestCAT (WCAT)
# WHEELS (WHE)
# ACE (ACE)
# BART (BART)
# Capitol Corridor (CAP)
# Caltrain (CAL)

