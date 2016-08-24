#For Tridelta, the current link is: http://api.511.org/transit/datafeeds?api_key=b5cb0334-749b-40ee-bcfb-98338d3ec5fc&operator_id=3D
#Check 511's api documentation, to get the GTFS files for a specific agence you must have
#1. an api key
#2. an operator ID
#The current api key is : b5cb0334-749b-40ee-bcfb-98338d3ec5fc, but you may request your own
#The operator id is from another api call:  http://api.511.org/transit/gtfsoperators?api_key=b5cb0334-749b-40ee-bcfb-98338d3ec5fc
agency_dict = {
#	"bart": [8, 'http://www.bart.gov/sites/default/files/docs/google_transit_20160208_v1.zip',
	"bart": [8, 'http://api.511.org/transit/datafeeds?api_key=b5cb0334-749b-40ee-bcfb-98338d3ec5fc&operator_id=BA',
		'http://api.bart.gov/gtfsrt/alerts.aspx',
		'http://api.bart.gov/gtfsrt/tripupdate.aspx',
		'http://api.transitime.org/api/v1/key/5ec0de94/agency/bart/command/gtfs-rt/vehiclePositions'],
	"tri_delta": [11, 'http://api.511.org/transit/datafeeds?api_key=b5cb0334-749b-40ee-bcfb-98338d3ec5fc&operator_id=3D',
		'http://70.232.147.132/rtt/public/utility/gtfsrealtime.aspx/alert',
		'http://70.232.147.132/rtt/public/utility/gtfsrealtime.aspx/tripupdate'],
	# "vta": [10, 'http://www.vta.org/sfc/servlet.shepherd/document/download/069A0000001NUea',
	"vta": [10, 'http://api.511.org/transit/datafeeds?api_key=b5cb0334-749b-40ee-bcfb-98338d3ec5fc&operator_id=SC',
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