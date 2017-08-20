import logging

# Check 511's api documentation, to get the GTFS files for a specific agence
# you must have
# 1. an api key
# 2. an operator ID
# The current api key is : b5cb0334-749b-40ee-bcfb-98338d3ec5fc,
# but you may request your own
# The operator id is from another api call:
# http://api.511.org/transit/gtfsoperators?api_key=b5cb0334-749b-40ee-bcfb-98338d3ec5fc

# TODO(erchpito) 511 and transittime data sets are not compatible

# TODO(erchpito) seems like BART isn't providing any live vehiclePosition data
agency_dict = {
    'bart': [8,
             'http://www.bart.gov/dev/schedules/google_transit.zip',
             'http://api.bart.gov/gtfsrt/alerts.aspx',
             'http://api.bart.gov/gtfsrt/tripupdate.aspx',
             None],
    'tri_delta': [11,
                  'http://70.232.147.132/rtt/public/utility/gtfs.aspx',
                  'http://70.232.147.132/rtt/public/utility/gtfsrealtime.aspx/alert',
                  'http://70.232.147.132/rtt/public/utility/gtfsrealtime.aspx/tripupdate',
                  'http://api.transitime.org/api/v1/key/5ec0de94/agency/tridelta/command/gtfs-rt/vehiclePositions'],
    'vta': [10,
            'http://api.511.org/transit/datafeeds?api_key=b5cb0334-749b-40ee-bcfb-98338d3ec5fc&operator_id=SC',
            None,
            'http://api.transitime.org/api/v1/key/5ec0de94/agency/vta/command/gtfs-rt/tripUpdates',
            'http://api.transitime.org/api/v1/key/5ec0de94/agency/vta/command/gtfs-rt/vehiclePositions'],
}

# BART: 1 Hz
# Tri Delta: 1/30 Hz


def get(agency, field):
    if field == 'name':
        return agency
    elif field == 'id':
        return agency_dict[agency][0]
    elif field == 'static':
        return agency_dict[agency][1]
    elif field == 'alert':
        return agency_dict[agency][2]
    elif field == 'trip_update':
        return agency_dict[agency][3]
    elif field == 'vehicle_position':
        return agency_dict[agency][4]
    else:
        return None


def is_valid_agency(agency):
    return agency in agency_dict.keys()


def is_valid_route(route, route_table):
    if route in route_table.route_id.apply(str).values:
        return True
    # TODO(erchpito) why is the apply(str) needed to suppress the warning
    elif route in route_table.route_short_name.values:
        return True
    elif route in route_table.route_long_name.values:
        return True
    else:
        return False


def get_route_id(route, route_table):
    if route in route_table.route_id.apply(str).values:
        return route
    # TODO(erchpito) add case-insensitive comparison, look into casefolding
    elif route in route_table.route_short_name.values:
        return (
            route_table.loc[route_table.route_short_name ==
                            route].route_id.iloc[0]
        )
    elif route in route_table.route_long_name.values:
        return (
            route_table.loc[route_table.route_long_name ==
                            route].route_id.iloc[0]
        )
    else:
        return None
