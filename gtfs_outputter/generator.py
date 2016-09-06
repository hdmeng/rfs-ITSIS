#! /usr/bin/python

import code
import gtfsutility
import logging
import os
from os import path
import sys
import tableutility
import transit_agencies

GENERATOR_AGENCY_ERROR_STRING = 'Please provide a valid transit agency name'
GENERATOR_AGENCY_ROUTE_ERROR_STRING = 'Please provide a valid route name for the given agency'
GENERATOR_TABLE_ERROR_STRING = 'Please provide a valid task 1 - 3 table name'

def process_feeds(static_feed, trip_update_feed, alert_feed, 
                  vehicle_position_feed, agencyID, routeID, tables, is_local, 
                  should_refresh, agency):
    pathname = None

    if is_local:
        pathname = "./agencies/" + agency + "/processed/"
        if not path.exists(pathname):
            os.makedirs(pathname)

    tableUtility = tableutility.TableUtility(
        agencyID, routeID, static_feed, trip_update_feed, alert_feed, vehicle_position_feed, is_local, pathname, should_refresh)

    if 'agency' in tables:
        tableUtility.agency()
    if 'routes' in tables:
        tableUtility.routes()
    if 'stops' in tables:
        tableUtility.stops()
    if 'route_stop_seq' in tables:
        tableUtility.route_stop_seq()
    if 'run_pattern'in tables:
        tableUtility.run_pattern()
    if 'schedules' in tables:
        tableUtility.schedules()
    if 'route_point_seq' in tables:
        tableUtility.route_point_seq()
    if 'points' in tables:
        tableUtility.points()
    # if 'fare' in tables:
    #   tableUtility.fare()
    # if 'calendar_dates' in tables:
    #   tableUtility.calendar_dates()
    if 'transfers' in tables:
        tableUtility.transfers()
    if 'gps_fixes' in tables:
        tableUtility.gps_fixes()
    if 'transit_eta' in tables:
        tableUtility.transit_eta()

def main(argv):
    logging.basicConfig(format='%(asctime)s %(levelname)s: %(message)s')

    refresh = False
    local = False

    agency = None
    agencyID = None
    route = None
    routeID = None
    tables = ['agency', 'routes', 'stops', 'route_stop_seq', 'run_pattern', 
              'schedules', 'route_point_seq', 'points', 'fare', 
              'calendar_dates', 'transfers', 'gps_fixes', 'transit_eta']

    if len(argv) < 2:
        logging.error(GENERATOR_AGENCY_ERROR_STRING)
        return
    else:
        agency = argv[1]
        if not transit_agencies.is_valid_agency(agency):
            logging.error(GENERATOR_AGENCY_ERROR_STRING)
            return
        agencyID = transit_agencies.get(agency, 'id')

    if len(argv) > 2 and argv[2][0] != '-':
        route = argv[2]

    if '-d' in argv:
        logging.getLogger().setLevel(logging.DEBUG)
    if '-i' in argv:
        logging.getLogger().setLevel(logging.INFO)
    if '-l' in argv:
        local = True
    if '-r' in argv:
        refresh = True
    if '-t' in argv:
        buf = argv[argv.index('-t') + 1]
        if buf[0] != '-' and buf in tables:
            tables = [buf]
        else:
            logging.error(GENERATOR_TABLE_ERROR_STRING)
            return

    logging.debug('Processing agency: {0}'.format(agency))

    static_feed, checksum = gtfsutility.get_static(agency, refresh)

    if route is not None:
        if transit_agencies.is_valid_route(route, static_feed['routes']):
            routeID = transit_agencies.get_route_id(route, 
                                                    static_feed['routes'])
        else:
            logging.error(GENERATOR_AGENCY_ROUTE_ERROR_STRING)
            return

    trip_update_feed = gtfsutility.get_realtime(agency, mode='trip_update')
    print(trip_update_feed)
    exit()
    alert_feed = gtfsutility.get_realtime(agency, mode='alert')
    vehicle_position_feed = gtfsutility.get_realtime(agency, 
                                                     mode='vehicle_position')

    # logging.debug('Arguments: agency - {0}, routeID - {1}, tables - {2}, refresh - {3}, local - {4}'.format(
    #     agency, routeID, tables, refresh, local))
    process_feeds(static_feed, trip_update_feed, alert_feed, 
                  vehicle_position_feed, agencyID, routeID, tables, local, refresh, agency)
        
if __name__ == '__main__':
    main(sys.argv)
        