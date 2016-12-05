#! /usr/bin/python

import code
import getpass
import logging
import os
from os import path
from slacker import Slacker
import sqlalchemy as sa
import sys

import gtfsutility
import tableutility
import transit_agencies

GENERATOR_AGENCY_ERROR_STRING = 'Please provide a valid transit agency name'
GENERATOR_AGENCY_ROUTE_ERROR_STRING = ('Please provide a valid route name for '
                                       'the given agency')
GENERATOR_TABLE_ERROR_STRING = 'Please provide a valid task 1 - 3 table name'


def process_feeds(static_feed, checksum, trip_update_feed, alert_feed,
                  vehicle_position_feed, agencyID, routeID, tables, is_local,
                  should_refresh, default_login, agency):
    datapath = {}
    pathname = None
    engine = None
    if is_local:
        pathname = "./agencies/" + agency + "/processed/"
        if not path.exists(pathname):
            os.makedirs(pathname)
    else:
        username = 'root'
        password = 'PATH452RFS'
        host = 'localhost'
        database = 'PATHTransit'

        # username = 'root'
        # password = 'PATH452RFS'
        # host = 'http://52.53.208.65'
        # database = 'TrafficTransit'

        if not default_login:
            username = raw_input('Enter username: ')
            password = getpass.getpass()
            host = raw_input('Enter host: ')
            database = raw_input('Enter database: ')

        engine = sa.create_engine('mysql://{0}:{1}@{2}/{3}'.format(
            username, password, host, database))

        crashpath = './crashed/'
        if not path.exists(crashpath):
            os.makedirs(crashpath)
        if not path.exists(crashpath + 'crashfile.txt'):
            with open(crashpath + 'crashfile.txt', 'w+') as crashfile:
                crashfile.write('False')

        try:
            conn = engine.connect()
        except sa.exc.OperationalError as e:
            error_code = e.orig.args[0]
            if error_code == 2002:
                logging.error('unable to reach database')

                with open(crashpath + 'crashfile.txt', 'r+') as crashfile:
                    state = crashfile.read()
                    if state == 'False':
                        slack = Slacker(os.environ.get('SQUEAKQL_BOT_TOKEN'))
                        slack.chat.post_message('#general', '<!channel> MySQL database has crashed', as_user=True)
                        crashfile.seek(0)
                        crashfile.write('True')
                        crashfile.truncate()
            else:
                logging.error('invalid credentials for database')
            sys.exit(1)
        datapath['conn'] = conn
        with open(crashpath + 'crashfile.txt', 'w+') as crashfile:
            crashfile.write('False')

    datapath['pathname'] = pathname
    datapath['engine'] = engine
    datapath['metadata'] = sa.MetaData()

    tableUtility = tableutility.TableUtility(
        agencyID, routeID, static_feed, checksum, trip_update_feed, alert_feed,
        vehicle_position_feed, datapath, should_refresh)

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
    if 'points' in tables:
        tableUtility.points()
    if 'route_point_seq' in tables:
        tableUtility.route_point_seq()
    # if 'fare' in tables:
    #   tableUtility.fare()
    # if 'calendar_dates' in tables:
    #   tableUtility.calendar_dates()
    # if 'transfers' in tables:
    #     tableUtility.transfers()
    if 'gps_fixes' in tables:
        tableUtility.gps_fixes()
    if 'transit_eta' in tables:
        tableUtility.transit_eta()
    if 'transit_eta_bart' in tables:
        tableUtility.transit_eta_bart()
    if 'transit_eta_tri_delta' in tables:
        tableUtility.transit_eta_tri_delta()
    if 'conn' in datapath:
        datapath['conn'].close()


def main(argv):
    logging.basicConfig(format=('%(asctime)s [%(levelname)s] '
                                '(%(threadName)-9s): %(message)s'))

    refresh = False
    local = False
    default_login = True

    agency = None
    agencyID = None
    route = None
    routeID = None
    tables = ['agency', 'routes', 'stops', 'route_stop_seq', 'run_pattern',
              'schedules', 'route_point_seq', 'points', 'fare',
              'calendar_dates', 'transfers', 'gps_fixes', 'transit_eta']
    experiment_tables = ['transit_eta_bart', 'transit_eta_tri_delta']

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
    if '-ml' in argv:
        default_login = False
    if '-r' in argv:
        refresh = True
    if '-t' in argv:
        buf = argv[argv.index('-t') + 1]
        if buf[0] != '-' and (buf in tables or buf in experiment_tables):
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
    alert_feed = gtfsutility.get_realtime(agency, mode='alert')
    vehicle_position_feed = gtfsutility.get_realtime(agency,
                                                     mode='vehicle_position')

    process_feeds(static_feed, checksum, trip_update_feed, alert_feed,
                  vehicle_position_feed, agencyID, routeID, tables, local,
                  refresh, default_login, agency)

if __name__ == '__main__':
    main(sys.argv)
