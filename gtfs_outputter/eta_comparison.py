from datetime import *
import requests
import pandas as pd
import sqlalchemy as sa
import sys
import time

import dataframeutility
import gtfsutility
import transit_agencies


TRANSITTIME_ENDPOINT = 'http://api.transitime.org/api/v1/key/5ec0de94/agency/{0}/command/{1}?{2}'
TRANSITTIME_AGENCY_NAMES = {'tri_delta': 'tridelta'}


def hms_to_protected_datetime(hms):
    """Converts a timestamp string into a datetime instance.

    An hours hand of '99' will be ignored, while any hours hand a general
    24-hour clock will convert the overflow into days.

    Args:
        hms: An HH:MM:SS formatted string.

    Returns:
        A datetime instance of the timestamp
    """
    delay = timedelta(seconds=0)
    if int(hms[:2]) == 99:
        pass
    elif int(hms[:2]) >= 24:
        hms = str(int(hms[:2]) - 24) + hms[2:]
        delay = timedelta(days=1)

    o_time = time.strptime(hms, '%H:%M:%S')
    n_time = (datetime.now() - timedelta(hours=7)).replace(hour=o_time.tm_hour, minute=o_time.tm_min, second=o_time.tm_sec, microsecond=0)
    return n_time + delay


def get_transittime_predictions(agency, route_short_name, stop_code, cache):
    filled_endpoint = TRANSITTIME_ENDPOINT.format(agency, 'predictions', 'rs={0}|{1}'.format(route_short_name, stop_code))

    if filled_endpoint not in cache:
        request = requests.get(filled_endpoint)
        if request.status_code != 200:
            cache[filled_endpoint] = None
        else:
            data = request.json()
            predictions = (data['preds'] if 'preds' in data.keys() else data)['predictions']
            cache[filled_endpoint] = predictions
    return cache[filled_endpoint]

def get_predictions_eta(predictions, route_dir, trip_id):
    for prediction in predictions:
        for destination in prediction['destinations']:
            if int(destination['dir']) == route_dir:
                for dest_prediction in destination['predictions']:
                    if dest_prediction['trip'] == trip_id:
                        return (datetime.fromtimestamp(dest_prediction['time']) - timedelta(hours=7)).strftime('%H:%M:%S')
    return None


def get_transittime_eta(agency, route_short_name, stop_code, route_dir, trip_id, cache):
    predictions = get_transittime_predictions(TRANSITTIME_AGENCY_NAMES[agency], route_short_name, stop_code, cache)
    transittime_eta = get_predictions_eta(predictions, route_dir, trip_id)
    return transittime_eta


def main(agency):
    # set up connection to the MySQL database

    datapath = {}
    pathname = None
    engine = None

    username = 'root'
    password = 'PATH452RFS'
    host = 'localhost'
    database = 'PATHTransit'

    engine = sa.create_engine('mysql://{0}:{1}@{2}/{3}'.format(username, password, host, database))

    try:
        conn = engine.connect()
    except sa.exc.OperationalError as e:
        error_code = e.orig.args[0]
        logging.error('unable to reach database' if error_code == 2002 else 'invalid credentials for database')
        sys.exit(1)
    datapath['conn'] = conn
    datapath['pathname'] = pathname
    datapath['engine'] = engine
    datapath['metadata'] = sa.MetaData()

    # read GTFS tables

    tables = {}
    table_names = ['Stops', 'RunPattern']
    for table_name in table_names:
        if dataframeutility.can_read_dataframe(table_name, datapath):
            tables[table_name] = dataframeutility.read_dataframe(table_name, datapath)
    datapath['conn'].close()
    for table_name in table_names:
        if tables[table_name] is None or tables[table_name].empty:
            sys.exit(1)

    stops = tables['Stops']
    run_pattern = tables['RunPattern']

    row_collector = []
    transittime_predictions = {}
    static_feed, _ = gtfsutility.get_static(agency, False)
    stop_times = static_feed['stop_times']
    trip_update_feed = gtfsutility.get_realtime(agency, mode='trip_update')
    for entity in trip_update_feed.entity:
        update = gtfsutility.TripUpdate(entity.trip_update)
        trip_descriptor = update.get_trip_descriptor()
        stop_time_updates = update.get_stop_time_updates()

        trip_id = str(trip_descriptor['trip_id'])

        run_pattern_entry = run_pattern.loc[run_pattern['trip_id'] == trip_id].iloc[0]
        route_short_name = str(run_pattern_entry['route_short_name'])
        route_dir = int(run_pattern_entry['route_dir'])

        trip_id_block = stop_times.loc[stop_times['trip_id'].apply(str) == trip_id]
        for i, stop_time_update in enumerate(stop_time_updates):
            if not ('departure' in stop_time_update and 'arrival' in stop_time_update):
                continue  # does this work? how many loops does it break
            stop_seq = stop_time_update['stop_sequence']
            stop_seq_til = (stop_time_updates[i + 1]['stop_sequence'] if i + 1 < len(stop_time_updates) else len(trip_id_block) + 1)

            time_diff = stop_time_update['departure' if 'departure' in stop_time_update else 'arrival']

            if 'delay' in time_diff:
                delay = timedelta(seconds=time_diff['delay'])
            else:
                delay_time = datetime.fromtimestamp(int(time_diff['time'])) - timedelta(hours=7)
                schedule_time = hms_to_protected_datetime(trip_id_block.iloc[stop_seq - 1]['departure_time'])
                delay = delay_time - schedule_time

            for stop_seq in range(stop_seq, stop_seq_til):
                stop_times_entry = trip_id_block.iloc[stop_seq - 1]
                stop_id = str(stop_times_entry['stop_id'])

                stops_entry = stops.loc[stops['stop_id'] == stop_id].iloc[0]
                stop_code = str(stops_entry['stop_code'])

                new_row = {}
                new_row['route_short_name'] = route_short_name
                new_row['route_dir'] = route_dir
                new_row['stop_id'] = stop_id
                new_row['seq'] = stop_seq
                new_row['agency ETA'] = (hms_to_protected_datetime(stop_times_entry['departure_time']) + delay).strftime('%H:%M:%S')
                new_row['transittime ETA'] = get_transittime_eta(agency, route_short_name, stop_code, route_dir, trip_id, transittime_predictions)
                new_row['STA'] = hms_to_protected_datetime(stop_times_entry['departure_time']).strftime('%H:%M:%S')
                row_collector.append(new_row)

    transit_eta_comparison = pd.DataFrame(row_collector)
    transit_eta_comparison.to_csv('{0}_eta_comparison.csv'.format(agency), index=False, columns=['route_short_name', 'route_dir', 'stop_id', 'seq', 'agency ETA', 'transittime ETA', 'STA'])

if __name__ == '__main__':
    if len(sys.argv) < 1:
        print('please provide an agency')
        sys.exit(1)
    agency = sys.argv[1]
    main(agency)
