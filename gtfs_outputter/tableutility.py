import csv
import dataframeutility
from datetime import datetime
import googlemaps
import gtfsutility
import logging
import MySQLdb
import numpy as np
import os
from os import path
import pandas as pd
import pytz
import sys
import time

class TableUtility:

    SQL_USER = 'root'
    SQL_PWD = 'PATH452RFS'

    LOGIN = {'host': 'localhost', 
             'user': SQL_USER, 
             'passwd': SQL_PWD, 
             'db': 'PATHTransit'}
    # LOGIN = {'host': 'http://52.53.208.65', 
    #          'user': SQL_USER, 
    #          'passwd': SQL_PWD, 
    #          'db': 'TrafficTransit'}

    tables = {}
    trip2pattern = {}
    trip2vehicle = {}
    shape2pattern = {}

    def __init__(self, agencyID, routeID, static_feed, trip_update_feed, 
                 alert_feed, vehicle_position_feed, is_local, pathname, 
                 should_refresh):
        self.agencyID = agencyID
        self.routeID = routeID
        self.static_feed = static_feed
        self.trip_update_feed = trip_update_feed
        self.alert_feed = alert_feed
        self.vehicle_position_feed = vehicle_position_feed
        self.is_local = is_local
        self.pathname = pathname
        self.should_refresh = should_refresh

    # MARK: HELPER FUNCTIONS

    @staticmethod
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
        n_time = datetime.now().replace(hour=o_time.tm_hour, 
                                        minute=o_time.tm_min, 
                                        second=o_time.tm_sec, 
                                        microsecond=0)
        return n_time + delay

    def generate_table(self, table_name, table, setup_row_func, rows=None, 
                       entities=None):
        if dataframeutility.can_read_dataframe(table_name, self.LOGIN, self.is_local, self.pathname) and not self.should_refresh:
            self.tables[table_name] = dataframeutility.read_dataframe(table_name, self.LOGIN, self.is_local, self.pathname)
        else:
            self.tables[table_name] = table

            if rows is not None:
                for i, row in rows.iterrows():
                    setup_row_func(i, row)
            elif entities is not None:
                for entity in entities:
                    setup_row_func(entity)

            dataframeutility.write_dataframe(self.tables[table_name], table_name, self.LOGIN, self.is_local, self.pathname, self.should_refresh)
        logging.debug('SUCCESS finished with {0}\n'.format(table_name))

    # MARK: TASK 1

    def agency(self):
        table_name = 'Agency'
        columns = ['agency_id', 'agency_name', 'agency_url', 'agency_timezone', 
                   'agency_lang', 'agency_phone', 'timezone_name']
        table = pd.DataFrame(index=np.r_[0:len(self.static_feed['agency'].index)], columns=columns)

        def agency_row_func(i, row):
            new_row = self.tables[table_name].loc[i]
            new_row['agency_id'] = self.agencyID
            new_row['agency_name'] = row['agency_name']
            new_row['agency_url'] = row['agency_url']
            timezone = pytz.timezone(row['agency_timezone'])
            # TODO(erchpito) figure out a way to get the offset without regard to DST
            # timezone = datetime.now(timezone).strftime('%z')
            new_row['agency_timezone'] = int(timezone)
            new_row['agency_lang'] = dataframeutility.optional_field(i, 'agency_lang', self.static_feed['agency'])
            new_row['agency_phone'] = dataframeutility.optional_field(i, 'agency_phone', self.static_feed['agency'])
            new_row['timezone_name'] = row['agency_timezone']

        self.generate_table(table_name, table, agency_row_func, rows=self.static_feed['agency'])

    def routes(self):
        table_name = 'Routes'
        columns = ['agency_id', 'route_short_name', 'route_dir', 'route_type', 
                   'route_long_name', 'route_desc', 'route_url', 'route_color', 
                   'route_text_color', 'route_id', 'version']
        table = pd.DataFrame()

        def routes_row_func(i, row):
            for direction_id in self.static_feed['trips'].loc[self.static_feed['trips']['route_id'] == row['route_id']]['direction_id'].unique():
                new_row = {}
                new_row['agency_id'] = self.agencyID
                new_row['route_short_name'] = str(dataframeutility.optional_field(i, 'route_short_name', self.static_feed['routes'], self.static_feed['routes'].iloc[i]['route_long_name']))
                new_row['route_dir'] = direction_id
                new_row['route_type'] = int(row['route_type'])
                new_row['route_long_name'] = str(dataframeutility.optional_field(i, 'route_long_name', self.static_feed['routes'], self.static_feed['routes'].iloc[i]['route_short_name']))
                new_row['route_desc'] = dataframeutility.optional_field(i, 'route_desc', self.static_feed['routes'])
                new_row['route_url'] = dataframeutility.optional_field(i, 'route_url', self.static_feed['routes'])
                new_row['route_color'] = dataframeutility.optional_field(i, 'route_color', self.static_feed['routes'], default='FFFFFF').upper()
                new_row['route_text_color'] = dataframeutility.optional_field(i, 'route_text_color', self.static_feed['routes'], default='000000').upper()
                new_row['route_id'] = str(row['route_id'])
                # TODO(erchpito) should be checksum
                new_row['version'] = 1
                self.tables[table_name] = self.tables[table_name].append(pd.Series(new_row), ignore_index=True)

        self.generate_table(table_name, table, routes_row_func, rows=self.static_feed['routes'])

    def stops(self):
        table_name = 'Stops'
        columns = ['agency_id', 'stop_id', 'stop_code', 'stop_name', 
                   'stop_desc', 'stop_lat', 'stop_lon', 'lat_lon', 'stop_url', 
                   'location_type', 'parent_station', 'wheelchair_boarding', 
                   'version']
        table = pd.DataFrame(index=np.r_[0:len(self.static_feed['stops'].index)], columns=columns)

        def stops_row_func(i, row):
            new_row = self.tables[table_name].loc[i]
            new_row['agency_id'] = self.agencyID
            new_row['stop_id'] = str(row['stop_id'])
            new_row['stop_code'] = str(dataframeutility.optional_field(i, 'stop_code', self.static_feed['stops']))
            new_row['stop_name'] = str(row['stop_name'])
            new_row['stop_desc'] = str(dataframeutility.optional_field(i, 'stop_desc', self.static_feed['stops']))
            new_row['stop_lat'] = float(row['stop_lat'])
            new_row['stop_lon'] = float(row['stop_lon'])
            new_row['lat_lon'] = 0 # some calculations, ignore until using MySQL
            new_row['stop_url'] = str(dataframeutility.optional_field(i, 'stop_url', self.static_feed['stops']))
            new_row['location_type'] = int(dataframeutility.optional_field(i, 'location_type', self.static_feed['stops'], 0))
            new_row['parent_station'] = int(dataframeutility.optional_field(i, 'parent_station', self.static_feed['stops'], 0))
            new_row['wheelchair_boarding'] = int(dataframeutility.optional_field(i, 'wheelchair_boarding', self.static_feed['stops'], 0))
            # TODO(erchpito) should be checksum
            new_row['version'] = 1

        self.generate_table(table_name, table, stops_row_func, rows=self.static_feed['stops'])

    # TODO(erchpito) routes are separated by direction
    # i.e. DALY/FREMONT and FREMONT/DALY are two different routes
    # and thus their pattern ids will be different too
    # TODO(erchpito) the 511 dataset does not include shape_id in trips
    # thus there's no way to link shapes to patterns
    def route_stop_seq(self):
        table_name = 'Route_stop_seq_2'
        columns = ['agency_id', 'route_short_name', 'route_dir', 'pattern_id', 
                   'stop_id', 'seq', 'is_time_point', 'version']
        table = pd.DataFrame()

        if self.routeID is not None:
            route_rows = self.static_feed['routes'].loc[self.static_feed['routes'].route_id == self.routeID]
        else:
            route_rows = self.static_feed['routes']

        def route_stop_seq_row_func(i, row):
            route_id = row['route_id']
            patterns = []
            for _, subrow in self.static_feed['trips'].loc[self.static_feed['trips']['route_id'] == route_id].iterrows():
                trip_id = subrow['trip_id']
                direction_id = subrow['direction_id'] if 'direction_id' in subrow else 0
                trip_id_block = self.static_feed['stop_times'].loc[self.static_feed['stop_times']['trip_id'] == trip_id]
                sequence = trip_id_block['stop_id'].tolist()
                sequence = trip_id_block['stop_id'].tolist().append(subrow['shape_id'])
                if str(sequence) not in patterns:
                    patterns += [str(sequence)]
                pattern_num = patterns.index(str(sequence)) + 1
                route_short_name = str(dataframeutility.optional_field(i, 'route_short_name', self.static_feed['routes'], self.static_feed['routes'].iloc[i]['route_long_name']))
                pattern_id = "{0}_{1}_{2}".format(route_short_name, direction_id, pattern_num)
                for k, subsubrow in trip_id_block.iterrows():
                    new_row = {}
                    new_row['agency_id'] = self.agencyID
                    new_row['route_short_name'] = route_short_name
                    new_row['route_dir'] = direction_id
                    new_row['pattern_id'] = pattern_id
                    new_row['stop_id'] = str(subsubrow['stop_id'])
                    new_row['seq'] = subsubrow['stop_sequence']
                    new_row['is_time_point'] = int(dataframeutility.optional_field(k, 'timepoint', self.static_feed['stop_times'], 0))
                    # TODO(erchpito) should be checksum
                    new_row['version'] = 1
                    self.tables[table_name] = self.tables[table_name].append(pd.Series(new_row), ignore_index=True)
                self.trip2pattern[trip_id] = pattern_id
                # TODO(erchpito) is this really the case?
                self.shape2pattern[subrow['shape_id']] = pattern_id

        self.generate_table(table_name, table, route_stop_seq_row_func, rows=route_rows)

        # TODO(erchpito) figure out how to do this if it were on the database
        if not self.trip2pattern:
            with open(self.pathname + 'Trip2Pattern_2.csv', 'rb') as f:
                reader = csv.reader(f)
                trip2pattern = dict(reader)
        else:
            with open(self.pathname + 'Trip2Pattern_2.csv', 'wb') as f:
                writer = csv.writer(f)
                for key, value in self.trip2pattern.items():
                    writer.writerow([key, value])

    # def route_stop_seq(self):
    #     count = 0
    #     columns = ['agency_id', 'route_short_name', 'route_dir',
    #                'pattern_id', 'stop_id', 'seq', 'is_time_point', 'version', 'trip_id']

    #     self.tables['Route_stop_seq'] = pd.DataFrame()

    #     if self.routeID is not None:
    #         route_rows = self.static_feed['routes'].loc[self.static_feed['routes'].route_id == self.routeID]
    #     else:
    #         route_rows = self.static_feed['routes']

    #     for i, row in route_rows.iterrows():
    #         route_id = row['route_id']
    #         patterns = []
    #         for _, subrow in self.static_feed['trips'].loc[self.static_feed['trips']['route_id'] == route_id].iterrows():
    #             trip_id = subrow['trip_id']
    #             direction_id = subrow['direction_id'] if 'direction_id' in subrow else 0 #get the direction id in the trip
    #             trip_id_block = self.static_feed['stop_times'].loc[self.static_feed['stop_times']['trip_id'] == trip_id]
    #             sequence = trip_id_block['stop_id'].tolist()
    #             if str(sequence) not in patterns:
    #                 patterns += [str(sequence)]
    #                 pattern_num = patterns.index(str(sequence)) + 1
    #                 route_short_name = str(dataframeutility.optional_field(i, 'route_long_name', self.static_feed['routes']))
    #                 pattern_id = "{0}_{1}_{2}".format(route_short_name, direction_id, pattern_num)
    #                 for k, subsubrow in trip_id_block.iterrows():
    #                     new_row = {}
    #                     new_row['trip_id'] = trip_id
    #                     new_row['agency_id'] = self.agencyID
    #                     new_row['route_short_name'] = route_short_name
    #                     new_row['route_dir'] = direction_id
    #                     new_row['pattern_id'] = pattern_id
    #                     new_row['stop_id'] = str(subsubrow['stop_id'])
    #                     new_row['seq'] = subsubrow['stop_sequence']
    #                     new_row['is_time_point'] = int(dataframeutility.optional_field(k, 'timepoint', self.static_feed['stop_times'], 0))
    #                     new_row['version'] = 1; #replace later
    #                     self.tables["Route_stop_seq"] = self.tables["Route_stop_seq"].append(pd.Series(new_row), ignore_index=True)
    #                     count += 1
    #             self.trip2pattern[trip_id] = pattern_id
    #         if self.routeID != 'all':
    #             break

    #     with open('Trip2Pattern.csv', 'wb') as f:
    #         writer = csv.writer(f)
    #         writer.writerow(["trip_id", "pattern_id"])
    #         for key, value in self.trip2pattern.items():
    #             writer.writerow([key, value])
    #     table_name = 'Route_stop_seq'
    #     dataframeutility.write_dataframe(self.tables[table_name], table_name, self.LOGIN, self.is_local, self.pathname, self.should_refresh)
    #     logging.debug('SUCCESS finished with {0}\n'.format(table_name))

    def run_pattern(self):
        table_name = 'RunPattern'
        columns = ['agency_id', 'route_short_name', 'start_date', 'end_date', 
                   'service_id', 'day', 'route_dir', 'run', 'pattern_id', 
                   'trip_headsign', 'trip_id', 'version']
        table = pd.DataFrame(index=np.r_[0:len(self.static_feed['trips'].index)], columns=columns)

        if self.routeID is not None:
            trip_rows = self.static_feed['trips'].loc[self.static_feed['trips'].route_id == self.routeID]
        else:
            trip_rows = self.static_feed['trips']

        run_count = {}
        def runPattern_row_func(i, row):
            new_row = tables[table_name].loc[i]
            new_row['agency_id'] = self.agencyID
            j = np.where(self.static_feed['routes']['route_id'] == row['route_id'])[0][0]
            new_row['route_short_name'] = str(dataframeutility.optional_field(j, 'route_short_name', self.static_feed['routes'], self.static_feed['routes'].iloc[j]['route_long_name']))
            new_row['service_id'] = row['service_id']
            calendar = self.static_feed['calendar'].loc[self.static_feed['calendar']['service_id'] == row['service_id']].iloc[0]
            new_row['start_date'] = datetime.strptime(str(calendar['start_date']), "%Y%m%d")
            new_row['end_date'] = datetime.strptime(str(calendar['end_date']), "%Y%m%d")
            new_row['day'] = "{0}{1}{2}{3}{4}{5}{6}".format(calendar['monday'], calendar['tuesday'], calendar['wednesday'], calendar['thursday'], calendar['friday'], calendar['saturday'], calendar['sunday'])
            new_row['route_dir'] = int(dataframeutility.optional_field(i, 'direction_id', self.static_feed['trips'], 0))
            # TODO(erchpito) you cannot guarantee that route_short_name will be unique for each route
            run_key = '{0}_{1}_{2}'.format(new_row['route_short_name'], new_row['route_dir'], new_row['day'])
            if run_key not in run_count:
                run_count[run_key] = 1
            new_row['run'] = run_count[run_key]
            run_count[run_key] += 1
            new_row['pattern_id'] = self.trip2pattern[row['trip_id']]
            new_row['trip_headsign'] = dataframeutility.optional_field(i, 'trip_headsign', self.static_feed['trips'], self.static_feed['stop_times'].loc[self.static_feed['stop_times']['trip_id'] == row['trip_id']]['stop_headsign'].iloc[0])
            new_row['trip_id'] = str(row['trip_id'])
            # TODO(erchpito) should be checksum
            new_row['version'] = 1

        if not self.trip2pattern:
            with open(self.pathname + 'Trip2Pattern_2.csv', 'rb') as f:
                reader = csv.reader(f)
                trip2pattern = dict(reader)

        self.generate_table(table_name, table, runPattern_row_func, rows=trip_rows)

    def schedules(self):
        table_name = 'Schedules'
        columns = ['agency_id', 'route_short_name', 'start_date', 'end_date', 
                   'day', 'route_dir', 'run', 'pattern_id', 'seq', 'stop_id', 
                   'is_time_point', 'pickup_type', 'dropoff_type', 
                   'arrival_time', 'departure_time', 'stop_headsign', 'trip_id',
                   'version']
        table = pd.DataFrame(index=np.r_[0:len(self.static_feed['stop_times'].index)], columns=columns)

        if 'Route_stop_seq' not in self.tables:
            self.route_stop_seq()
        if 'RunPattern' not in self.tables:
            self.run_pattern()

        runPattern_entry_memo = {}
        def schedules_row_func(i, row):
            new_row = self.tables[table_name].loc[i]

            if row['trip_id'] not in runPattern_entry_memo:
                runPattern_entry_memo[row['trip_id']] = self.tables['RunPattern'].loc[self.tables['RunPattern']['trip_id'] == row['trip_id']].iloc[0]
            runPattern_entry = runPattern_entry_memo[row['trip_id']]
            new_row['agency_id'] = runPattern_entry['agency_id']
            new_row['route_short_name'] = runPattern_entry['route_short_name']
            new_row['start_date'] = runPattern_entry['start_date']
            new_row['end_date'] = runPattern_entry['end_date']
            new_row['day'] = runPattern_entry['day']
            new_row['route_dir'] = runPattern_entry['route_dir']
            new_row['run'] = runPattern_entry['run']
            new_row['pattern_id'] = runPattern_entry['pattern_id']
            new_row['trip_id'] = runPattern_entry['trip_id']
            new_row['version'] = runPattern_entry['version']

            route_stop_seq_entry = self.tables['Route_stop_seq'].loc[self.tables['Route_stop_seq']['stop_id'] == row['stop_id']].iloc[0]
            new_row['seq'] = route_stop_seq_entry['seq']
            new_row['stop_id'] = route_stop_seq_entry['stop_id']
            new_row['is_time_point'] = route_stop_seq_entry['is_time_point']

            # TODO(erchpito) confirm default
            new_row['pickup_type'] = int(dataframeutility.optional_field(i, 'pickup_type', self.static_feed['stop_times'], default=0))
            # TODO(erchpito) confirm default
            new_row['dropoff_type'] = int(dataframeutility.optional_field(i, 'drop_off_type', self.static_feed['stop_times'], default=0))
            new_row['arrival_time'] = row['arrival_time']
            new_row['departure_time'] = row['departure_time']
            # TODO(erchpito) how to do default NULL
            new_row['stop_headsign'] = str(dataframeutility.optional_field(i, 'stop_headsign', self.static_feed['stop_times']))

        self.generate_table(table_name, table, schedules_row_func, rows=self.static_feed['stop_times'])

    # TODO(erchpito) shapes is an optional table in static GTFS
    def route_point_seq(self):
        table_name = 'Route_point_seq'
        columns = ['agency_id', 'route_short_name','route_dir', 'pattern_id', 
                   'shape_id', 'point_id', 'seq', 'length', 'heading', 'dist', 
                   'version']
        table = pd.DataFrame(index=np.r_[0:len(self.static_feed['shapes'].index)], columns=columns)

        if 'Route_stop_seq' not in self.tables:
            self.route_stop_seq()
        if 'Points' not in self.tables:
            self.points()

        route_stop_seq_entry_memo = {}
        last_point = None
        def route_point_seq_row_func(i, row):
            new_row = self.tables[table_name].loc[i]

            if row['shape_id'] not in route_stop_seq_entry_memo:
                # TODO(erchpito) make this dictionary
                pattern_id = self.shape2pattern[row['shape_id']]
                route_stop_seq_entry_memo[row['shape_id']] = self.tables['Route_stop_seq'].loc[self.tables['Route_stop_seq']['pattern_id'] == pattern_id].iloc[0]
                last_point = None
            route_stop_seq_entry = route_stop_seq_entry_memo[row['shape_id']]
            new_row['agency_id'] = route_stop_seq_entry['agency_id']
            new_row['route_short_name'] = route_stop_seq_entry['route_short_name']
            new_row['route_dir'] = route_stop_seq_entry['route_dir']
            new_row['pattern_id'] = route_stop_seq_entry['pattern_id']
            new_row['version'] = route_stop_seq_entry['version']

            points_entry = self.tables['Points'].loc[(self.tables['Points']['point_lat'] == row['shape_pt_lat']) & (self.tables['Points']['point_lon'] == row['shape_pt_lon'])].iloc[0]
            new_row['point_id'] = points_entry['point_id']

            new_row['shape_id'] = row['shape_id']
            new_row['seq'] = row['shape_pt_sequence'] + 1
            if last_point is None:
                new_row['length'] = 0
                total_dist = 0
                # TODO(erchpito) what is the default heading, or do we really mean to have the heading be between i and i + 1 (much harder to do)
                new_row['heading'] = 0
            else:
                new_row['length'] = maputility.get_distance(last_point[0], last_point[1], row['shape_pt_lat'], row['shape_pt_lon'])
                total_dist += new_row['length']
                new_row['heading'] = maputility.get_heading(last_point[0], last_point[1], row['shape_pt_lat'], row['shape_pt_lon'])
            new_row['dist'] = total_dist
            last_point = (row['shape_pt_lat'], row['shape_pt_lon'])

        self.generate_table(table_name, table, route_point_seq_row_func, rows=self.static_feed['shapes'])

    # TODO(erchpito) shapes is an optional table in static GTFS
    def points(self):
        table_name = 'Points'
        columns = ['agency_id', 'point_id','point_lat', 'point_lon', 'lat_lon', 
                   'version']
        table = pd.DataFrame()

        waypoints = []
        def points_row_func(i, row):
            new_row = {}

            waypoint = (row['shape_pt_lat'], row['shape_pt_lon'])
            if waypoint in waypoints:
                return
            else:
                waypoints.append(waypoint)

            new_row['agency_id'] = self.agencyID
            new_row['point_id'] = waypoints.index(waypoint)
            new_row['point_lat'] = row['shape_pt_lat']
            new_row['point_lon'] = row['shape_pt_lon']
            # TODO(erchpito) figure out lat_lon calculation
            new_row['lat_lon'] = None
            # TODO(erchpito) should be checksum
            new_row['version'] = 1

            self.tables[table_name] = self.tables[table_name].append(pd.Series(new_row), ignore_index=True)

        self.generate_table(table_name, table, points_row_func, rows=self.static_feed['shapes'])

    # TODO(erchpito) fare_rules and fare_attributes are optional tables in static GTFS
    def fare(self):
        table_name = 'Fare'
        columns = ['agency_id', 'route_short_name', 'route_dir', 'pattern_id', 'price', 'currency_type', 'payment_method', 'origin_id', 'destination_id', 'transfers', 'transfer_duration', 'fare_id', 'version']
        table = pd.DataFrame()

        def fare_row_func(i, row):
            pass

        # self.generate_table(table_name, table, fare_row_func, rows=self.static_feed['fare_rules'])

    # TODO(erchpito) calendar_dates is an optional table in static GTFS
    def calendar_dates(self):
        table_name = 'Calendar_dates'
        columns = ['agency_id', 'route_short_name', 'special_date', 'route_dir', 'run', 'exception_type', 'day', 'service_id', 'version']
        table = pd.DataFrame()

        def calendar_dates_row_func(i, row):
            pass

        # self.generate_table(table_name, table, calendar_dates_row_func, rows=self.static_feed['calendar_dates'])

    # MARK: TASK 2

    def transfers(self):
        table_name = 'Transfers'
        columns = ['from_agency_id', 'from_id', 'to_agency_id', 'to_id', 
                   'transfer_type', 'min_transfer_time', 'transfer_dist']
        table = pd.DataFrame()

        if 'Route_stop_seq' not in self.tables:
            self.route_stop_seq()

        max_distance = 100

        # TODO(erchpito) presume the given table is multimodal
        def transfers_row_func(i, row):
            pattern_id = row['pattern_id']
            from_agency = row['agency_id']
            from_stop_id = row['stop_id']
            from_stop_entry = self.static_feed['stops'].loc[self.static_feed['stops']['stop_id'] == from_stop_id].iloc[0]
            from_stop_point = (from_stop_entry['stop_lat'], from_stop_entry['stop_lon'])

            for _, subrow in self.tables['Route_stop_seq'].loc[self.tables['Route_stop_seq']['pattern_id'] != pattern_id].iterrows():
                to_agency = subrow['agency_id']
                to_stop_id = subrow['stop_id']
                # TODO(erchpito) this does not work since these stops can be from other agencies
                # this would be the only calculation that would have to search through the static GTFS feeds of other agencies
                to_stop_entry = self.static_feed['stops'].loc[self.static_feed['stops']['stop_id'] == to_stop_id].iloc[0]
                to_stop_point = (to_stop_entry['stop_lat'], to_stop_entry['stop_lon'])

                (transfer_dist, min_transfer_time) = maputility.get_distance_and_time(from_stop_point[0], from_stop_point[1], to_stop_point[0], to_stop_point[1])
                if transfer_dist > max_distance:
                    continue
                else:
                    new_row = {}
                    new_row['from_agency_id'] = from_agency
                    new_row['from_id'] = from_stop_id
                    new_row['to_agency_id'] = to_agency
                    new_row['to_id'] = to_stop_id

                    transfer_type = 0

                    if 'transfers' in self.static_feed and from_agency == to_agency:
                        transfers_entry = self.static_feed['transfers'].loc[(self.static_feed['transfers']['from_stop_id'] == from_stop_id) & (self.static_feed['transfers']['to_stop_id'] == to_stop_id)]
                        other_transfers_entry = self.static_feed['transfers'].loc[(self.static_feed['transfers']['from_stop_id'] == to_stop_id) & (self.static_feed['transfers']['to_stop_id'] == from_stop_id)]
                        if not transfers_entry.empty:
                            transfer_type = transfers_entry.iloc[0]['transfer_type']
                            min_transfer_time = int(dataframeutility.optional_field(0, 'min_transfer_time', transfers_entry, min_transfer_time))
                        elif not other_transfers_entry.empty:
                            transfer_type = other_transfers_entry.iloc[0]['transfer_type']
                            min_transfer_time = int(dataframeutility.optional_field(0, 'min_transfer_time', other_transfers_entry, min_transfer_time))

                    new_row['transfer_type'] = tranfer_type
                    new_row['min_transfer_time'] = min_transfer_time    
                    new_row['transfer_dist'] = transfer_dist

                    self.tables[table_name] = self.tables[table_name].append(pd.Series(new_row), ignore_index=True)

        self.generate_table(table_name, table, transfers_row_func, rows=self.tables['Route_stop_seq'])

    # MARK: TASK 3

    def gps_fixes(self):
        table_name = 'gps_fixes_2'
        columns = ['agency_id', 'veh_id', 'RecordedDate', 'RecordedTime', 
                   'UTC_at_date', 'latitude', 'longitude', 'speed', 'course']
        table = pd.DataFrame()

        def gps_fixes_row_func(entity):
            update = gtfsutility.VehiclePosition(entity.vehicle)
            trip = update.get_trip_descriptor()
            position = update.get_position()
            vehicle = update.get_vehicle_descriptor()
            misc = update.get_update_fields()

            new_row = {}
            new_row['agency_id'] = self.agencyID
            new_row['veh_id'] = int(vehicle['id'] if (vehicle and 'id' in vehicle) else -1)
            new_row['RecordedDate'] = str(datetime.now().strftime('%Y-%m-%d'))
            new_row['RecordedTime'] = str(datetime.now().strftime('%H:%M:%S'))
            # TODO(erchpito) there are timestamps in the VTA set but for whatever reason they're not read correctly
            timestamp = datetime.fromtimestamp(int(misc['timestamp'])) if (misc and 'timestamp' in misc) else -1
            new_row['UTC_at_date'] = str(timestamp.strftime('%Y-%m-%d') if (timestamp == -1) else 'N/A')
            new_row['UTC_at_time'] = str(timestamp.strftime('%H:%M:%S') if (timestamp == -1) else 'N/A')
            new_row['latitude'] = float(position['latitude'] if (position and 'latitude' in position) else -1)
            new_row['longitude'] = float(position['longitude'] if (position and 'longitude' in position) else -1)
            new_row['speed'] = float(position['speed'] if (position and 'speed' in position) else -1)
            new_row['course'] = float(position['bearing'] if (position and 'bearing' in position) else -1)
            self.tables[table_name] = self.tables[table_name].append(pd.Series(new_row), ignore_index=True)

            # TODO(erchpito) figure out what to do with this
            if (trip and 'trip_id' in trip) and (vehicle and 'id' in vehicle):
                self.trip2vehicle[trip['trip_id']] = vehicle['id']

        self.generate_table(table_name, table, gps_fixes_row_func, entities=self.vehicle_position_feed.entity)

    # TODO(erchpito) is this suppose to contain a row for every stop, or just ones that have updated times?
    def transit_eta(self):
        table_name = 'TransitETA'
        columns = ['agency_id', 'RecordedDate', 'RecordedTime', 'veh_id', 
                   'veh_lat', 'veh_lon', 'veh_speed', 'veh_location_time', 
                   'route_short_name', 'route_dir', 'day', 'run', 'pattern_id', 
                   'stop_id', 'seq', 'ETA']
        table = pd.DataFrame()

        if 'Route_stop_seq' not in self.tables:
            self.route_stop_seq()
        if 'RunPattern' not in self.tables:
            self.run_pattern()
        if 'Route_point_seq' not in self.tables:
            self.route_point_seq()
        if 'gps_fixes' not in self.tables:
            self.gps_fixes()

        def transit_eta_row_func(entity):
            update = gtfsutility.TripUpdate(entity.trip_update)
            trip_descriptor = update.get_trip_descriptor()
            stop_time_updates = update.get_stop_time_updates()

            trip_id = trip_descriptor['trip_id']
            runPattern_entry = self.tables['RunPattern'].loc[self.tables['RunPattern']['trip_id'] == trip_id].iloc[0]
            # trip_update can include the veh_id, not always an int
            veh_id = int(self.trip2vehicle[trip_id])
            gps_fixes_entry = self.tables['gps_fixes'].loc[self.tables['gps_Fixes']['veh_id'] == veh_id].iloc[0]
            s = datetime.strptime(gps_fixes_entry['UTC_at_date'] + ' ' + gps_fixes_entry['UTC_at_time'], '%Y-%m-%d %H:%M:%S')
            timestamp = time.mktime(s.timetuple())

            trip_id_block = self.static_feed['stop_times'].loc[self.static_feed['stop_times']['trip_id'] == trip_id]
            i = 0
            while i < len(stop_time_updates):
                stop_time_update = stop_time_updates[i]
                if 'departure' in stop_time_update or 'arrival' in stop_time_update:
                    stop_seq = stop_time_update['stop_sequence']
                    stop_seq_til = stop_time_updates[i + 1]['stop_sequence'] if i + 1 < len(stop_time_updates) else len(trip_id_block) + 1

                    time_diff = stop_time_update['departure' if 'departure' in stop_time_update else 'arrival']

                    if 'delay' in time_diff:
                        delay = timedelta(seconds=time_diff['delay'])
                    else: # it would seem this is stop_sequence specific
                        delay_time = datetime.datetime.fromtimestamp(int(time_diff['time']))
                        schedule_time = datetimeFromHMS(trip_id_block.iloc[stop_seq - 1]['departure_time'])
                        delay = delay_time - schedule_time

                    # this would ignore all stops until there's a delay
                    while stop_seq < stop_seq_til:
                        stop_times_entry = trip_id_block.iloc[stop_seq - 1]
                        new_row = {}
                        new_row['agency_id'] = self.agencyID
                        new_row['RecordedDate'] = str(datetime.now().strftime('%Y-%m-%d'))
                        new_row['RecordedTime'] = str(datetime.now().strftime('%H:%M:%S'))

                        new_row['veh_id'] = gps_fixes_entry['veh_id']
                        new_row['veh_lat'] = gps_fixes_entry['veh_lat']
                        new_row['veh_lon'] = gps_fixes_entry['veh_lon']
                        new_row['veh_speed'] = gps_fixes_entry['veh_speed']
                        new_row['veh_location_time'] =  timestamp

                        new_row['route_short_name'] = runPattern_entry['route_short_name']
                        new_row['route_dir'] = runPattern_entry['route_dir']
                        new_row['day'] = runPattern_entry['day']
                        new_row['run'] = runPattern_entry['run']
                        new_row['pattern_id'] = runPattern_entry['pattern_id']

                        new_row['stop_id'] = stop_times_entry['stop_id']
                        new_row['seq'] = stop_seq
                        new_row['ETA'] = str((datetimeFromHMS(stop['departure_time']) + delay).strftime('%H:%M:%S'))
                        self.tables[table_name] = self.tables[table_name].append(pd.Series(new_row), ignore_index=True)
                        stop_seq += 1

        self.generate_table(table_name, table, transit_eta, entities=self.trip_update_feed.entity)
