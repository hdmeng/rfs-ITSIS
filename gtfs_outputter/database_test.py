#! /usr/bin/python

import pandas as pd
import sqlalchemy as sa
import sqlalchemy.dialects.mysql as samysql
import sys

def main(argv):
    username = 'root'
    password = '3EasSarcasEgot3'
    host = 'localhost'
    port = '3306'
    database = 'gtfs'

    engine = sa.create_engine('mysql://{0}:{1}@{2}:{3}/{4}'.format(username, password, host, port, database))
    with engine.connect() as conn, conn.begin():

        metadata = sa.MetaData()

        agency = sa.Table('Agency', metadata,
            sa.Column('agency_id', samysql.INTEGER(10, unsigned=True), nullable=False, primary_key=True),
            sa.Column('agency_name', samysql.VARCHAR(255), nullable=False),
            sa.Column('agency_url', samysql.VARCHAR(255), nullable=False),
            sa.Column('agency_timezone', samysql.SMALLINT(6), nullable=False, key='agency_timezone'),
            sa.Column('agency_lang', samysql.VARCHAR(255), nullable=False),
            sa.Column('agency_phone', samysql.VARCHAR(255), nullable=False),
            sa.Column('timezone_name', samysql.VARCHAR(45), nullable=False)
            )
        agency.create(engine)

        routes = sa.Table('Routes', metadata,
            sa.Column('agency_id', samysql.INTEGER(10, unsigned=True), nullable=False, primary_key=True),
            sa.Column('route_short_name', samysql.VARCHAR(255), nullable=False, primary_key=True),
            sa.Column('route_dir', samysql.INTEGER(10, unsigned=True), nullable=False, primary_key=True),
            sa.Column('route_type', samysql.INTEGER(10, unsigned=True), nullable=False),
            sa.Column('route_long_name', samysql.VARCHAR(255), nullable=False, default='N/A'),
            sa.Column('route_desc', samysql.VARCHAR(255), nullable=False, default='N/A'),
            sa.Column('route_url', samysql.VARCHAR(255), nullable=False, default='N/A'),
            sa.Column('route_color', samysql.VARCHAR(255), nullable=False, default='FFFFFF'),
            sa.Column('route_text_color', samysql.VARCHAR(255), nullable=False, default='000000'),
            sa.Column('route_id', samysql.VARCHAR(255), nullable=False, default='000000'),
            sa.Column('version', samysql.VARCHAR(255), nullable=False, primary_key=True)
            )
        routes.create(engine)

        stops = sa.Table('Stops', metadata,
            sa.Column('agency_id', samysql.INTEGER(10, unsigned=True), nullable=False, primary_key=True),
            sa.Column('stop_id', samysql.VARCHAR(255), nullable=False, primary_key=True),
            sa.Column('stop_code', samysql.VARCHAR(255), nullable=False, default='N/A'),
            sa.Column('stop_name', samysql.VARCHAR(255), nullable=False),
            sa.Column('stop_desc', samysql.VARCHAR(255), nullable=False, default='N/A'),
            sa.Column('stop_lat', samysql.DOUBLE(), nullable=False),
            sa.Column('stop_lon', samysql.DOUBLE(), nullable=False),
            sa.Column('stop_url', samysql.VARCHAR(255), nullable=False, default='N/A'),
            sa.Column('location_type', samysql.INTEGER(10, unsigned=True), nullable=False, default=0),
            sa.Column('parent_station', samysql.BIGINT(20), nullable=False, default=0),
            sa.Column('wheelchair_boarding', samysql.INTEGER(10, unsigned=True), nullable=False, default=0),
            sa.Column('version', samysql.VARCHAR(255), nullable=False, primary_key=True)
            )
        stops.create(engine)

        trip_pattern_shape = sa.Table('Trip_pattern_shape', metadata,
            sa.Column('agency_id', samysql.INTEGER(10, unsigned=True), nullable=False, primary_key=True),
            sa.Column('trip_id', samysql.VARCHAR(255), nullable=False, primary_key=True),
            sa.Column('pattern_id', samysql.VARCHAR(255), nullable=False, primary_key=True),
            sa.Column('shape_id', samysql.VARCHAR(255), default='N/A'),
            )
        trip_pattern_shape.create(engine)

        route_stop_seq = sa.Table('Route_stop_seq', metadata,
            sa.Column('agency_id', samysql.INTEGER(10, unsigned=True), nullable=False, primary_key=True),
            sa.Column('route_short_name', samysql.VARCHAR(255), nullable=False, primary_key=True),
            sa.Column('route_dir', samysql.INTEGER(10, unsigned=True), nullable=False, primary_key=True),
            sa.Column('pattern_id', samysql.VARCHAR(255), nullable=False, primary_key=True),
            sa.Column('stop_id', samysql.VARCHAR(255), nullable=False),
            sa.Column('seq', samysql.INTEGER(10, unsigned=True), nullable=False, primary_key=True),
            sa.Column('is_time_point', samysql.INTEGER(10, unsigned=True), nullable=False, default=0),
            sa.Column('version', samysql.VARCHAR(255), nullable=False, primary_key=True)
            )
        route_stop_seq.create(engine)

        run_pattern = sa.Table('RunPattern', metadata,
            sa.Column('agency_id', samysql.INTEGER(10, unsigned=True), nullable=False, primary_key=True),
            sa.Column('route_short_name', samysql.VARCHAR(255), nullable=False, primary_key=True),
            sa.Column('start_date', samysql.DATE(), nullable=False, primary_key=True),
            sa.Column('end_date', samysql.DATE(), nullable=False),
            sa.Column('service_id', samysql.VARCHAR(255), nullable=False),
            sa.Column('day', samysql.CHAR(7), nullable=False, primary_key=True),
            sa.Column('route_dir', samysql.INTEGER(10, unsigned=True), nullable=False, primary_key=True),
            sa.Column('run', samysql.INTEGER(10, unsigned=True), nullable=False, primary_key=True),
            sa.Column('pattern_id', samysql.VARCHAR(255), nullable=False),
            sa.Column('trip_headsign', samysql.VARCHAR(255), nullable=False),
            sa.Column('trip_id', samysql.VARCHAR(255), nullable=False),
            sa.Column('version', samysql.VARCHAR(255), nullable=False, primary_key=True)
            )
        run_pattern.create(engine)

        schedules = sa.Table('Schedules', metadata,
            sa.Column('agency_id', samysql.INTEGER(10, unsigned=True), nullable=False, primary_key=True),
            sa.Column('route_short_name', samysql.VARCHAR(255), nullable=False, primary_key=True, key='route_short_name'),
            sa.Column('start_date', samysql.DATE(), nullable=False, primary_key=True, key='start_date'),
            sa.Column('end_date', samysql.DATE(), nullable=False),
            sa.Column('day', samysql.CHAR(7), nullable=False, primary_key=True, key='day'),
            sa.Column('route_dir', samysql.INTEGER(10, unsigned=True), nullable=False, primary_key=True),
            sa.Column('run', samysql.INTEGER(10, unsigned=True), nullable=False, primary_key=True, key='run'),
            sa.Column('pattern_id', samysql.VARCHAR(255), nullable=False),
            sa.Column('seq', samysql.INTEGER(10, unsigned=True), nullable=False, primary_key=True),
            sa.Column('stop_id', samysql.VARCHAR(255), nullable=False, key='stop_id'),
            sa.Column('is_time_point', samysql.INTEGER(10, unsigned=True), nullable=False, default=0),
            sa.Column('pickup_type', samysql.INTEGER(10, unsigned=True), nullable=False),
            sa.Column('dropoff_type', samysql.INTEGER(10, unsigned=True), nullable=False),
            sa.Column('arrival_time', samysql.VARCHAR(10), nullable=False),
            sa.Column('departure_time', samysql.VARCHAR(10), nullable=False),
            sa.Column('stop_headsign', samysql.VARCHAR(255), nullable=False),
            sa.Column('trip_id', samysql.VARCHAR(255), nullable=False),
            sa.Column('version', samysql.VARCHAR(255), nullable=False, primary_key=True)
            )
        schedules.create(engine)

        route_point_seq = sa.Table('Route_point_seq', metadata,
            sa.Column('agency_id', samysql.INTEGER(10, unsigned=True), nullable=False),
            sa.Column('route_short_name', samysql.VARCHAR(255), nullable=False),
            sa.Column('route_dir', samysql.INTEGER(10, unsigned=True), nullable=False),
            sa.Column('pattern_id', samysql.VARCHAR(255), nullable=False),
            sa.Column('shape_id', samysql.VARCHAR(255), nullable=False),
            sa.Column('point_id', samysql.INTEGER(10, unsigned=True), nullable=False),
            sa.Column('seq', samysql.INTEGER(10, unsigned=True), nullable=False),
            sa.Column('length', samysql.DOUBLE(), nullable=False),
            sa.Column('heading', samysql.DOUBLE(), nullable=False),
            sa.Column('dist', samysql.DOUBLE(), nullable=False),
            sa.Column('version', samysql.VARCHAR(255), nullable=False)
            )
        route_point_seq.create(engine)

        points = sa.Table('Points', metadata,
            sa.Column('agency_id', samysql.INTEGER(10, unsigned=True), nullable=False, primary_key=True),
            sa.Column('point_id', samysql.INTEGER(10, unsigned=True), nullable=False, primary_key=True),
            sa.Column('point_lat', samysql.DOUBLE(), nullable=False),
            sa.Column('point_lon', samysql.DOUBLE(), nullable=False),
            sa.Column('version', samysql.VARCHAR(255), nullable=False, primary_key=True)
            )
        points.create(engine)

        transit_eta = sa.Table('TransitETA', metadata,
            sa.Column('agency_id', samysql.INTEGER(10, unsigned=True), nullable=False, primary_key=True),
            sa.Column('RecordedDate', samysql.DATE(), nullable=False, primary_key=True, key='RecordedDate'),
            sa.Column('RecordedTime', samysql.TIME(), nullable=False, primary_key=True),
            sa.Column('veh_id', samysql.INTEGER(11), nullable=False, primary_key=True),
            sa.Column('veh_lat', samysql.DOUBLE(), nullable=False),
            sa.Column('veh_lon', samysql.DOUBLE(), nullable=False),
            sa.Column('veh_speed', samysql.DOUBLE(), nullable=False),
            sa.Column('veh_location_time', samysql.BIGINT(20), nullable=False),
            sa.Column('route_short_name', samysql.VARCHAR(255), nullable=False, primary_key=True, key='route_short_name'),
            sa.Column('route_dir', samysql.INTEGER(10, unsigned=True), nullable=False, primary_key=True),
            sa.Column('day', samysql.CHAR(7), nullable=False, primary_key=True),
            sa.Column('run', samysql.INTEGER(10, unsigned=True), nullable=False, primary_key=True),
            sa.Column('pattern_id', samysql.VARCHAR(255), nullable=False),
            sa.Column('stop_id', samysql.VARCHAR(255), nullable=False, key='stop_id'),
            sa.Column('seq', samysql.INTEGER(10, unsigned=True), nullable=False, primary_key=True),
            sa.Column('ETA', samysql.TIME(), nullable=False, primary_key=True)
            )
        transit_eta.create(engine)

        # df = pd.read_csv('./agencies/tri_delta/processed/stops.csv', sep=',', header=0, na_values=['', '#N/A', '#N/A N/A', '#NA', '-1.#IND', '-1.#QNAN', '-NaN', '-nan', '1.#IND', '1.#QNAN', 'NA', 'NULL', 'NaN', 'nan'], keep_default_na=False)
        # df.replace('\"', '')
        # # print(df)
        #
        # df.to_sql('Stops', engine, chunksize=1000, if_exists='append', index=False,
        #           dtype={'agency_id': samysql.INTEGER(10, unsigned=True),
        #                   'stop_id': samysql.VARCHAR(255),
        #                   'stop_code': samysql.VARCHAR(255),
        #                   'stop_name': samysql.VARCHAR(255),
        #                   'stop_desc': samysql.VARCHAR(255),
        #                   'stop_lat': samysql.DOUBLE(),
        #                   'stop_lon': samysql.DOUBLE(),
        #                   'lat_lon': samysql.INTEGER(10),
        #                   'stop_url': samysql.VARCHAR(255),
        #                   'location_type': samysql.INTEGER(10, unsigned=True),
        #                   'parent_station': samysql.BIGINT(20, unsigned=True),
        #                   'wheelchair_boarding': samysql.INTEGER(10, unsigned=True),
        #                   'version': samysql.VARCHAR(255),
        #                   })
        #
        # df = pd.read_csv('./agencies/bart/processed/stops.csv', sep=',', header=0, na_values=['', '#N/A', '#N/A N/A', '#NA', '-1.#IND', '-1.#QNAN', '-NaN', '-nan', '1.#IND', '1.#QNAN', 'NA', 'NULL', 'NaN', 'nan'], keep_default_na=False)
        # df.replace('\"', '')
        # # print(df)
        #
        # df.to_sql('Stops', engine, chunksize=1000, if_exists='append', index=False,
        #           dtype={'agency_id': samysql.INTEGER(10, unsigned=True),
        #                   'stop_id': samysql.VARCHAR(255),
        #                   'stop_code': samysql.VARCHAR(255),
        #                   'stop_name': samysql.VARCHAR(255),
        #                   'stop_desc': samysql.VARCHAR(255),
        #                   'stop_lat': samysql.DOUBLE(),
        #                   'stop_lon': samysql.DOUBLE(),
        #                   'lat_lon': samysql.INTEGER(10),
        #                   'stop_url': samysql.VARCHAR(255),
        #                   'location_type': samysql.INTEGER(10, unsigned=True),
        #                   'parent_station': samysql.BIGINT(20, unsigned=True),
        #                   'wheelchair_boarding': samysql.INTEGER(10, unsigned=True),
        #                   'version': samysql.VARCHAR(255),
        #                   })

        # df = pd.read_sql_table('Stops', engine)
        # df.replace('\"', '')
        # print(df)

    # stops.drop(engine, checkfirst=True)
    # Column('id', Integer, ForeignKey(stops.c.agency_id))

if __name__ == '__main__':
    main(sys.argv)
