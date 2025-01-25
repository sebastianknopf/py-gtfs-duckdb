import csv
import duckdb
import importlib
import io
import logging
import os
import polars
import tempfile
import zipfile

import datetime as dt

from queue import Queue

import gtfslake.ddbdef

class GtfsLake:

    def __init__(self, database_filename, read_only_flag=False):
        self._connection = duckdb.connect(database=database_filename, read_only=read_only_flag)

        self._batch_size = 1000000

        self.static_tables = [
            'agency',
            'calendar_dates',
            'calendar',
            'feed_info',
            'routes',
            'shapes',
            'stop_times',
            'stops',
            'transfers',
            'trips'
        ]

        self.realtime_tables = [
            'realtime_service_alerts',
            'realtime_alert_active_periods',
            'realtime_alert_informed_entities',
            'realtime_trip_updates',
            'realtime_trip_stop_time_updates',
            'realtime_vehicle_positions'
        ]

        # queues for realtime data inserts
        self._realtime_service_alerts_delete_queue = Queue()
        self._realtime_trip_updates_delete_queue = Queue()
        self._realtime_vehicle_positions_delete_queue = Queue()

        self._realtime_service_alerts_queue = Queue()
        self._realtime_trip_update_queue = Queue()
        self._realtime_vehicle_positions_queue = Queue()

        if not read_only_flag:
            # generate static tables
            for static_table in self.static_tables:
                create_stmt = gtfslake.ddbdef.schema[static_table]
                self._connection.execute(create_stmt)

            # generate realtime tables
            for realtime_table in self.realtime_tables:
                create_stmt = gtfslake.ddbdef.schema[realtime_table]
                self._connection.execute(create_stmt)

    def load_static(self, gtfs_static_filename):
        with zipfile.ZipFile(gtfs_static_filename) as gtfs_static_file:
            for txt_filename in gtfs_static_file.namelist():
                if txt_filename.replace('.txt', '') in self.static_tables:
                    with io.TextIOWrapper(gtfs_static_file.open(txt_filename), encoding='utf-8') as txt_file:
                        logging.info(f"loading {txt_filename}")
                        
                        self._load_txt_file(txt_file, txt_filename.replace('.txt', ''))

    def remove_agencies(self, agency_pattern, remove_dependent_objects=True):
        self._connection.execute('DELETE FROM agency WHERE agency_id LIKE ?', [agency_pattern])

        if remove_dependent_objects:
            self._remove_dependent_objects()

    def remove_routes(self, route_pattern, remove_dependent_objects=True):
        self._connection.execute('DELETE FROM routes WHERE route_id LIKE ?', [route_pattern])

        if remove_dependent_objects:
            self._remove_dependent_objects()

    def remove_trips(self, trip_pattern, remove_dependent_objects=True):
        self._connection.execute('DELETE FROM trips WHERE trip_id LIKE ?', [trip_pattern])

        if remove_dependent_objects:
            self._remove_dependent_objects()

    def drop_subset(self, subset_filename, strategy_name):
        subset = duckdb.connect(subset_filename)

        strategy = importlib.import_module(f"gtfslake.strategy.{strategy_name}")
        strategy.run(self._connection, subset, self.static_tables)

    def export_static(self, output, tmpdir=tempfile.gettempdir()):
        if os.path.isdir(output):
            for tbl in self.static_tables:
                filename = os.path.join(output, f"{tbl}.txt")
                self._connection.sql(f"SELECT * FROM {tbl}").write_csv(filename, sep=',')

        elif output.lower().endswith('.zip'):
            tmp_files = dict()

            try:
                # export all tables to temporary txt files
                for tbl in self.static_tables:
                    with tempfile.NamedTemporaryFile(delete=False) as tmp:
                        filename = tmp.name
                        tmp_files[f"{tbl}.txt"] = filename

                    self._connection.sql(f"SELECT * FROM {tbl}").write_csv(filename, sep=',')

                with zipfile.ZipFile(output, 'w', zipfile.ZIP_DEFLATED) as gtfs_static_file:
                    for txt_filename, tmp_filename in tmp_files.items():
                        gtfs_static_file.write(
                            tmp_filename,
                            txt_filename
                        )
            finally:
                for tmp_file in tmp_files:
                    if os.path.exists(tmp_file):
                        os.remove(tmp_file)

    def insert_realtime_service_alert(self, service_alert, alert_active_periods, alert_informed_entities):
        self._realtime_service_alerts_queue.put((service_alert, alert_active_periods, alert_informed_entities))

    def fetch_realtime_service_alerts(self):

        service_alerts = self._connection.table('realtime_service_alerts').select(duckdb.StarExpression())
        alert_active_periods = self._connection.table('realtime_alert_active_periods').select(duckdb.StarExpression())
        alert_informed_entities = self._connection.table('realtime_alert_informed_entities').select(duckdb.StarExpression())

        return (service_alerts.pl(), alert_active_periods.pl(), alert_informed_entities.pl())

    def delete_realtime_service_alert(self, service_alert, alert_active_periods, alert_informed_entities):
        self._realtime_service_alerts_delete_queue.put((service_alert, alert_active_periods, alert_informed_entities))

    def insert_realtime_trip_updates(self, trip_update, stop_time_updates):
        self._realtime_trip_update_queue.put((trip_update, stop_time_updates))          

    def fetch_realtime_trip_updates(self):

        trip_updates = self._connection.table('realtime_trip_updates').select(duckdb.StarExpression())
        trip_stop_time_updates = self._connection.table('realtime_trip_stop_time_updates').select(duckdb.StarExpression())

        return (trip_updates.pl(), trip_stop_time_updates.pl())

    def delete_realtime_trip_updates(self, trip_update, stop_time_updates):
        self._realtime_trip_updates_delete_queue.put((trip_update, stop_time_updates))

    def insert_realtime_vehicle_positions(self, vehicle_positions):
        pass

    def fetch_realtime_vehicle_positions(self):
        vehicle_positions = self._connection.table('realtime_vehicle_positions').select(duckdb.StarExpression())

        return (vehicle_positions.pl())
    
    def clear_realtime_data(self):
        self._connection.execute('DELETE FROM realtime_vehicle_positions')
        self._connection.execute('DELETE FROM realtime_trip_updates')
        self._connection.execute('DELETE FROM realtime_trip_stop_time_updates')
        self._connection.execute('DELETE FROM realtime_service_alerts')
        self._connection.execute('DELETE FROM realtime_alert_active_periods')
        self._connection.execute('DELETE FROM realtime_alert_informed_entities')

    def fetch_nominal_stops(self):
        return self._connection.table('stops').select(duckdb.StarExpression()).pl()
    
    def fetch_nominal_routes(self):
        return self._connection.table('routes').select(duckdb.StarExpression()).pl()
    
    def fetch_nominal_operation_day_trips(self, operation_day_date: dt.datetime, full_trips = False):

        opd_reference = operation_day_date.strftime("%Y%m%d")
        opd_dayname = operation_day_date.strftime("%A").lower()

        # determine valid service days
        calendar_ids = self._connection.table('calendar').filter(f"start_date <= {opd_reference} AND end_date >= {opd_reference} AND {opd_dayname} = '1'").select(duckdb.ColumnExpression('service_id'))
        calendar_date_added_ids = self._connection.table('calendar_dates').filter(f"date = {opd_reference} AND exception_type = '1'").select(duckdb.ColumnExpression('service_id'))
        calendar_date_removed_ids = self._connection.table('calendar_dates').filter(f"date = {opd_reference} AND exception_type = '2'").select(duckdb.ColumnExpression('service_id'))

        service_ids = calendar_ids.union(calendar_date_added_ids).except_(calendar_date_removed_ids).fetchall()
        service_ids = list(zip(*service_ids))[0]

        # get all trips with stop times for matching services        
        trips = self._connection.table('trips').select(duckdb.StarExpression()).filter(duckdb.ColumnExpression('service_id').isin(*[duckdb.ConstantExpression(s) for s in service_ids]))
        stop_ids = self._connection.table('stop_times')

        if not full_trips:
            stop_ids = stop_ids.filter('stop_sequence = 1')

        return (trips.join(stop_ids, "stop_times.trip_id = trips.trip_id").order("trips.trip_id, stop_times.stop_sequence").pl())
    
    def fetch_realtime_operation_day_monitor_trips(self, operation_day_date: dt.datetime):
        opd_reference = operation_day_date.strftime("%Y%m%d")
        opd_dayname = operation_day_date.strftime("%A").lower()

        # determine valid service days
        calendar_ids = self._connection.table('calendar').filter(f"start_date <= {opd_reference} AND end_date >= {opd_reference} AND {opd_dayname} = '1'").select(duckdb.ColumnExpression('service_id'))
        calendar_date_added_ids = self._connection.table('calendar_dates').filter(f"date = {opd_reference} AND exception_type = '1'").select(duckdb.ColumnExpression('service_id'))
        calendar_date_removed_ids = self._connection.table('calendar_dates').filter(f"date = {opd_reference} AND exception_type = '2'").select(duckdb.ColumnExpression('service_id'))

        service_ids = calendar_ids.union(calendar_date_added_ids).except_(calendar_date_removed_ids).fetchall()
        service_ids = list(zip(*service_ids))[0]

        # get all trips with stop times for matching services        
        trips = self._connection.table('trips').select(duckdb.StarExpression()).filter(duckdb.ColumnExpression('service_id').isin(*[duckdb.ConstantExpression(s) for s in service_ids]))
        stops = self._connection.table('stops')
        routes = self._connection.table('routes')
        stop_times = self._connection.table('stop_times').filter('stop_sequence = 1')
        realtime_trip_updates = self._connection.table('realtime_trip_updates')

        # put it all together ...
        result = trips.join(routes, "routes.route_id = trips.route_id").join(stop_times, "stop_times.trip_id = trips.trip_id").join(realtime_trip_updates, 'realtime_trip_updates.trip_id = trips.trip_id', how='left').join(stops, 'stops.stop_id = stop_times.stop_id').order("stop_times.departure_time")

        # select required destination columns
        result_columns = [
            duckdb.ConstantExpression(opd_reference).alias('operation_day'),
            duckdb.ColumnExpression('routes.agency_id').alias('agency_id'),
            duckdb.ColumnExpression('trips.route_id').alias('route_id'),
            duckdb.ColumnExpression('routes.route_short_name').alias('route_short_name'),
            duckdb.ColumnExpression('trips.trip_id').alias('trip_id'),
            duckdb.ColumnExpression('trips.trip_headsign').alias('trip_headsign'),
            duckdb.ColumnExpression('trips.direction_id').alias('direction_id'),
            duckdb.ColumnExpression('stop_times.stop_id').alias('start_stop_id'),
            duckdb.ColumnExpression('stops.stop_name').alias('start_stop_name'),
            duckdb.ColumnExpression('stop_times.departure_time').alias('start_time'),
            duckdb.ColumnExpression('realtime_trip_updates.trip_id').isnotnull().alias('realtime_available'),
            duckdb.ColumnExpression('realtime_trip_updates.last_updated_timestamp').alias('realtime_last_update')
        ]

        return result.select(*result_columns).pl()

    def execute_sql(self, sqlfilename):
        with open(sqlfilename, 'r') as sql_file:
            sql_stmt = sql_file.read()

        self._connection.execute(sql_stmt)

    def _insert(self, table, columns, values, multiple=False):
        if not type(columns) == type(values):
            raise ValueError('mismatching input types for columns and values - both inputs must have same type')
        
        if not len(columns) == len(values):
            raise ValueError('mismatching input lengt for columns and values - both inputs must have same length')

        if multiple:
            for n in range(0, len(columns)):
                cols = columns[n]
                vals = values[n]

                self._connection.execute(f"INSERT INTO {table} ({','.join(cols)}) VALUES ({','.join(['?' for k in cols])})", vals)
        else:
            self._connection.execute(f"INSERT INTO {table} ({','.join(columns)}) VALUES ({','.join(['?' for k in columns])})", values)

    def _execute_realtime_queues(self, data_review_seconds=7200):
        # delete realtime elements which were not updated for more than 2 hours
        delete_timestamp = dt.datetime.now() - dt.timedelta(seconds=data_review_seconds)
        delete_timestamp = delete_timestamp.strftime('%Y-%m-%d %H:%M:%S')

        # TODO: implement clearing routine for service alerts too
        self._connection.execute("DELETE FROM realtime_trip_updates WHERE last_updated_timestamp <= strptime(?, '%Y-%m-%d %H:%M:%S')", [delete_timestamp])
        self._connection.execute("DELETE FROM realtime_trip_stop_time_updates WHERE last_updated_timestamp <= strptime(?, '%Y-%m-%d %H:%M:%S')", [delete_timestamp])

        self._connection.execute("DELETE FROM realtime_vehicle_positions WHERE last_updated_timestamp <= strptime(?, '%Y-%m-%d %H:%M:%S')", [delete_timestamp])

        # process service alerts
        while not self._realtime_service_alerts_delete_queue.empty():
            service_alert, alert_active_periods, alert_informed_entities = self._realtime_service_alerts_delete_queue.get()

            self._connection.execute('DELETE FROM realtime_service_alerts WHERE service_alert_id = ?', [service_alert['service_alert_id']])
            self._connection.execute('DELETE FROM realtime_alert_active_periods WHERE service_alert_id = ?', [service_alert['service_alert_id']])
            self._connection.execute('DELETE FROM realtime_alert_informed_entities WHERE service_alert_id = ?', [service_alert['service_alert_id']])
        
        while not self._realtime_service_alerts_queue.empty():
            service_alert, alert_active_periods, alert_informed_entities = self._realtime_service_alerts_queue.get()

            self._connection.execute('DELETE FROM realtime_service_alerts WHERE service_alert_id = ?', [service_alert['service_alert_id']])
            self._connection.execute('DELETE FROM realtime_alert_active_periods WHERE service_alert_id = ?', [service_alert['service_alert_id']])
            self._connection.execute('DELETE FROM realtime_alert_informed_entities WHERE service_alert_id = ?', [service_alert['service_alert_id']])

            self._insert('realtime_service_alerts', list(service_alert.keys()), list(service_alert.values()))
            
            if len(alert_active_periods) > 0:
                self._insert('realtime_alert_active_periods', [list(c.keys()) for c in alert_active_periods], [list(c.values()) for c in alert_active_periods], True)
            
            if len(alert_informed_entities) > 0:
                self._insert('realtime_alert_informed_entities', [list(c.keys()) for c in alert_informed_entities], [list(c.values()) for c in alert_informed_entities], True)
        
        # process trip updates
        while not self._realtime_trip_updates_delete_queue.empty():
            trip_update, stop_time_updates = self._realtime_trip_updates_delete_queue.get()

            self._connection.execute('DELETE FROM realtime_trip_updates WHERE trip_update_id = ?', [trip_update['trip_update_id']])
            self._connection.execute('DELETE FROM realtime_trip_stop_time_updates WHERE trip_update_id = ?', [trip_update['trip_update_id']])

        while not self._realtime_trip_update_queue.empty():
            trip_update, stop_time_updates = self._realtime_trip_update_queue.get()

            self._connection.execute('DELETE FROM realtime_trip_updates WHERE trip_update_id = ?', [trip_update['trip_update_id']])
            self._connection.execute('DELETE FROM realtime_trip_stop_time_updates WHERE trip_update_id = ?', [trip_update['trip_update_id']])

            self._insert('realtime_trip_updates', list(trip_update.keys()), list(trip_update.values()))
            self._insert('realtime_trip_stop_time_updates', [list(c.keys()) for c in stop_time_updates], [list(v.values()) for v in stop_time_updates], True)

        # process vehicle positions

    def _remove_dependent_objects(self):
        self._connection.execute('DELETE FROM routes WHERE agency_id NOT IN (SELECT agency_id FROM agency)')
        self._connection.execute('DELETE FROM trips WHERE route_id NOT IN (SELECT route_id FROM routes)')
        self._connection.execute('DELETE FROM stop_times WHERE trip_id NOT IN (SELECT trip_id FROM trips)')
        
        self._connection.execute('DELETE FROM stops WHERE (location_type = \'0\' OR location_type = \'\') AND stop_id NOT IN (SELECT stop_id FROM stop_times)')
        self._connection.execute('DELETE FROM stops WHERE location_type = \'1\' AND stop_id NOT IN (SELECT parent_station FROM stops)')

        self._connection.execute('DELETE FROM shapes WHERE shape_id NOT IN (SELECT shape_id FROM trips)')
        self._connection.execute('DELETE FROM transfers WHERE from_route_id NOT IN (SELECT route_id FROM routes) OR to_route_id NOT IN (SELECT route_id FROM routes)')
        self._connection.execute('DELETE FROM transfers WHERE from_trip_id NOT IN (SELECT trip_id FROM trips) OR to_trip_id NOT IN (SELECT trip_id FROM trips)')
        self._connection.execute('DELETE FROM calendar WHERE service_id NOT IN (SELECT service_id FROM trips)')
        self._connection.execute('DELETE FROM calendar_dates WHERE service_id NOT IN (SELECT service_id FROM trips)')

    def _load_txt_file(self, txt_file, table_name):

        # load existing headers of table, keep track of all header indices which exist in the TXT file but not in the DDB table
        existing_headers = list()
        hdr_index_blacklist = list()
        for record in self._connection.execute(f"DESCRIBE {table_name}").pl().iter_rows(named=True):
            existing_headers.append(record['column_name'])

        # open reader on TXT file
        headers = list()
        records = list()

        for row in csv.reader(txt_file):
            if len(headers) == 0:
                for index, hdr in enumerate(row):
                    if hdr in existing_headers:
                        headers.append(hdr)
                    else:
                        hdr_index_blacklist.append(index)
            else:
                record = list()
                for index, rec in enumerate(row):
                    if not index in hdr_index_blacklist:
                        record.append(rec)

                records.append(record)

            # insert if batch size was reached
            if len(records) >= self._batch_size:
                df = polars.DataFrame(records, schema=headers, orient='row')
                self._connection.execute(f"INSERT INTO {table_name} ({','.join(headers)}) SELECT * FROM df")
                records = list()

        df = polars.DataFrame(records, schema=headers, orient='row')
        self._connection.execute(f"INSERT INTO {table_name} ({','.join(headers)}) SELECT * FROM df")
        records = list()

