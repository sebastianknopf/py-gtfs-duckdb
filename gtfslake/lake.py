import csv
import duckdb
import importlib
import io
import os
import polars
import tempfile
import zipfile

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

    def fetch_realtime_service_alerts(self):

        service_alerts = self._connection.table('realtime_service_alerts').select(duckdb.StarExpression())
        alert_active_periods = self._connection.table('realtime_alert_active_periods').select(duckdb.StarExpression())
        alert_informed_entities = self._connection.table('realtime_alert_informed_entities').select(duckdb.StarExpression())

        return (service_alerts.pl(), alert_active_periods.pl(), alert_informed_entities.pl())

    def fetch_realtime_trip_updates(self):

        trip_updates = self._connection.table('realtime_trip_updates').select(duckdb.StarExpression())
        trip_stop_time_updates = self._connection.table('realtime_trip_stop_time_updates').select(duckdb.StarExpression())

        return (trip_updates.pl(), trip_stop_time_updates.pl())

    def fetch_realtime_vehicle_positions(self):
        vehicle_positions = self._connection.table('realtime_vehicle_positions').select(duckdb.StarExpression())

        return (vehicle_positions.pl())

    def execute_sql(self, sqlfilename):
        with open(sqlfilename, 'r') as sql_file:
            sql_stmt = sql_file.read()

        self._connection.execute(sql_stmt)

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

