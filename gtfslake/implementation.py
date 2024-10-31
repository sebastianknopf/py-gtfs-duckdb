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

    def __init__(self, database_filename):
        self._connection = duckdb.connect(database_filename)

        self._tables = [
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

        self._batch_size = 1000000

    def load_static(self, gtfs_static_filename):
        with zipfile.ZipFile(gtfs_static_filename) as gtfs_static_file:
            for txt_filename in gtfs_static_file.namelist():
                if txt_filename.replace('.txt', '') in self._tables:
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
        strategy.run(self._connection, subset, self._tables)

    def export_static(self, output, tmpdir=tempfile.gettempdir()):
        if os.path.exists(output):
            for tbl in self._tables:
                filename = os.path.join(output, f"{tbl}.txt")                    
                self._connection.sql(f"SELECT * FROM {tbl}").write_csv(filename, sep=',')

        elif output.lower().endswith('.zip'):
            
            tmp_files = dict()
            
            try:
                # export all tables to temporary txt files
                for tbl in self._tables:
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

    def execute_sql(self, sqlfilename):
        with open(sqlfilename, 'r') as sql_file:
            sql_stmt = sql_file.read()

        self._connection.execute(sql_stmt)

    def _remove_dependent_objects(self):
        self._connection.execute('DELETE FROM routes WHERE agency_id NOT IN (SELECT agency_id FROM agency)')
        self._connection.execute('DELETE FROM trips WHERE route_id NOT IN (SELECT route_id FROM routes)')
        self._connection.execute('DELETE FROM stop_times WHERE trip_id NOT IN (SELECT trip_id FROM trips)')
        self._connection.execute('DELETE FROM shapes WHERE shape_id NOT IN (SELECT shape_id FROM trips)')
        self._connection.execute('DELETE FROM transfers WHERE from_route_id NOT IN (SELECT route_id FROM routes) OR to_route_id NOT IN (SELECT route_id FROM routes)')
        self._connection.execute('DELETE FROM transfers WHERE from_trip_id NOT IN (SELECT trip_id FROM trips) OR to_trip_id NOT IN (SELECT trip_id FROM trips)')
        self._connection.execute('DELETE FROM calendar WHERE service_id NOT IN (SELECT service_id FROM trips)')
        self._connection.execute('DELETE FROM calendar_dates WHERE service_id NOT IN (SELECT service_id FROM trips)')

    def _load_txt_file(self, txt_file, table_name):

        # run create statement to ensure destination table exists
        create_stmt = gtfslake.ddbdef.schema[table_name]
        self._connection.execute(create_stmt)
        
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
        
