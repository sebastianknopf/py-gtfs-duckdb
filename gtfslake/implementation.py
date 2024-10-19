import csv
import duckdb
import io
import polars
import zipfile

import gtfslake.ddbdef

class GtfsLake:

    def __init__(self, database_filename):
        self._connection = duckdb.connect(database_filename)

        self._batch_size = 1000000

    def load_static(self, gtfs_static_filename):
        
        txt_filenames = [
            'agency.txt', 
            'calendar_dates.txt', 
            'calendar.txt', 
            'feed_info.txt', 
            'routes.txt', 
            'shapes.txt', 
            'stop_times.txt', 
            'stops.txt', 
            'transfers.txt', 
            'trips.txt'
        ]
        
        with zipfile.ZipFile(gtfs_static_filename) as gtfs_static_file:
            for txt_filename in gtfs_static_file.namelist():
                if txt_filename in txt_filenames:
                    with io.TextIOWrapper(gtfs_static_file.open(txt_filename), encoding='utf-8') as txt_file:
                        self._load_txt_file(txt_file, txt_filename.replace('.txt', ''))

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
        