import duckdb

def run(lake, subset, tables):
    
    # check which tables exists in subset
    existing_tables = list()
    for tbl in subset.sql('CALL duckdb_tables()').pl().iter_rows(named=True):
        existing_tables.append(tbl['table_name'])

    # iterate over all objects of stops table in subset and match them with existing objects in lake
    for stop in subset.table('stops').select(duckdb.StarExpression()).pl().iter_rows(named=True):
        matching_stop = lake.table('stops').select(duckdb.ColumnExpression('stop_id')).filter(f"stop_id = '{stop['stop_id']}'").fetchone()
        
        if matching_stop: # this stop ID already exists, update its data only
            
            uds = list()
            for key in stop:
                if not key == 'stop_id':
                    uds.append(f"{key} = ?")

            uds = ', '.join(uds)

            vls = list()
            for key, value in stop.items():
                if not key == 'stop_id':
                    vls.append(value)

            lake.execute(f"UPDATE stops SET {uds} WHERE stop_id = ?", vls + [stop['stop_id']])
            
        else: # this stop ID does not exist yet, add it right now
            cls = ','.join(stop.keys())
            
            qms = ['?'] * len(stop.values())
            qms = ','.join(qms)

            lake.execute(f"INSERT INTO stops ({cls}) VALUES ({qms})", stop.values())

    # copy remaining tables
    for tbl in list(set(existing_tables) & set(tables)):
        if not tbl == 'stops': # remember: stops have been matched before
            localdf = subset.execute(f"SELECT * FROM {tbl}").pl()
            lake.sql(f"INSERT INTO {tbl} SELECT * FROM localdf")