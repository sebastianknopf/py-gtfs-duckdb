import click
import logging
import time
import uvicorn

import datetime as dt
import polars as pl

from gtfsduckdb.ddb import GtfsDuckDB
from gtfsduckdb.realtime import GtfsRealtimeServer
from gtfsduckdb.version import __version__


logging.basicConfig(
    level=logging.INFO, 
    format= '[%(asctime)s] %(levelname)s: %(message)s',
    datefmt='%H:%M:%S'
)

@click.group()
def cli():
    pass

@cli.command()
def version():
    print(__version__)

@cli.command()
@click.argument('database')
@click.option('--input', '-i', help='Directory or ZIP file containing GTFS data')
def load(database, input):

    ddb = GtfsDuckDB(database)
    ddb.load_static(input)

@cli.command()
@click.argument('database')
@click.option('--agencies', '-a', multiple=True, help='Pattern matching the agency IDs to be removed')
@click.option('--routes', '-r', multiple=True, help='Pattern matching the route IDs to be removed')
@click.option('--trips', '-t', multiple=True, help='Pattern matching the trip IDs to be removed')
def remove(database, agencies, routes, trips):

    ddb = GtfsDuckDB(database)

    for agency in agencies:
        ddb.remove_agencies(agency, False)
    
    for route in routes:
        ddb.remove_routes(route, False)

    for trip in trips:
        ddb.remove_trips(trip, False)

    ddb._remove_dependent_objects()

@cli.command()
@click.argument('database')
@click.option('--inputs', '-i', multiple=True, help='Filename of the DDB subset which should be dropped to the actual database')
@click.option('--strategy', '-s', default='match_stop_id', help='Strategy used for matching existing data between the actual database and the subset')
def drop(database, inputs, strategy):
    
    ddb = GtfsDuckDB(database)

    for subset in inputs:
        ddb.drop_subset(subset, strategy_name=strategy)

@cli.command()
@click.argument('database')
@click.option('--output', '-o', help='Destination directory or ZIP file containing GTFS data')
def export(database, output):
    
    ddb = GtfsDuckDB(database)
    ddb.export_static(output)

@cli.command()
@click.argument('database')
@click.option('--files', '-f', multiple=True, help='Filename of the file containing SQL statements')
def sql(database, files):

    ddb = GtfsDuckDB(database)

    for sql_file in files:
        ddb.execute_sql(sql_file)

@cli.command()
@click.argument('database')
@click.option('--date', '-d', help='Date in format YYYYMMDD to show nominal trips for')
@click.option('--num-results', '-n', default=200, help='Number of results to show; ignored when using option --output/-o')
@click.option('--full-trips', '-f', default=False, help='Whether to select all stop times of a trip or not')
@click.option('--output', '-o', default=None, help='Output file for writing the results')
def show(database, date, num_results, full_trips, output):

    ref = dt.datetime.strptime(date, '%Y%m%d')
    
    ddb = GtfsDuckDB(database)

    start_time = time.time()
    trips = ddb.fetch_nominal_operation_day_trips(ref, full_trips)
    end_time = time.time()

    if output is not None:
        trips.write_csv(output)
    else:
        pl.Config.set_tbl_rows(num_results)

        print(f"found {len(trips)} ({num_results} shown) results in {str(end_time - start_time)} seconds")
        print()
        print(trips.select(['route_id', 'trip_id', 'direction_id', 'trip_headsign', 'stop_id', 'departure_time']))

@cli.command()
@click.argument('database')
@click.option('--host', '-h', default='0.0.0.0', help='Host to run the realtime server')
@click.option('--port', '-p', default='8030', help='Port to run the realtime server')
@click.option('--config', '-c', default=None, help='Additional configuration file for realtime server')
def realtime(database, config, host, port):

    realtime = GtfsRealtimeServer(database, config)
    uvicorn.run(app=realtime.create(), host=host, port=int(port))

if __name__ == '__main__':
    cli()
