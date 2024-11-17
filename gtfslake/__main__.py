import click
import time
import uvicorn

import datetime as dt
import polars as pl

from gtfslake.lake import GtfsLake
from gtfslake.realtime import GtfsLakeRealtimeServer


@click.group()
def cli():
    pass

@cli.command()
@click.argument('database')
@click.option('--input', '-i', help='Directory or ZIP file containing GTFS data')
def load(database, input):

    lake = GtfsLake(database)
    lake.load_static(input)

@cli.command()
@click.argument('database')
@click.option('--agencies', '-a', multiple=True, help='Pattern matching the agency IDs to be removed')
@click.option('--routes', '-r', multiple=True, help='Pattern matching the route IDs to be removed')
@click.option('--trips', '-t', multiple=True, help='Pattern matching the trip IDs to be removed')
def remove(database, agencies, routes, trips):

    lake = GtfsLake(database)

    for agency in agencies:
        lake.remove_agencies(agency, False)
    
    for route in routes:
        lake.remove_routes(route, False)

    for trip in trips:
        lake.remove_trips(trip, False)

    lake._remove_dependent_objects()

@cli.command()
@click.argument('database')
@click.option('--inputs', '-i', multiple=True, help='Filename of the DDB subset which should be dropped to the lake')
@click.option('--strategy', '-s', default='match_stop_id', help='Strategy used for matching existing data between the lake and the subset')
def drop(database, inputs, strategy):
    
    lake = GtfsLake(database)

    for subset in inputs:
        lake.drop_subset(subset, strategy_name=strategy)

@cli.command()
@click.argument('database')
@click.option('--output', '-o', help='Destination directory or ZIP file containing GTFS data')
def export(database, output):
    
    lake = GtfsLake(database)
    lake.export_static(output)

@cli.command()
@click.argument('database')
@click.option('--files', '-f', multiple=True, help='Filename of the file containing SQL statements')
def sql(database, files):

    lake = GtfsLake(database)

    for sql_file in files:
        lake.execute_sql(sql_file)

@cli.command()
@click.argument('database')
@click.option('--date', '-d', help='Date in format YYYYMMDD to show nominal trips for')
@click.option('--num-results', '-n', default=200, help='Number of results to show; ignored when using option --output/-o')
@click.option('--full-trips', '-f', default=False, help='Whether to select all stop times of a trip or not')
@click.option('--output', '-o', default=None, help='Output file for writing the results')
def show(database, date, num_results, full_trips, output):

    ref = dt.datetime.strptime(date, '%Y%m%d')
    
    lake = GtfsLake(database)

    start_time = time.time()
    trips = lake.fetch_nominal_operation_day_trips(ref, full_trips)
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

    realtime = GtfsLakeRealtimeServer(database, config)
    uvicorn.run(app=realtime.create(), host=host, port=int(port))

if __name__ == '__main__':
    cli()
