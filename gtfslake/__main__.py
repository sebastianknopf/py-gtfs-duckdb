import click
import uvicorn

from gtfslake.implementation import GtfsLake
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
@click.option('--host', '-h', help='Host to run the realtime server')
@click.option('--port', '-p', help='Port to run the realtime server')
@click.option('--config', '-c', default=None, help='Additional configuration file for realtime server')
def realtime(database, config, host, port):

    realtime = GtfsLakeRealtimeServer(database, config)

    uvicorn.run(app=realtime.create(), host=host, port=int(port))

if __name__ == '__main__':
    cli()
