import click

from gtfslake.implementation import GtfsLake


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


if __name__ == '__main__':
    cli()