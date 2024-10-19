import click

from gtfslake.implementation import GtfsLake

@click.command
@click.argument('command')
@click.option('--input', '-i', help='Directory or ZIP file containing GTFS data')
@click.option('--database', '-d', help='Destination database for GTFS data lake')
def cli(command, input, database):

    lake = GtfsLake(database)

    if command == 'load':
        lake.load_static(input)


if __name__ == '__main__':
    cli()