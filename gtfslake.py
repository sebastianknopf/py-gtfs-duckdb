import click
import duckdb
import logging

@click.command
@click.argument('cmd')
@click.argument('destination')
@click.argument('args', nargs=-1)
def cli(cmd, destination, args):

	lake = duckdb.connect(destination)

	if len(args) < 1:
		logging.error('required at least one argument, run --help for details')

	if cmd == 'drop':
		for arg in args:
			subset = duckdb.connect(arg)

			existing_tables = list()
			for tbl in subset.sql('CALL duckdb_tables()').fetchall():
				existing_tables.append(tbl[4])

			copy_tables = ['agency', 'routes', 'trips', 'stop_times', 'calendar', 'calendar_dates']

			for tbl in list(set(existing_tables) & set(copy_tables)):
				localdf = subset.execute(f"SELECT * FROM {tbl}").pl()
				lake.sql(f"INSERT INTO {tbl} SELECT * FROM localdf")

	elif cmd == 'remove':
		for arg in args:
			lake.execute('DELETE FROM routes WHERE route_id LIKE $route_id', {'route_id': arg})

		lake.execute('DELETE FROM trips WHERE route_id NOT IN (SELECT route_id FROM routes)')
		lake.execute('DELETE FROM stop_times WHERE trip_id NOT IN (SELECT trip_id FROM trips)')
		lake.execute('DELETE FROM shapes WHERE shape_id NOT IN (SELECT shape_id FROM trips)')
		lake.execute('DELETE FROM transfers WHERE from_route_id NOT IN (SELECT route_id FROM routes) OR to_route_id NOT IN (SELECT route_id FROM routes)')
		lake.execute('DELETE FROM transfers WHERE from_trip_id NOT IN (SELECT trip_id FROM trips) OR to_trip_id NOT IN (SELECT trip_id FROM trips)')
		lake.execute('DELETE FROM calendar WHERE service_id NOT IN (SELECT service_id FROM trips)')
		lake.execute('DELETE FROM calendar_dates WHERE service_id NOT IN (SELECT service_id FROM trips)')

if __name__ == '__main__':
	cli()
