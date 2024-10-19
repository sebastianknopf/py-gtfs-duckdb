import click
import csv
import duckdb
import polars

def csv2ddb(file, db, table, schema, batch, verbose):

	# print filename for debugging in verbose mode
	if verbose:
		print(f"loading {file} ...")
	
	# open connection to destination DB
	connection = duckdb.connect(db)
	
	# run create statement if present
	if schema is not None:
		with open(schema, 'r') as stmt_file:
			stmt = stmt_file.read()
			connection.sql(stmt)
	
	# load existing headers of table
	existing_headers = list()

	ddb_result = connection.sql(f"DESCRIBE {table}").fetchall()
	for record in ddb_result:
		existing_headers.append(record[0])

	# open CSV file
	hdr_index_blacklist = list()
	with open(file, 'r') as csv_file:
		csv_reader = csv.reader(csv_file)
		
		headers = list()
		records = list()
		
		batch_count = 1
		for row in csv_reader:
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
			if len(records) >= batch:
				if verbose:
					print(f"inserting batch {batch_count} ...")
			
				df = polars.DataFrame(records, schema=headers, orient='row')
				connection.sql(f"INSERT INTO {table} ({','.join(headers)}) SELECT * FROM df")
				records = list()
				
				batch_count = batch_count + 1
				
		
		# final insert for all remaining datasets
		if verbose:
			print(f"inserting final batch ...")
		
		df = polars.DataFrame(records, schema=headers, orient='row')
		connection.sql(f"INSERT INTO {table} ({','.join(headers)}) SELECT * FROM df")
		records = list()
		
		# print out num total datasets in verbose mode
		if verbose:
			connection.sql(f"SELECT COUNT(*) FROM {table}").show()
	
	
	# close connection
	connection.close()

@click.command
@click.option('--file', '-f', help='Input CSV file')
@click.option('--db', '-d', help='DuckDB filename')
@click.option('--table', '-t', help='Destination table name to import the CSV file')
@click.option('--schema', '-s', default=None, help='Schema filename for destination table')
@click.option('--batch', '-b', default=100000, help='Batch size to import into database')
@click.option('--verbose', '-v', default=False, is_flag=True, help='Print out verbose information messages')
def cli(file, db, table, schema, batch, verbose):
        csv2ddb(file, db, table, schema, batch, verbose)

if __name__ == '__main__':
	cli();
