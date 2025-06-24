# Commands
There're several commands which you can execute for using the GTFS-DuckDB. All commands are executed in the following scheme after installation:

```bash
python -m gtfsduckdb {command} {arguments} [options]
```

## version
To see the currently installed GTFS-DuckDB version, run
```bash
python -m gtfsduckdb version
```

## load
To load a initial GTFS dataset into the GTFS-DuckDB, run
```bash
python -m gtfsduckdb load ./database.ddb -i ./gtfs.zip
``` 
Options:
- -i/--input        Path to the GTFS dataset to load

## remove
To remove unwanted data from the loaded GTFS data, run
```bash
python -m gtfsduckdb remove ./database.ddb -a agency-1 -r de:vpe:04* -t de:vpe:trip:12345,de:vpe:trip:6789
``` 
All objects, which are not related to other objects anymore after deleting some entities, are also removed entirely.

Options:
- -a/--agencies     ID of the agencies to be removed; can be separated by comma; * = wildcard
- -r/--routes       ID of the routes to be removed; can be separated by comma; * = wildcard
- -t/--trips        ID of the trips to be removed; can be separated by comma; * = wildcard

## drop
To merge ("drop") another GTFS dataset to the existing GTFS dataset, run
```bash
python -m gtfsduckdb drop ./database.ddb -i ./additional-gtfs.zip [-s match_stop_id]
``` 
Additional data are added to the existing GTFS data using certain merging strategies. See [strategies overview](STRATEGY.py) for reference.

Options:
- -i/--inputs       Path to additional GTFS feeds to be added; can be separated by comma
- -s/--strategy     Name of the merging strategy to be used

## export
To export a ready modified GTFS dataset from GTFS-DuckDB, run
```bash
python -m gtfsduckdb export ./database.ddb -o ./gtfs.zip
``` 
Options:
- -o/--output       Path to the destination GTFS dataset to load

## sql
To execute arbitrary SQL statements in the GTFS-DuckDB, run
```bash
python -m gtfsduckdb sql ./database.ddb -f ./sql_statement.sql
``` 
Options:
- -f/--files        Path to the SQL files to be executed

## realtime
To start a GTFS-RT compliant realtime server, run
```bash
python -m gtfsduckdb realtime ./database.ddb -h 127.0.0.1 -p 8080 -c ./gtfsduckdb-realtime.yaml
``` 
Please see detailed information [about running a realtime server](REALTIME.md).

Options:
- -h/--host         Hostname for the GTFS-RT server to listen
- -p/--port         Port for the GTFS-RT server to listen
- -c/--config       Path to the configuration file for the realtime server