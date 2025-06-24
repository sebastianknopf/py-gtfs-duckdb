# GTFS-DuckDB
This package provides several python utilities to read, aggregate, merge, modify and export [GTFS](https://gtfs.org/) compliant static and realtime data. The package works without any database server using a [DuckDB](https://duckdb.org/).

## Core Functionaliy
GTFS-DuckDB supports several core functionalities, described as follows:

- Load and merge ("drop") GTFS static feeds: Several GTFS static feeds can be loaded and merged using different [strategies](docs/STRATEGIES.md). Even huge GTFS feeds (e.g. whole Baden-Württemberg) are working properly.
- Remove unwanted agencies, routes or trips and their related objects
- Execute SQL statements on the GTFS data directly
- Export modified and merged GTFS datasets
- Grabbing up realtime data and match them against the loaded static data. Matched realtime data are exposed with GTFS-RT endpoints providing a GTFS-RT server.

The functionalities are used in production and also tested in a working GoogleMaps integration.

See [commands cheat-sheet](docs/COMMANDS.md) for reference how to use the GTFS-DuckDB.

## Installation
You can use the GTFS-DuckDB by cloning this repository and install it into your virtual environment directly:
```bash
git clone https://github.com/sebastianknopf/py-gtfs-duckdb.git
cd py-gtfs-duckdb

pip install .
```
and run it by using
```bash
python -m gtfsduckdb {command} {arguments} [options]
```

## License
This project is licensed under the Apache License. See [LICENSE.md](LICENSE.md) for more information.