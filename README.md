# GTFS-DuckDB
This package provides several python utilities to read, aggregate, merge, modify and export [GTFS](https://gtfs.org/) compliant static and realtime data. The package works without any database server using a [DuckDB](https://duckdb.org/).

## Core Functionaliy
GTFS-DuckDB supports several core functionalities, described as follows:

- Load and merge ("drop") GTFS static feeds: Several GTFS static feeds can be loaded and merged using different strategies. Even huge GTFS feeds (e.g. whole Baden-Württemberg) are working properly.
- Remove unwanted agencies, routes or trips and their related objects
- Execute SQL statements on the GTFS data directly
- Export modified and merged GTFS datasets
- **Running a GTFS-RT server grabbing realtime data of several different sources**

The functionalities are used in production and also tested in a working GoogleMaps integration.

## Installation
You can use the GTFS-DuckDB by cloning this repository and install it into your virtual environment directly:
```bash
git clone https://github.com/sebastianknopf/py-gtfs-duckdb.git
cd py-gtfs-duckdb

pip install .
```

See [commands cheat-sheet](docs/COMMANDS.md) for reference how to use the GTFS-DuckDB.

## Realtime Server
GTFS-DuckDB can act as a GTFS-RT server and is used in production environments for serving GoogleMaps.

See [realtime configuration](docs/REALTIME.md) for more information about running a realtime server.

## License
This project is licensed under the Apache License. See [LICENSE.md](LICENSE.md) for more information.