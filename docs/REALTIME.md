# Running a GTFS Realtime Server
Running a GTFS-RT server and matching GTFS-RT streams of various source is one of the core functions of GTFS-DuckDB. To handle different GTFS-RT data streams in realtime, GTFS-DuckDB works with differential GTFS-RT data published using a MQTT broker.

All GTFS-RT data are exposed as protobuf and JSON GTFS-RT streams using the configured endpoints.

## Configuration
For running a GTFS-RT server, you need a configuration YAML file. See [gtfsduckdb-realtime.yaml](/gtfsduckdb-realtime.yaml) for reference.

Using the `app` key, you can enable / disable several features within the GTFS-RT server. For example, there's a simple monitor giving an overview over all trips and their realtime state as well all service alerts which are in the system currently. You also can define custom routes for the GTFS-RT endpoints.

_Note: If a configuration key is not defined explicitly, its default value of [gtfsduckdb-realtime.yaml](/gtfsduckdb-realtime.yaml) will be used._

### Data Review Time
By using the key `data_review_seconds` you can define how long realtime data are kept in the system after their last update.

### Caching
GTFS-RT endpoints can be cached using an external running instance of memcached. To enable caching, the key `app.caching_enabled` needs to be set to `true` and the configuration for `caching` has to be filled with proper values.

### Matching
Matching incoming realtime data against the existing static data has several options explained in the following section:

- **match_against_first_stop_id**: A GTFS-RT trip update is matched using the first stop ID (stop_sequence=1). If this ID differs from possible nominal candidates, the GTFS-RT trip update is discarded
- **match_against_stop_ids**: Like **match_against_first_stop_id**, but more strict. Matches all stop IDs against the possible nominal candidates. If at least one stop ID differs, the GTFS-RT trip update is discarded
- **remove_invalid_stop_ids**: Ignores all stop IDs which are not present in the nominal trip candidate

_Hint: Matching is currently only performed for GTFS-RT trip updates! Other entity types are not matched, but can be used with mapping tables_

### MQTT
GTFS-DuckDB uses differential GTFS-RT data transmitted over a MQTT broker. See [gtfsrt2mqtt](https://github.com/sebastianknopf/gtfsrt2mqtt) for transforming a GTFS-RT stream into MQTT topics. To make GTFS-DuckDB processing the GTFS-RT data, the corresponding topics have to be subscribed at the MQTT broker using the following syntax in the config YAML file:

```yaml
- topic: realtime/sample/service-alerts/#
  type: gtfsrt-service-alerts
  mapping:
      routes: ./routes_mapping.csv
      stops: ./stops_mapping.csv
```

- **`topic`**: The topic name to be subscribed, including MQTT defined wildcards
- **`type`**: Type of data, which are transmitted using this topic. Currently available are `gtfsrt-service-alerts`, `gtfsrt-trip-updates` and `gtfsrt-vehicle-positions` (not implemented yet!)

- **`mapping`**: Optionally you can define mapping tables for routes and stops. By using mapping tables, you have the option to map external IDs to your internal IDs manually which cannot be matched like trip updates. A mapping table needs to be defined in the following format:

```csv
rvs-1-001-1;de:vpe:03001_:
```

The first column is the external ID, the second column is the internal ID. Each external ID can appear only once in a mapping table. All other additional columns are ignored and can be used for internal comments. Please also note, that the mapping table CSV file _does not include headers_ in the first line.

See [routes_mapping.csv](/routes_mapping.csv) and [stops_mapping.csv](/stops_mapping.csv) for reference.

## GTFS-RT Endpoints
The GTFS-RT endpoints can be called using every HTTP capable device and contain a valid protobuf GTFS-RT message in their body. By appending the GET parameter `debug` (e.g. `http://127.0.0.1/gtfs/realtime/service-alerts.pbf?debug`), the output will be streamed as JSON for debugging purposes.

## RSS Endpoint
Optionally, the GTFS-RT service alerts can be streamed as basic RSS feed. This enables users without any social media profile to subscribe the RSS feed and become informed about any incident this way.