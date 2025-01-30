import csv
import json
import logging
import polars as pl
import pytz
import yaml

from contextlib import asynccontextmanager
from datetime import datetime
from fastapi import APIRouter
from fastapi import FastAPI
from fastapi import Request
from fastapi import Response
from fastapi.middleware.cors import CORSMiddleware
from google.transit import gtfs_realtime_pb2
from google.protobuf.message import DecodeError
from google.protobuf.json_format import ParseDict
from math import floor
from paho.mqtt import client
from typing import Any
from threading import Thread

from gtfslake.lake import GtfsLake
from gtfslake.repeatedtimer import RepeatedTimer
from gtfslake.adapter.gtfsrt import GtfsRealtimeAdapter

from gtfslake.__version__ import __version__

class GtfsLakeRealtimeServer:

    def __init__(self, database_filename: str, config_filename: str|None):

        self._database_filename = database_filename

		# connect to GTFS lake database
        self._lake = GtfsLake(database_filename)

        # connect to GTFS lake database a second time for independent writing purposes
        self._lake_mqtt = GtfsLake(database_filename)
        self._lake_mqtt_timer = RepeatedTimer(15, self._execute_realtime_queues)

        # create cache container for nominal trips
        self._nominal_trips = None
        self._nominal_trips_ids = None
        self._nominal_trips_reference = None

        # load config and set default values
        if config_filename is not None:
            with open(config_filename, 'r') as config_file:
                self._config = yaml.safe_load(config_file)
        else:
            self._config = dict()
            self._config['app'] = dict()
            self._config['app']['caching_enabled'] = False
            self._config['app']['monitor_enabled'] = True
            self._config['app']['cors_enabled'] = False
            self._config['app']['mqtt_enabled'] = False

            self._config['app']['routing'] = dict()
            self._config['app']['routing']['service_alerts_endpoint'] = '/gtfs/realtime/service-alerts.pbf'
            self._config['app']['routing']['trip_updates_endpoint'] = '/gtfs/realtime/trip-updates.pbf'
            self._config['app']['routing']['vehicle_positions_endpoint'] = '/gtfs/realtime/vehicle-positions.pbf'
            self._config['app']['routing']['monitor_endpoint'] = '/monitor'

            self._config['app']['data_review_seconds'] = 600
            self._config['app']['timezone'] = 'Europe/Berlin'

            self._config['caching']['caching_server_endpoint'] = ''
            self._config['caching']['caching_service_alerts_ttl_seconds'] = 60
            self._config['caching']['caching_trip_updates_ttl_seconds'] = 30
            self._config['caching']['caching_vehicle_positions_ttl_seconds'] = 15

            self._config['matching']['match_against_first_stop_id'] = True
            self._config['matching']['match_against_stop_ids'] = False

            self._config['mqtt']['host'] = ''
            self._config['mqtt']['port'] = ''
            self._config['mqtt']['client'] = 'gtfslake-realtime-default-client'
            self._config['mqtt']['keepalive'] = 60
            self._config['mqtt']['username'] = None
            self._config['mqtt']['password'] = None
            self._config['mqtt']['subscriptions'] = list()

        # create data notification client
        if self._config['app']['mqtt_enabled']:
            self._mqtt = client.Client(client.CallbackAPIVersion.VERSION2, protocol=client.MQTTv5, client_id=self._config['mqtt']['client'])
            self._mqtt.on_message = self._on_message
            self._mqtt.on_connect = self._on_connect
            self._mqtt.on_disconnect = self._on_disconnect

            self._mqtt_topic_types = dict()
            self._mqtt_topic_mappings = dict()
            for subscription in self._config['mqtt']['subscriptions']:
                self._mqtt_topic_types[subscription['topic']] = subscription['type']

                if 'mapping' in subscription:
                    self._mqtt_topic_mappings[subscription['topic']] = dict()

                    if 'routes' in subscription['mapping']:
                        self._mqtt_topic_mappings[subscription['topic']]['routes'] = self._read_mapping_csv_dict(subscription['mapping']['routes'])
                    
                    if 'stops' in subscription['mapping']:
                        self._mqtt_topic_mappings[subscription['topic']]['stops'] = self._read_mapping_csv_dict(subscription['mapping']['stops'])

        # create routes
        self._fastapi = FastAPI(lifespan=self._lifespan)
        self._api_router = APIRouter()

        self._api_router.add_api_route(self._config['app']['routing']['service_alerts_endpoint'], endpoint=self._service_alerts, methods=['GET'], name='service_alerts')
        self._api_router.add_api_route(self._config['app']['routing']['trip_updates_endpoint'], endpoint=self._trip_updates, methods=['GET'], name='trip_updates')
        self._api_router.add_api_route(self._config['app']['routing']['vehicle_positions_endpoint'], endpoint=self._vehicle_positions, methods=['GET'], name='vehicle_positions')

        if self._config['app']['monitor_enabled']:
            self._api_router.add_api_route(self._config['app']['routing']['monitor_endpoint'], endpoint=self._monitor, methods=['GET'], name='monitor')

        # add CORS features if enabled in config
        if self._config['app']['cors_enabled']:
            self._fastapi.add_middleware(
                CORSMiddleware,
                allow_origins=['*'],
                allow_credentials=True,
                allow_methods=['GET'],
                allow_headers=['*']
            )

        # enable chaching if configured
        if 'caching_enabled' in self._config['app'] and self._config['app']['caching_enabled'] == True:
            import memcache
            self._cache = memcache.Client([self._config['caching']['caching_server_endpoint']], debug=0)
        else:
            self._cache = None

    @asynccontextmanager
    async def _lifespan(self, app):
        logger = logging.getLogger('uvicorn')

        # load nominal data
        self._load_nominal_stops()
        self._load_nominal_routes()
        self._load_nominal_trips(datetime.now())

        # start database realtime insert timer
        self._lake_mqtt_timer.start()

        # delete existing realtime data in order to avoid deprecated data
        # since data are only loaded from MQTT broker as retained messages,
        # they sould be restored after server startup, if they're still valid
        self._lake.clear_realtime_data()

        logger.info('Started realtime data insert timer with interval of 15s')

        if self._config['mqtt']['username'] is not None and self._config['mqtt']['password'] is not None:
            self._mqtt.username_pw_set(username=self._config['mqtt']['username'], password=self._config['mqtt']['password'])

        self._mqtt.connect(self._config['mqtt']['host'], self._config['mqtt']['port'], keepalive=self._config['mqtt']['keepalive'])
        self._mqtt.loop_start()

        logger.info(f"Connected to MQTT {self._config['mqtt']['host']}:{self._config['mqtt']['port']}")

        yield

        self._mqtt.loop_stop()
        self._mqtt.disconnect()

        logger.info(f"Disconnected from MQTT {self._config['mqtt']['host']}:{self._config['mqtt']['port']}")

        # stop database realtime insert timer
        self._lake_mqtt_timer.stop()

        logger.info('Stopped realtime data insert timer')

    def _on_connect(self, client, userdata, flags, rc, properties):
        logger = logging.getLogger('uvicorn')

        if not rc.is_failure:
            # subscribe all desired topics
            for subscription in self._config['mqtt']['subscriptions']:
                logger.info(f"Subscribing topic {subscription['topic']} ({subscription['type']})")
                self._mqtt.subscribe(subscription['topic'])
        else:
            logger.error(f"Failed to connect to MQTT broker with reason: {rc}")

    def _on_message(self, client: client.Client, userdata, message: client.MQTTMessage):   
        
        # process message according to the topic type
        subscription_type = self._get_subscription_type(message.topic)
        if subscription_type == 'gtfsrt-service-alerts':
            adapter = GtfsRealtimeAdapter(self._config, self._lake_mqtt, self._get_subscription_mappings(message.topic))
            adapter.set_nominal_data(self._nominal_stop_ids, self._nominal_route_ids, self._nominal_trips_ids, self._nominal_trips_start_times, self._nominal_trips_intermediate_stops)
            adapter.process_service_alerts(message.topic, message.payload)
        elif subscription_type == 'gtfsrt-trip-updates':
            adapter = GtfsRealtimeAdapter(self._config, self._lake_mqtt, self._get_subscription_mappings(message.topic))
            adapter.set_nominal_data(self._nominal_stop_ids, self._nominal_route_ids, self._nominal_trips_ids, self._nominal_trips_start_times, self._nominal_trips_intermediate_stops)
            adapter.process_trip_updates(message.topic, message.payload)
        elif subscription_type == 'gtfsrt-vehicle-positions':
            adapter = GtfsRealtimeAdapter(self._config, self._lake_mqtt, self._get_subscription_mappings(message.topic))
            adapter.set_nominal_data(self._nominal_stop_ids, self._nominal_route_ids, self._nominal_trips_ids, self._nominal_trips_start_times, self._nominal_trips_intermediate_stops)
            #adapter.process_vehicle_positions(message.topic, message.payload)

    def _on_disconnect(self, client, userdata, flags, rc, properties):
        logger = logging.getLogger('uvicorn')

        # unsubscribe all MQTT topics
        for subscription in self._config['mqtt']['subscriptions']:
            logger.info(f"Unsubscribing topic {subscription['topic']} ({subscription['type']})")
            self._mqtt.unsubscribe(subscription['topic'])

    def _get_subscription_type(self, topic):
        f=lambda s,p:p in(s,'#')or p[:1]in(s[:1],'+')and f(s[1:],p['+'!=p[:1]or(s[:1]in'/')*2:])
        for subscription in self._config['mqtt']['subscriptions']:
            if f(topic, subscription['topic']):
                return subscription['type']
            
        return None
    
    def _get_subscription_mappings(self, topic):
        f=lambda s,p:p in(s,'#')or p[:1]in(s[:1],'+')and f(s[1:],p['+'!=p[:1]or(s[:1]in'/')*2:])
        for subscription in self._config['mqtt']['subscriptions']:
            if f(topic, subscription['topic']) and subscription['topic'] in self._mqtt_topic_mappings:
                return self._mqtt_topic_mappings[subscription['topic']]
            
        return dict()
    
    def _read_mapping_csv_dict(self, csv_filename):
        result = dict()
        
        with open(csv_filename, 'r') as csv_file:
            csv_reader = csv.reader(csv_file, delimiter=';', quotechar='"')
            for row in csv_reader:
                result[row[0]] = row[1]

        return result
    
    def _load_nominal_stops(self):
        logger = logging.getLogger('uvicorn')

        logger.info('Creating nominal stop index ...')
        self._nominal_stop_ids = pl.Series(
            self._lake.fetch_nominal_stops()
            .select('stop_id')
        ).to_list()

    def _load_nominal_routes(self):
        logger = logging.getLogger('uvicorn')

        logger.info('Creating nominal route index ...')
        self._nominal_route_ids = pl.Series(
            self._lake.fetch_nominal_routes()
            .select('route_id')
        ).to_list()

    def _load_nominal_trips(self, dtoday: datetime):
        logger = logging.getLogger('uvicorn')

        reference = dtoday.strftime('%Y%m%d')

        if self._nominal_trips_reference is None or not self._nominal_trips_reference == reference:
            logger.info('Creating nominal trip index ...')

            self._nominal_trips_reference = reference
            self._nominal_trips = self._lake.fetch_nominal_operation_day_trips(dtoday, True)

            self._nominal_trips_ids = pl.Series(self._nominal_trips.select('trip_id').unique()).to_list()
            
            self._nominal_trips_start_times = dict()
            self._nominal_trips_intermediate_stops = dict()
            for nominal_trip in self._nominal_trips.iter_rows(named=True):
                
                # store start times to to trip IDs
                if nominal_trip['route_id'] not in self._nominal_trips_start_times:
                    self._nominal_trips_start_times[nominal_trip['route_id']] = dict()

                if nominal_trip['departure_time'] not in self._nominal_trips_start_times[nominal_trip['route_id']]:
                    self._nominal_trips_start_times[nominal_trip['route_id']][nominal_trip['departure_time']] = list()

                if nominal_trip['stop_sequence'] == 1:
                    self._nominal_trips_start_times[nominal_trip['route_id']][nominal_trip['departure_time']].append(nominal_trip['trip_id'])
                
                # store intermediate stops for each trip                
                if nominal_trip['trip_id'] not in self._nominal_trips_intermediate_stops:
                    self._nominal_trips_intermediate_stops[nominal_trip['trip_id']] = list()

                self._nominal_trips_intermediate_stops[nominal_trip['trip_id']].append(nominal_trip['stop_id'])

            # delete data frame, we won't use it further for performance issues
            self._nominal_trips = None

            logger.info(f"Found {len(self._nominal_trips_ids)} unique trips")
    
    def _execute_realtime_queues(self):
        logger = logging.getLogger('uvicorn')

        logger.info('Executing realtime queues ...')
        self._lake_mqtt._execute_realtime_queues(self._config['app']['data_review_seconds'])

    async def _service_alerts(self, request: Request) -> Response:

        # check whether there're cached data
        format = request.query_params['f'] if 'f' in request.query_params else 'pbf'
        if self._cache is not None:
            cached_response = self._cache.get(f"{request.url.path}-{format}")
            if cached_response is not None:
                if format == 'json':
                    mime_type = 'application/json'
                else:
                    mime_type = 'application/octet-stream'

                return Response(content=cached_response, media_type=mime_type)

        # if there're no data cached, fetch and create them
        service_alerts, alert_active_periods, alert_informed_entities = self._lake.fetch_realtime_service_alerts()

        objects = list()
        for service_alert in service_alerts.iter_rows(named=True):

            obj = dict()
            obj['id'] = service_alert['service_alert_id']

            obj['alert'] = dict()
            obj['alert']['cause'] = service_alert['cause']
            obj['alert']['effect'] = service_alert['effect']

            if service_alert['url'] is not None:
                obj['alert']['url'] = dict()
                obj['alert']['url']['translation'] = list()
                obj['alert']['url']['translation'].append({
                    'text': service_alert['url'],
                    'language': 'de-DE'
                })

            obj['alert']['header_text'] = dict()
            obj['alert']['header_text']['translation'] = list()
            obj['alert']['header_text']['translation'].append({
                'text': service_alert['header_text'],
                'language': 'de-DE'
            })

            if service_alert['tts_header_text'] is not None:
                obj['alert']['tts_header_text'] = dict()
                obj['alert']['tts_header_text']['translation'] = list()
                obj['alert']['tts_header_text']['translation'].append({
                    'text': service_alert['tts_header_text'],
                    'language': 'de-DE'
                })

            obj['alert']['description_text'] = dict()
            obj['alert']['description_text']['translation'] = list()
            obj['alert']['description_text']['translation'].append({
                'text': service_alert['description_text'],
                'language': 'de-DE'
            })

            if service_alert['tts_description_text'] is not None:
                obj['alert']['tts_description_text'] = dict()
                obj['alert']['tts_description_text']['translation'] = list()
                obj['alert']['tts_description_text']['translation'].append({
                    'text': service_alert['tts_description_text'],
                    'language': 'de-DE'
                })

            obj['alert']['active_period'] = list()
            obj['alert']['informed_entity'] = list()

            for active_period in alert_active_periods.filter(pl.col('service_alert_id') == service_alert['service_alert_id']).iter_rows(named=True):
                obj['alert']['active_period'].append({
                    'start': active_period['start_timestamp'],
                    'end': active_period['end_timestamp']
                })

            for informed_entity in alert_informed_entities.filter(pl.col('service_alert_id') == service_alert['service_alert_id']).iter_rows(named=True):
                ie = dict()

                if informed_entity['agency_id'] is not None:
                    ie['agency_id'] = informed_entity['agency_id']

                if informed_entity['route_id'] is not None:
                    ie['route_id'] = informed_entity['route_id']    

                if informed_entity['route_type'] is not None:
                    ie['route_type'] = informed_entity['route_type']

                if informed_entity['stop_id'] is not None:
                    ie['stop_id'] = informed_entity['stop_id']

                # request trip descriptor, if None, there's no trip informed
                trip_descriptor = self._create_trip_descriptor(informed_entity)
                if trip_descriptor is not None:
                    ie['trip'] = trip_descriptor

                obj['alert']['informed_entity'].append(ie)

            objects.append(obj)

        # send response
        feed_message = self._create_feed_message(objects)
        if format  == 'json':
            json_result = json.dumps(feed_message, indent=4)

            if self._cache is not None:
                self._cache.set(f"{request.url.path}-{format}", json_result, self._config['caching']['caching_service_alerts_ttl_seconds'])

            return Response(content=json_result, media_type='application/json')
        else:
            pbf_result = ParseDict(feed_message, gtfs_realtime_pb2.FeedMessage()).SerializeToString()

            if self._cache is not None:
                self._cache.set(f"{request.url.path}-{format}", pbf_result, self._config['caching']['caching_service_alerts_ttl_seconds'])

            return Response(content=pbf_result, media_type='application/octet-stream')

    async def _trip_updates(self, request: Request) -> Response:

        # check whether there're cached data
        format = request.query_params['f'] if 'f' in request.query_params else 'pbf'
        if self._cache is not None:
            cached_response = self._cache.get(f"{request.url.path}-{format}")
            if cached_response is not None:
                if 'f' in request.query_params and request.query_params['f'] == 'json':
                    mime_type = 'application/json'
                else:
                    mime_type = 'application/octet-stream'

                return Response(content=cached_response, media_type=mime_type)

        # if nothing is cached, fetch trip updates
        trip_updates, trip_stop_time_updates = self._lake.fetch_realtime_trip_updates()

        objects = list()
        for trip_update in trip_updates.iter_rows(named=True):
            obj = dict()
            obj['id'] = trip_update['trip_update_id']

            obj['trip_update'] = dict()

            trip_descriptor = self._create_trip_descriptor(trip_update)
            if trip_descriptor is not None:
                obj['trip_update']['trip'] = trip_descriptor

            vehicle_descriptor = self._create_vehicle_descriptor(trip_update)
            if vehicle_descriptor is not None:
                obj['vehicle_descriptor']['vehicle'] = vehicle_descriptor

            obj['trip_update']['stop_time_update'] = list()
            for stop_time_update in trip_stop_time_updates.filter(pl.col('trip_update_id') == trip_update['trip_update_id']).iter_rows(named=True):
                stu = dict()

                if stop_time_update['stop_sequence'] is not None:
                    stu['stop_sequence'] = stop_time_update['stop_sequence']

                if stop_time_update['stop_id'] is not None:
                    stu['stop_id'] = stop_time_update['stop_id']

                # build arrival time update
                if stop_time_update['arrival_time'] is not None or stop_time_update['arrival_delay'] is not None:
                    stu['arrival'] = dict()
                    if stop_time_update['arrival_time'] is not None:
                        stu['arrival']['time'] = stop_time_update['arrival_time']

                    if stop_time_update['arrival_delay'] is not None:
                        stu['arrival']['delay'] = stop_time_update['arrival_delay']

                    if stop_time_update['arrival_uncertainty'] is not None:
                        stu['arrival']['uncertainty'] = stop_time_update['arrival_uncertainty']

                # build departure time update
                if stop_time_update['departure_time'] is not None or stop_time_update['departure_delay'] is not None:
                    stu['departure'] = dict()
                    if stop_time_update['departure_time'] is not None:
                        stu['departure']['time'] = stop_time_update['departure_time']

                    if stop_time_update['departure_delay'] is not None:
                        stu['departure']['delay'] = stop_time_update['departure_delay']

                    if stop_time_update['departure_uncertainty'] is not None:
                        stu['departure']['uncertainty'] = stop_time_update['departure_uncertainty']

                stu['schedule_relationship'] = stop_time_update['schedule_relationship']

                obj['trip_update']['stop_time_update'].append(stu)

            # see #16, some trip updates do obviously not contain some stop time updates and are invalid
            if len(obj['trip_update']['stop_time_update']) > 0:
                objects.append(obj)

        # send response
        feed_message = self._create_feed_message(objects)
        if format  == 'json':
            json_result = json.dumps(feed_message, indent=4)

            if self._cache is not None:
                self._cache.set(f"{request.url.path}-{format}", json_result, self._config['caching']['caching_trip_updates_ttl_seconds'])

            return Response(content=json_result, media_type='application/json')
        else:
            pbf_result = ParseDict(feed_message, gtfs_realtime_pb2.FeedMessage()).SerializeToString()

            if self._cache is not None:
                self._cache.set(f"{request.url.path}-{format}", pbf_result, self._config['caching']['caching_trip_updates_ttl_seconds'])

            return Response(content=pbf_result, media_type='application/octet-stream')


    async def _vehicle_positions(self, request: Request) -> Response:

        # check whether there're cached data
        format = request.query_params['f'] if 'f' in request.query_params else 'pbf'
        if self._cache is not None:
            cached_response = self._cache.get(f"{request.url.path}-{format}")
            if cached_response is not None:
                if 'f' in request.query_params and request.query_params['f'] == 'json':
                    mime_type = 'application/json'
                else:
                    mime_type = 'application/octet-stream'

                return Response(content=cached_response, media_type=mime_type)

        # if nothing is cached, fetch trip updates
        vehicle_positions = self._lake.fetch_realtime_vehicle_positions()

        objects = list()
        for vehicle_position in vehicle_positions.iter_rows(named=True):
            obj = dict()
            obj['id'] = vehicle_position['vehicle_position_id']

            obj['vehicle'] = dict()

            trip_descriptor = self._create_trip_descriptor(vehicle_position)
            if trip_descriptor is not None:
                obj['vehicle']['trip'] = trip_descriptor

            vehicle_descriptor = self._create_vehicle_descriptor(vehicle_position)
            if vehicle_descriptor is not None:
                obj['vehicle']['vehicle'] = vehicle_descriptor

            # extract position attributes
            obj['vehicle']['position'] = dict()
            obj['vehicle']['position']['latitude'] = vehicle_position['position_latitude']
            obj['vehicle']['position']['longitude'] = vehicle_position['position_longitude']

            if vehicle_position['position_bearing'] is not None:
                obj['vehicle']['position']['bearing'] = vehicle_position['position_bearing']

            if vehicle_position['position_odometer'] is not None:
                obj['vehicle']['position']['odometer'] = vehicle_position['position_odometer']

            if vehicle_position['position_speed'] is not None:
                obj['vehicle']['position']['speed'] = vehicle_position['position_speed']

            # extract remaining vehicle position parameters
            if vehicle_position['current_stop_sequence'] is not None:
                obj['vehicle']['current_stop_sequence'] = vehicle_position['current_stop_sequence']

            if vehicle_position['stop_id'] is not None:
                obj['vehicle']['stop_id'] = vehicle_position['stop_id']

            if vehicle_position['current_status'] is not None:
                obj['vehicle']['current_status'] = vehicle_position['current_status']

            if vehicle_position['timestamp'] is not None:
                obj['vehicle']['timestamp'] = vehicle_position['timestamp']

            if vehicle_position['congestion_level'] is not None:
                obj['vehicle']['congestion_level'] = vehicle_position['congestion_level']

            objects.append(obj)

        # send response
        feed_message = self._create_feed_message(objects)
        if format  == 'json':
            json_result = json.dumps(feed_message, indent=4)

            if self._cache is not None:
                self._cache.set(f"{request.url.path}-{format}", json_result, self._config['caching']['caching_vehicle_positions_ttl_seconds'])

            return Response(content=json_result, media_type='application/json')
        else:
            pbf_result = ParseDict(feed_message, gtfs_realtime_pb2.FeedMessage()).SerializeToString()

            if self._cache is not None:
                self._cache.set(f"{request.url.path}-{format}", pbf_result, self._config['caching']['caching_vehicle_positions_ttl_seconds'])

            return Response(content=pbf_result, media_type='application/octet-stream')
        
    async def _monitor(self, request: Request) -> Response:

        # read request GET params
        format = request.query_params['f'] if 'f' in request.query_params else 'html'
        date = request.query_params['d'] if 'd' in request.query_params else None
        realtime = True if 'r' in request.query_params else False
        line = request.query_params['l'] if 'l' in request.query_params else None
        
        # create reference date
        reference = datetime.now()
        if date is not None:
            try:
                reference = datetime.strptime(date, '%Y%m%d')
            except ValueError:
                pass

        # fetch data
        alerts = self._lake.fetch_realtime_monitor_alerts()
        trips = self._lake.fetch_realtime_operation_day_monitor_trips(reference)

        # apply filters here ...
        if realtime:
            trips = trips.filter(pl.col('realtime_available') == 'true')

        if line is not None:
            trips = trips.filter(pl.col('route_id') == line)

        # return results
        if format == 'json':
            json_object = {
                'alerts': alerts.to_dicts(),
                'trips': trips.to_dicts()
            }

            return Response(content=json.dumps(json_object, indent=4, default=str), media_type='application/json')
        else:
            
             # generate viewable alerts HTML table
            alerts_table = '<table width="100%" cellpadding="4" cellspacing="2" border="1"><thead style="font-weight:bold"><tr><td>Entity ID</td><td>Cause</td><td>Effect</td><td>Header</td><td>Description</td></tr></thead><tbody>'

            for alert in alerts.iter_rows(named=True):
                alerts_table = alerts_table + f"<tr><td>{alert['service_alert_id']}</td><td>{alert['cause']}</td><td>{alert['effect']}</td><td>{alert['header_text']}</td><td>{alert['description_text']}</td></tr>"

            alerts_table = alerts_table + '</tbody></table>'

            # generate viewable trips HTML table
            trips_table = '<table width="100%" cellpadding="4" cellspacing="2" border="1"><thead style="font-weight:bold"><tr><td>OperationDay</td><td>AgencyID</td><td>RouteID</td><td>RouteShortName</td><td>TripID</td><td>DirectionID</td><td>StartTime</td><td>StartStopID</td><td>StartStopName</td><td>TripHeadsign</td><td>RealtimeAvailable</td><td>RealtimeLastUpdate</td></tr></thead><tbody>'

            for trip in trips.iter_rows(named=True):
                if trip['realtime_available']:
                    style = 'style="background:green"'
                else:
                    style = 'style="background:red"'
                
                trips_table = trips_table + f"<tr><td>{trip['operation_day']}</td><td>{trip['agency_id']}</td><td>{trip['route_id']}</td><td>{trip['route_short_name']}</td><td>{trip['trip_id']}</td><td>{trip['direction_id']}</td><td>{trip['start_time']}</td><td>{trip['start_stop_id']}</td><td>{trip['start_stop_name']}</td><td>{trip['trip_headsign']}</td><td {style}>{trip['realtime_available']}</td><td>{trip['realtime_last_update']}</td></tr>"
                
            trips_table = trips_table + '</tbody></table>'

            # wrap with very basic html
            html = f"""
            <!DOCTYPE html>
            <html>
                <head>
                    <meta charset="UTF-8" />
                </head>
                <body>
                    <h1>gtfslake realtime server (version {__version__})</h1>
                    <h2>Alerts</h2>
                    {alerts_table}
                    <h2>Trips</h2>
                    {trips_table}
                </body>
            </html>
            """
            
            return Response(content=html, media_type='text/html')

    def _create_feed_message(self, entities):
        timestamp = datetime.now().astimezone(pytz.timezone(self._config['app']['timezone'])).timestamp()
        timestamp = floor(timestamp)
        
        return {
            'header': {
                'gtfs_realtime_version': '2.0',
                'incrementality': 'FULL_DATASET',
                'timestamp': timestamp
            },
            'entity': entities
        }

    def _create_trip_descriptor(self, input):
        trip_descriptor_fields = [
            'trip_id', 'trip_route_id', 'trip_direction_id', 'trip_start_time', 'trip_start_date', 'trip_schedule_relationship'
        ]

        if not all(e is None for e in list(input[k] for k in trip_descriptor_fields)):
            trip_descriptor = dict()

            if 'trip_id' in input.keys() and input['trip_id'] is not None:
                trip_descriptor['trip_id'] = input['trip_id']

            if 'trip_route_id' in input.keys() and input['trip_route_id'] is not None:
                trip_descriptor['route_id'] = input['trip_route_id']

            if 'trip_direction_id' in input.keys() and input['trip_direction_id'] is not None:
                trip_descriptor['direction_id'] = input['trip_direction_id']

            if 'trip_start_time' in input.keys() and input['trip_start_time'] is not None:
                trip_descriptor['start_time'] = input['trip_start_time']

            if 'trip_start_date' in input.keys() and input['trip_start_date'] is not None:
                trip_descriptor['start_date'] = input['trip_start_date']

            if 'trip_schedule_relationship' in input.keys() and input['trip_schedule_relationship'] is not None:
                trip_descriptor['schedule_relationship'] = input['trip_schedule_relationship']

            return trip_descriptor
        else:
            return None

    def _create_vehicle_descriptor(self, input):
        vehicle_descriptor_fields = [
            'vehicle_id', 'vehicle_label', 'vehicle_license_plate', 'vehicle_wheelchair_accessible'
        ]

        if not all(e is None for e in list(input[k] for k in vehicle_descriptor_fields)):
            vehicle_descriptor = dict()

            if 'vehicle_id' in input.keys() and input['vehicle_id'] is not None:
                vehicle_descriptor['id'] = input['vehicle_id']

            if 'vehicle_label' in input.keys() and input['vehicle_label'] is not None:
                vehicle_descriptor['label'] = input['vehicle_label']

            if 'vehicle_license_plate' in input.keys() and input['vehicle_license_plate'] is not None:
                vehicle_descriptor['license_plate'] = input['vehicle_license_plate']

            if 'vehicle_wheelchair_accessible' in input.keys() and input['vehicle_wheelchair_accessible'] is not None:
                vehicle_descriptor['wheelchair_accessible'] = input['vehicle_wheelchair_accessible']

            return vehicle_descriptor
        else:
            return None

    def create(self):
        self._fastapi.include_router(self._api_router)

        return self._fastapi
