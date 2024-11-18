import logging
import time

from google.transit import gtfs_realtime_pb2
from google.protobuf.message import DecodeError

class GtfsRealtimeAdapter:

    def __init__(self, config, lake, mappings, nominal_trips_ids, nominal_trips_start_times, nominal_trips_intermediate_stops):
        self._lake = lake
        self._config = config
        self._mappings = mappings

        self._nominal_trips_ids = nominal_trips_ids
        self._nominal_trips_start_times = nominal_trips_start_times
        self._nominal_trips_intermediate_stops = nominal_trips_intermediate_stops

    def process_trip_updates(self, topic, payload):
        logger = logging.getLogger('uvicorn')

        try:
            feed_message = gtfs_realtime_pb2.FeedMessage()
            feed_message.ParseFromString(payload)

            # verify that the message is not older than review time in hours
            if feed_message.HasField('header') and feed_message.header.HasField('timestamp'):
                feed_message_timestamp = feed_message.header.timestamp
                
                if (int(time.time()) - feed_message_timestamp) > 60 * 60 * 2:
                    logger.warning(f"Deprecated feed message for topic {topic} discarded")
                    return

            # process all entities of type trip_update
            for entity in feed_message.entity:
                if entity.HasField('trip_update'):

                    # create new message with adapted data
                    matching_feed_message = gtfs_realtime_pb2.FeedMessage()
                    matching_entity = matching_feed_message.entity.add()

                    matching_entity.CopyFrom(entity)

                    # map route and stop IDs
                    route_id = matching_entity.trip_update.trip.route_id if matching_entity.trip_update.HasField('trip') and matching_entity.trip_update.trip.HasField('route_id') else None
                    if 'routes' in self._mappings and route_id in self._mappings['routes']:
                        matching_entity.trip_update.trip.route_id = self._mappings['routes'][route_id]

                    for stop_time_update in matching_entity.trip_update.stop_time_update:
                        stop_id = stop_time_update.stop_id if stop_time_update.HasField('stop_id') else None
                        if 'stops' in self._mappings and stop_id in self._mappings['stops']:
                            stop_time_update.stop_id = self._mappings['stops'][stop_id]

                    # check whether the trip is already known or must be matched
                    if matching_entity.trip_update.trip.trip_id in self._nominal_trips_ids: # if the trip ID is already known from nominal trip IDs
                        logger.info(f"Trip {matching_entity.trip_update.trip.trip_id} found in nominal trips")
                        
                        # add trip update to database
                        self._insert_trip_update(matching_entity)
                        
                    else: # if the trip ID does not exists, start matching here
                        if not matching_entity.trip_update.trip.HasField('start_time'):
                            logger.warning(f"Trip {matching_entity.trip_update.trip.trip_id} as no start_time attribute and cannot be matched")
                            return
                        
                        if matching_entity.trip_update.trip.route_id not in self._nominal_trips_start_times:
                            return
                    
                        if matching_entity.trip_update.trip.start_time not in self._nominal_trips_start_times[matching_entity.trip_update.trip.route_id]:
                            return
                        
                        trip_id_matched = False
                        for candidate in self._nominal_trips_start_times[matching_entity.trip_update.trip.route_id][matching_entity.trip_update.trip.start_time]:
                            matching_entity.id = candidate
                            matching_entity.trip_update.trip.trip_id = candidate

                            # check whether stop time updates match the nominal intermediate stops
                            intermediate_stops_matching = True
                            for stu in matching_entity.trip_update.stop_time_update:
                                if not self._config['matching']['match_against_first_stop_id'] and not self._config['matching']['match_against_stop_ids']:
                                    break

                                if self._config['matching']['match_against_first_stop_id'] and not self._config['matching']['match_against_stop_ids']:
                                    if stu.stop_sequence != 1:
                                        continue

                                if stu.stop_sequence >= len(self._nominal_trips_intermediate_stops[candidate]):
                                    logger.warning(f"Could not match trip {entity.trip_update.trip.trip_id} due to nominal stop number mismatch")
                                    intermediate_stops_matching = False
                                    break

                                stu_index = (stu.stop_sequence - 1)
                                act_id = stu.stop_id
                                nom_id = self._nominal_trips_intermediate_stops[candidate][max(0, stu_index)]
                                if not nom_id == act_id:
                                    logger.warning(f"Could not match trip {entity.trip_update.trip.trip_id} due to an intermediate stop mismatch (nom: seq={stu_index},id={nom_id}; act: seq={stu_index},id={act_id})")
                                    intermediate_stops_matching = False
                                    break

                            if intermediate_stops_matching:
                                trip_id_matched = True
                                break

                        if not trip_id_matched:
                            return
                        
                        logger.info(f"Matched trip {entity.trip_update.trip.trip_id} to nominal trip {matching_entity.trip_update.trip.trip_id}")
                        
                        # add trip update to database
                        self._insert_trip_update(matching_entity)

                else:
                    logger.error(f"Trip update {topic} has no trip descriptor")
                    
        except DecodeError:
            logger.info('DecodeError while processing GTFSRT message')

    def _insert_trip_update(self, entity):
        trip_update_data = dict()
        trip_stop_time_update_data = list()

        trip_update_data['trip_update_id'] = entity.id
        trip_update_data['trip_id'] = entity.trip_update.trip.trip_id if entity.trip_update.HasField('trip') and entity.trip_update.trip.HasField('trip_id') else None
        trip_update_data['trip_route_id'] = entity.trip_update.trip.route_id if entity.trip_update.HasField('trip') and entity.trip_update.trip.HasField('route_id') else None
        trip_update_data['trip_direction_id'] = entity.trip_update.trip.direction_id if entity.trip_update.HasField('trip') and entity.trip_update.trip.HasField('direction_id') else None
        trip_update_data['trip_start_time'] = entity.trip_update.trip.start_time if entity.trip_update.HasField('trip') and entity.trip_update.trip.HasField('start_time') else None
        trip_update_data['trip_start_date'] = entity.trip_update.trip.start_date if entity.trip_update.HasField('trip') and entity.trip_update.trip.HasField('start_date') else None
        trip_update_data['trip_schedule_relationship'] = entity.trip_update.trip.schedule_relationship if entity.trip_update.HasField('trip') and entity.trip_update.trip.HasField('schedule_relationship') else None
        # TODO: implement vehicle data

        for stu in entity.trip_update.stop_time_update:
            stop_time_update_data = dict()

            stop_time_update_data['trip_update_id'] = entity.id
            stop_time_update_data['stop_sequence'] = stu.stop_sequence if stu.HasField('stop_sequence') else None
            stop_time_update_data['stop_id'] = stu.stop_id if stu.HasField('stop_id') else None
            stop_time_update_data['arrival_time'] = stu.arrival.time if stu.HasField('arrival') and stu.arrival.HasField('time') else None
            stop_time_update_data['arrival_delay'] = stu.arrival.delay if stu.HasField('arrival') and stu.arrival.HasField('delay') else None
            stop_time_update_data['arrival_uncertainty'] = stu.arrival.uncertainty if stu.HasField('arrival') and stu.arrival.HasField('uncertainty') else None
            stop_time_update_data['departure_time'] = stu.departure.time if stu.HasField('departure') and stu.departure.HasField('time') else None
            stop_time_update_data['departure_delay'] = stu.departure.delay if stu.HasField('departure') and stu.departure.HasField('delay') else None
            stop_time_update_data['departure_uncertainty'] = stu.departure.uncertainty if stu.HasField('departure') and stu.departure.HasField('uncertainty') else None
            stop_time_update_data['schedule_relationship'] = stu.schedule_relationship if stu.HasField('schedule_relationship') else None

            trip_stop_time_update_data.append(stop_time_update_data)

        self._lake.insert_realtime_trip_updates(trip_update_data, trip_stop_time_update_data)