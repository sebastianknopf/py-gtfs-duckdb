import logging
import time

from google.transit import gtfs_realtime_pb2
from google.protobuf.message import DecodeError

class GtfsRealtimeAdapter:

    def __init__(self, config, lake, mappings):
        self._lake = lake
        self._config = config
        self._mappings = mappings

    def set_nominal_data(self, stop_ids, route_ids, trip_ids, trip_start_times, trips_intermediate_stops):
        self._nominal_stop_ids = stop_ids
        self._nominal_route_ids = route_ids
        self._nominal_trips_ids = trip_ids
        self._nominal_trips_start_times = trip_start_times
        self._nominal_trips_intermediate_stops = trips_intermediate_stops

    def process_service_alerts(self, topic, payload):
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

            # process all entities of type alert
            for entity in feed_message.entity:
                if entity.HasField('alert'):
                    matching_feed_message = gtfs_realtime_pb2.FeedMessage()
                    matching_entity = matching_feed_message.entity.add()

                    matching_entity.CopyFrom(entity)

                    # check whether the entity is marked to be deleted actively
                    # proceed, if the entity is not going to be deleted
                    if matching_entity.HasField('is_deleted') and matching_entity.is_deleted:
                        logger.info(f"Deleted existing service alert {matching_entity.id} of service alerts")
                        self._delete_service_alert(entity)
                    else:
                        # map route and stop IDs
                        # check, whether the mapped IDs are contained in nominal ID list
                        # if there's no informed entity, remove the entire entity selector
                        # in general, only alerts with at least one valid entity selector should be imported
                        deleted_entity_selectors = list()
                        for i in range(0, len(matching_entity.alert.informed_entity)):
                            # map and verify route_id
                            if matching_entity.alert.informed_entity[i].HasField('route_id'):
                                route_id = matching_entity.alert.informed_entity[i].route_id
                                if 'routes' in self._mappings and route_id in self._mappings['routes']:
                                    matching_entity.alert.informed_entity[i].route_id = self._mappings['routes'][route_id]

                                if matching_entity.alert.informed_entity[i].route_id not in self._nominal_route_ids:
                                    matching_entity.alert.informed_entity[i].ClearField('route_id')

                            # map and verify stop_id
                            if matching_entity.alert.informed_entity[i].HasField('stop_id'):
                                stop_id = matching_entity.alert.informed_entity[i].stop_id
                                if 'stops' in self._mappings and stop_id in self._mappings['stops']:
                                    matching_entity.alert.informed_entity[i].stop_id = self._mappings['stops'][stop_id]
                    
                                if matching_entity.alert.informed_entity[i].stop_id not in self._nominal_stop_ids:
                                    matching_entity.alert.informed_entity[i].ClearField('stop_id')

                            # TODO: what about the other fields of an entity selector ... ?

                            # mark entity selector as deleted, it contains no valid reference
                            if not matching_entity.alert.informed_entity[i].HasField('route_id') and not matching_entity.alert.informed_entity[i].HasField('route_id'):
                                deleted_entity_selectors.append(i)

                        # finally, delete all invalid entity selectors
                        for d in deleted_entity_selectors:
                            logger.warning(f"Removed entity selector [{d}] from service alert {matching_entity.id} as it contains no valid references")
                            del matching_entity.alert.informed_entity[d]

                        # if there's no entity selector left, skip this alert
                        # we don't have any relation to an object in database
                        if len(matching_entity.alert.informed_entity) == 0:
                            logger.warning(f"Service alert {matching_entity.id} discarded as it contains no valid references")
                            continue

                        # if everything is okay until here, insert the alert
                        logger.info(f"Added service alert {matching_entity.id} to service alerts")
                        self._insert_service_alert(matching_entity)
                            
        except DecodeError:
            logger.info('DecodeError while processing GTFSRT message')

    def _insert_service_alert(self, entity):
        service_alert, alert_active_periods, alert_informed_enities = self._transform_service_alert(entity)
        self._lake.insert_realtime_service_alert(service_alert, alert_active_periods, alert_informed_enities)

    def _delete_service_alert(self, entity):
        service_alert, alert_active_periods, alert_informed_enities = self._transform_service_alert(entity)
        self._lake.delete_realtime_service_alert(service_alert, alert_active_periods, alert_informed_enities)

    def _transform_service_alert(self, entity):
        service_alert_data = dict()
        alert_active_period_data = list()
        alert_informed_entity_data = list()

        service_alert_data['service_alert_id'] = entity.id
        service_alert_data['cause'] = gtfs_realtime_pb2.Alert.Cause.Name(entity.alert.cause)
        service_alert_data['effect'] = gtfs_realtime_pb2.Alert.Effect.Name(entity.alert.effect)
        service_alert_data['url'] = self._extract_translation_value(entity.alert.url.translation)
        service_alert_data['header_text'] = self._extract_translation_value(entity.alert.header_text.translation)
        service_alert_data['description_text'] = self._extract_translation_value(entity.alert.description_text.translation)
        service_alert_data['tts_header_text'] = self._extract_translation_value(entity.alert.tts_header_text.translation)
        service_alert_data['tts_description_text'] = self._extract_translation_value(entity.alert.tts_description_text.translation)
        service_alert_data['severity_level'] = entity.alert.severity_level if entity.alert.HasField('severity_level') else 'UNKNOWN_SEVERITY'

        for ap in entity.alert.active_period:
            active_period_data = dict()

            active_period_data['service_alert_id'] = entity.id
            active_period_data['start_timestamp'] = ap.start if ap.HasField('start') else None
            active_period_data['end_timestamp'] = ap.end if ap.HasField('end') else None
        
            alert_active_period_data.append(active_period_data)

        for ie in entity.alert.informed_entity:
            informed_entity_data = dict()

            # TODO: implement all other fields of an entity selector here
            informed_entity_data['service_alert_id'] = entity.id
            informed_entity_data['route_id'] = ie.route_id if ie.HasField('route_id') else None
            informed_entity_data['stop_id'] = ie.stop_id if ie.HasField('stop_id') else None

            alert_informed_entity_data.append(informed_entity_data)

        return (service_alert_data, alert_active_period_data, alert_informed_entity_data)
    
    def _extract_translation_value(self, translation_list, lang='de'):
        for item in translation_list:
            if item.language == lang:
                return item.text
            
        if len(translation_list) > 0:
            return translation_list[0].text
        else:
            return None
    
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
                        
                        if matching_entity.HasField('is_deleted') and matching_entity.is_deleted:
                            # remove trip update from database
                            logger.info(f"Deleted existing trip {matching_entity.trip_update.trip.trip_id} of nominal trips")
                            self._delete_trip_update(matching_entity)
                        else:
                            # add trip update to database
                            logger.info(f"Added existing trip {matching_entity.trip_update.trip.trip_id} of nominal trips")
                            self._insert_trip_update(matching_entity)
                        
                    else: # if the trip ID does not exists, start matching here
                        if not matching_entity.trip_update.trip.HasField('start_time'):
                            logger.warning(f"Trip {matching_entity.trip_update.trip.trip_id} has no start_time attribute and cannot be matched")
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
                            deleted_stop_time_updates = list()
                            for i, stu in enumerate(matching_entity.trip_update.stop_time_update):
                                if not self._config['matching']['match_against_first_stop_id'] and not self._config['matching']['match_against_stop_ids'] and not self._config['matching']['remove_invalid_stop_ids']:
                                    break

                                if self._config['matching']['match_against_first_stop_id'] and not (self._config['matching']['match_against_stop_ids'] or self._config['matching']['remove_invalid_stop_ids']):
                                    if stu.stop_sequence != 1:
                                        continue

                                if stu.stop_sequence > len(self._nominal_trips_intermediate_stops[candidate]):
                                    logger.warning(f"Could not match trip {entity.trip_update.trip.trip_id} due to nominal stop number mismatch")
                                    intermediate_stops_matching = False
                                    break

                                stu_index = (stu.stop_sequence - 1)
                                act_id = stu.stop_id
                                nom_id = self._nominal_trips_intermediate_stops[candidate][max(0, stu_index)]
                                if not nom_id == act_id:
                                    if self._config['matching']['remove_invalid_stop_ids']:
                                        logger.warning(f"Discarded stop time update for trip {entity.trip_update.trip.trip_id} due to mismatch (nom: seq={stu_index},id={nom_id}; act: seq={stu_index},id={act_id})")
                                        deleted_stop_time_updates.append(i)

                                    if self._config['matching']['match_against_stop_ids']:
                                        logger.warning(f"Could not match trip {entity.trip_update.trip.trip_id} due to an intermediate stop mismatch (nom: seq={stu_index},id={nom_id}; act: seq={stu_index},id={act_id})")
                                        intermediate_stops_matching = False
                                        break

                            # finally delete discarded stop time updates
                            for d in sorted(deleted_stop_time_updates, reverse=True):
                                del matching_entity.trip_update.stop_time_update[d]

                            # if all intermediate stops matching, the whole trip matches
                            if intermediate_stops_matching:
                                trip_id_matched = True
                                break

                        if not trip_id_matched:
                            return
              
                        if matching_entity.HasField('is_deleted') and matching_entity.is_deleted:
                            # remove trip update from database
                            logger.info(f"Deleted trip {entity.trip_update.trip.trip_id} matching to nominal trip {matching_entity.trip_update.trip.trip_id}")
                            self._delete_trip_update(matching_entity)
                        else:
                            # add trip update to database
                            logger.info(f"Added trip {entity.trip_update.trip.trip_id} matching to nominal trip {matching_entity.trip_update.trip.trip_id}")
                            self._insert_trip_update(matching_entity)

                else:
                    logger.error(f"Trip update {topic} has no trip descriptor")
                    
        except DecodeError:
            logger.info('DecodeError while processing GTFSRT message')

    def _insert_trip_update(self, entity):
        trip_update_data, trip_stop_time_update_data = self._transform_trip_update(entity)
        self._lake.insert_realtime_trip_updates(trip_update_data, trip_stop_time_update_data)

    def _delete_trip_update(self, entity):
        trip_update_data, trip_stop_time_update_data = self._transform_trip_update(entity)
        self._lake.delete_realtime_trip_updates(trip_update_data, trip_stop_time_update_data)

    def _transform_trip_update(self, entity):
        trip_update_data = dict()
        trip_stop_time_update_data = list()

        trip_update_data['trip_update_id'] = entity.id
        trip_update_data['trip_id'] = entity.trip_update.trip.trip_id if entity.trip_update.HasField('trip') and entity.trip_update.trip.HasField('trip_id') else None
        trip_update_data['trip_route_id'] = entity.trip_update.trip.route_id if entity.trip_update.HasField('trip') and entity.trip_update.trip.HasField('route_id') else None
        trip_update_data['trip_direction_id'] = entity.trip_update.trip.direction_id if entity.trip_update.HasField('trip') and entity.trip_update.trip.HasField('direction_id') else None
        trip_update_data['trip_start_time'] = entity.trip_update.trip.start_time if entity.trip_update.HasField('trip') and entity.trip_update.trip.HasField('start_time') else None
        trip_update_data['trip_start_date'] = entity.trip_update.trip.start_date if entity.trip_update.HasField('trip') and entity.trip_update.trip.HasField('start_date') else None
        trip_update_data['trip_schedule_relationship'] = gtfs_realtime_pb2.TripDescriptor.ScheduleRelationship.Name(entity.trip_update.trip.schedule_relationship) if entity.trip_update.HasField('trip') and entity.trip_update.trip.HasField('schedule_relationship') else None
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
            stop_time_update_data['schedule_relationship'] = gtfs_realtime_pb2.TripUpdate.StopTimeUpdate.ScheduleRelationship.Name(stu.schedule_relationship) if stu.HasField('schedule_relationship') else None

            trip_stop_time_update_data.append(stop_time_update_data)

        return (trip_update_data, trip_stop_time_update_data)
