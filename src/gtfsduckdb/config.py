class Configuration:

    @classmethod
    def default_config(cls, config):
        # some of the default config keys are commented in order to force
        # the user to provide these configurations actively
        
        default_config = {
            'app': {
                'caching_enabled': False,
                'monitor_enabled': True,
                'cors_enabled': True,
                'mqtt_enabled': True,
                'routing': {
                    'service_alerts_endpoint': '/gtfs/realtime/service-alerts.pbf',
                    'trip_updates_endpoint': '/gtfs/realtime/trip-updates.pbf',
                    'vehicle_positions_endpoint': '/gtfs/realtime/vehicle-positions.pbf',
                    'monitor_endpoint': '/monitor'
                },
                'data_review_seconds': 7200,
                'timezone': 'Europe/Berlin'
            },
            'caching': {
                'caching_server_endpoint': ['MemcachedServerInstance'],
                'caching_service_alerts_ttl_seconds': 60,
                'caching_trip_updates_ttl_seconds': 30,
                'caching_vehicle_positions_ttl_seconds': 15
            },
            'matching': {
                'match_against_first_stop_id': True,
                'match_against_stop_ids': False,
                'remove_invalid_stop_ids': True
            },
            'mqtt': {
                'host': 'test.mosquitto.org',
                'port': 1883,
                'client': 'gtfslake-realtime',
                'keepalive': 60,
                'username': None,
                'password': None,
                'subscriptions': []
            }
        }

        return cls._merge_config(default_config, config)
    
    @classmethod
    def _merge_config(cls, defaults: dict, actual: dict) -> dict:
        if isinstance(defaults, dict) and isinstance(actual, dict):
            return {k: cls._merge_config(defaults.get(k, {}), actual.get(k, {})) for k in set(defaults) | set(actual)}
        
        return actual if actual else defaults