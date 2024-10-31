import yaml

from fastapi import APIRouter
from fastapi import FastAPI
from fastapi import Request
from fastapi import Response

from gtfslake.implementation import GtfsLake

class GtfsLakeRealtimeServer:

    def __init__(self, database: str, config_filename: str|None):

		# connect to GTFS lake database
        self._lake = GtfsLake(database)

        # load config and set default values
        if config_filename is not None:
            with open(config_filename, 'r') as config_file:
                self._config = yaml.safe_load(config_file)

        self._config = dict()
        self._config['app'] = dict()
        self._config['app']['caching_enabled'] = False
        self._config['app']['caching_server_endpoint'] = ''
        self._config['app']['caching_server_ttl_seconds'] = ''

        self._config['app']['routing'] = dict()
        self._config['app']['routing']['service_alerts_endpoint'] = '/gtfs/realtime/service-alerts.pbf'
        self._config['app']['routing']['trip_updates_endpoint'] = '/gtfs/realtime/trip-updates.pbf'
        self._config['app']['routing']['vehicle_positions_endpoint'] = '/gtfs/realtime/vehicle-positions.pbf'

        # create routes
        self._fastapi = FastAPI()
        self._api_router = APIRouter()

        self._api_router.add_api_route(self._config['app']['routing']['service_alerts_endpoint'], endpoint=self._service_alerts, methods=['GET'], name='service_alerts')
        self._api_router.add_api_route(self._config['app']['routing']['trip_updates_endpoint'], endpoint=self._trip_updates, methods=['GET'], name='trip_updates')
        self._api_router.add_api_route(self._config['app']['routing']['vehicle_positions_endpoint'], endpoint=self._vehicle_positions, methods=['GET'], name='vehicle_positions')

        # enable chaching if configured
        if 'caching_enabled' in self._config['app'] and self._config['app']['caching_enabled'] == True:
            import memcache

            self._cache = memcache.Client([self._config['caching']['caching_server_endpoint']], debug=0)
            self._cache_ttl = self._config['caching']['caching_server_ttl_seconds']
        else:
            self._cache = None

    async def _service_alerts(self, request: Request) -> Response:
        return 'Test'

    async def _trip_updates(self, request: Request) -> Response:
        return 'Test'

    async def _vehicle_positions(self, request: Request) -> Response:
        return 'Test'

    def create(self):
        self._fastapi.include_router(self._api_router)

        return self._fastapi
