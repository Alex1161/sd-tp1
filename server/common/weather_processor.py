import logging
from .trips_processor import TripsProcessor
from .processor import Processor
RAIN_FILTER_FIELDS = ['date', 'prectot']
RAIN_FILTER = 'rain_filter'


class WeatherProcessor():
    def __init__(self, connections):
        self._connections = connections
        self._processor = Processor()

        rain_filter_channel = self._connections[RAIN_FILTER].channel()
        rain_filter_channel.queue_declare(queue='task_queue', durable=True)

        self._queues_filters = [[rain_filter_channel, RAIN_FILTER_FIELDS]]

    def get_next_processor(self):
        return TripsProcessor(self._connections)

    def process(self, data: bytes):
        msgs = self._processor.process(data, self._queues_filters)

        # Atipic case, when the last msg is a chunk of the last line
        if len(msgs) == 0:
            return

        logging.debug(f'action: weather_processor_processing | result: success | msg_to_rain_filter: {msgs[0]}')

    def end_of_file(self):
        last_msgs = self._processor.end_of_file()

        logging.debug(f'action: weather_processor_processing_last | result: success | msg_to_rain_filter: {last_msgs[0]}')
