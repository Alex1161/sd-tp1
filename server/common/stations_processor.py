import logging
from .weather_processor import WeatherProcessor
from .processor import Processor
TIME_FILTER_FIELDS = ['code', 'name']
MONT_ROYAL_FILTER_FIELDS = ['code', 'name', 'latitude', 'longitude']
TIME_FILTER = 'time_filter'
MONT_ROYAL_FILTER = 'mont_royal_filter'


class StationsProcessor():
    def __init__(self, connections):
        self._connections = connections
        self._processor = Processor()
        time_filter_channel = self._connections[TIME_FILTER].channel()
        time_filter_channel.queue_declare(queue='task_queue', durable=True)

        mont_royal_filter_channel = self._connections[MONT_ROYAL_FILTER].channel()
        mont_royal_filter_channel.queue_declare(queue='task_queue', durable=True)

        self._queues_filters = [
            [time_filter_channel, TIME_FILTER_FIELDS],
            [mont_royal_filter_channel, MONT_ROYAL_FILTER_FIELDS]
        ]

    def get_next_processor(self):
        return WeatherProcessor(self._connections)

    def process(self, data: bytes):
        msgs = self._processor.process(data, self._queues_filters)

        if len(msgs) == 0:
            return

        logging.debug(f'action: stations_processor_processing | result: success | msg_to_time_filter: {msgs[0]}')
        logging.debug(f'action: stations_processor_processing | result: success | msg_to_mont_royal_filter: {msgs[1]}')

    def end_of_file(self):
        last_msgs = self._processor.end_of_file()

        logging.debug(f'action: stations_processor_processing_last | result: success | msg_to_time_filter: {last_msgs[0]}')
        logging.debug(f'action: stations_processor_processing_last | result: success | msg_to_mont_royal_filter: {last_msgs[1]}')
