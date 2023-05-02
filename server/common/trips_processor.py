import logging
from .processor import Processor
RAIN_FILTER_FIELDS = ['start_date', 'duration_sec']
TIME_FILTER_FIELDS = ['yearid', 'start_station_code']
MONT_ROYAL_FILTER_FIELDS = ['start_station_code', 'end_station_code']
RAIN_FILTER = 'rain_filter'
TIME_FILTER = 'time_filter'
MONT_ROYAL_FILTER = 'mont_royal_filter'


class TripsProcessor():
    def __init__(self, connections):
        self._connections = connections
        self._processor = Processor()

        rain_filter_channel = self._connections[RAIN_FILTER].channel()
        rain_filter_channel.queue_declare(queue='task_queue', durable=True)

        time_filter_channel = self._connections[TIME_FILTER].channel()
        time_filter_channel.queue_declare(queue='task_queue', durable=True)

        mont_royal_filter_channel = self._connections[MONT_ROYAL_FILTER].channel()
        mont_royal_filter_channel.queue_declare(queue='task_queue', durable=True)

        self._queues_filters = [
            [rain_filter_channel, RAIN_FILTER_FIELDS],
            [time_filter_channel, TIME_FILTER_FIELDS],
            [mont_royal_filter_channel, MONT_ROYAL_FILTER_FIELDS]
        ]

    def get_next_processor(self):
        return None

    def process(self, data: bytes):
        msgs = self._processor.process(data, self._queues_filters)

        # Atipic case, when the last msg is a chunk of the last line
        if len(msgs) == 0:
            return

        logging.debug(f'action: trips_processor_processing | result: success | msg_to_rain_filter: {msgs[0]}')
        logging.debug(f'action: trips_processor_processing | result: success | msg_to_time_filter: {msgs[1]}')
        logging.debug(f'action: trips_processor_processing | result: success | msg_to_mont_royal_filter: {msgs[2]}')

    def end_of_file(self):
        last_msgs = self._processor.end_of_file()

        logging.debug(f'action: trips_processor_processing_last | result: success | msg_to_rain_filter: {last_msgs[0]}')
        logging.debug(f'action: trips_processor_processing_last | result: success | msg_to_time_filter: {last_msgs[1]}')
        logging.debug(f'action: trips_processor_processing_last | result: success | msg_to_mont_royal_filter: {last_msgs[2]}')
