import logging
from .weather_processor import WeatherProcessor
from .processor import Processor
TIME_FILTER_FIELDS = ['code', 'name']
MONT_ROYAL_FILTER_FIELDS = ['code', 'name', 'latitude', 'longitude']


class StationsProcessor(Processor):
    def __init__(self):
        super().__init__()

    def get_next_processor(self):
        return WeatherProcessor()

    def process(self, data: bytes):
        msgs = super().get_msgs_filtered(
            data,
            [TIME_FILTER_FIELDS, MONT_ROYAL_FILTER_FIELDS]
        )

        if len(msgs) == 0:
            return

        logging.info(f'action: stations_processor_processing | result: success | msg_to_time_filter: {msgs[0]}')
        logging.info(f'action: stations_processor_processing | result: success | msg_to_mont_royal_filter: {msgs[1]}')

    def end_of_file(self):
        last_msgs = super().get_last_msgs_filtered()

        logging.info(f'action: stations_processor_processing_last | result: success | msg_to_time_filter: {last_msgs[0]}')
        logging.info(f'action: stations_processor_processing_last | result: success | msg_to_mont_royal_filter: {last_msgs[1]}')
