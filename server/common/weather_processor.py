import logging
from .trips_processor import TripsProcessor
from .processor import Processor
RAIN_FILTER_FIELDS = ['date', 'prectot']


class WeatherProcessor(Processor):
    def __init__(self):
        super().__init__()

    def get_next_processor(self):
        return TripsProcessor()

    def process(self, data: bytes):
        msgs = super().get_msgs_filtered(
            data,
            [RAIN_FILTER_FIELDS]
        )

        if len(msgs) == 0:
            return

        logging.info(f'action: weather_processor_processing | result: success | msg_to_rain_filter: {msgs[0]}')

    def end_of_file(self):
        last_msgs = super().get_last_msgs_filtered()

        logging.info(f'action: weather_processor_processing_last | result: success | msg_to_rain_filter: {last_msgs[0]}')
