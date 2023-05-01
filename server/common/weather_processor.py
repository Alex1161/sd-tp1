import logging
from .trips_processor import TripsProcessor


class WeatherProcessor():
    def process(self, data: bytes):
        msg = data.decode('utf-8')
        logging.info(f'action: weather_processor_processing | result: success | msg: {msg}')

    def end_of_file(self):
        return

    def get_next_processor(self):
        return TripsProcessor()
