import logging
from .trips_processor import TripsProcessor
from .processor import Processor
RAIN_FILTER_FIELDS = ['date', 'prectot']


class WeatherProcessor(Processor):
    def __init__(self):
        self._header = None
        self._rest_last_msg = ''

    def __set_headers(self, msg: str):
        header, rest = msg.split('\n', 1)
        self._header = header
        self._field_positions_to_rain_filter = [
            index for index,
            field in enumerate(self._header.split(','))
            if field in RAIN_FILTER_FIELDS
        ]

        return rest

    def get_next_processor(self):
        return TripsProcessor()

    def process(self, data: bytes):
        msg = data.decode('utf-8')
        if not self._header:
            msg = self.__set_headers(msg)

        rows = super().complete_rows_from_msg(msg)
        # Atipic case when the last chunk in a piece of the last line
        if len(rows) == 0:
            return

        msg_to_rain_filter = super().filter_positions(rows, self._field_positions_to_rain_filter)

        logging.info(f'action: weather_processor_processing | result: success | msg_to_rain_filter: {msg_to_rain_filter}')

    # To improve, minimizing the last send
    def end_of_file(self):
        rows = [self._rest_last_msg]
        msg_to_rain_filter = super().filter_positions(rows, self._field_positions_to_rain_filter)

        logging.info(f'action: weather_processor_processing_last | result: success | msg_to_time_filter: {msg_to_rain_filter}')

        self._header = None
        self._rest_last_msg = ''
