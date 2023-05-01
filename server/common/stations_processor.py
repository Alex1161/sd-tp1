import logging
from .weather_processor import WeatherProcessor
from .processor import Processor
TIME_FILTER_FIELDS = ['code', 'name']
MONT_ROYAL_FILTER_FIELDS = ['code', 'name', 'latitude', 'longitude']


class StationsProcessor(Processor):
    def __init__(self):
        self._header = None
        self._rest_last_msg = ''

    def __set_headers(self, msg: str):
        header, rest = msg.split('\n', 1)
        self._header = header
        self._field_positions_to_time_filter = [
            index for index,
            field in enumerate(self._header.split(','))
            if field in TIME_FILTER_FIELDS
        ]

        self._field_positions_to_mont_royal_filter = [
            index for index,
            field in enumerate(self._header.split(','))
            if field in MONT_ROYAL_FILTER_FIELDS
        ]

        return rest

    def process(self, data: bytes):
        msg = data.decode('utf-8')
        if not self._header:
            msg = self.__set_headers(msg)

        rows = super().complete_rows_from_msg(msg)
        if len(rows) == 0:
            return

        msg_to_time_filter = super().filter_positions(rows, self._field_positions_to_time_filter)
        msg_to_mont_royal_filter = super().filter_positions(rows, self._field_positions_to_mont_royal_filter)

        logging.info(f'action: stations_processor_processing | result: success | msg_to_time_filter: {msg_to_time_filter}')
        logging.info(f'action: stations_processor_processing | result: success | msg_to_mont_royal_filter: {msg_to_mont_royal_filter}')

    # To improve, minimizing the last send
    def end_of_file(self):
        rows = [self._rest_last_msg]
        msg_to_time_filter = super().filter_positions(rows, self._field_positions_to_time_filter)
        msg_to_mont_royal_filter = super().filter_positions(rows, self._field_positions_to_mont_royal_filter)

        logging.info(f'action: stations_processor_processing_last | result: success | msg_to_time_filter: {msg_to_time_filter}')
        logging.info(f'action: stations_processor_processing_last | result: success | msg_to_mont_royal_filter: {msg_to_mont_royal_filter}')

        self._header = None
        self._rest_last_msg = ''

    def get_next_processor(self):
        return WeatherProcessor()
