import logging
from .weather_processor import WeatherProcessor
TIME_FILTER_FIELDS = ['code', 'name']
MONT_ROYAL_FILTER_FIELDS = ['code', 'name', 'latitude', 'longitude']


class StationsProcessor():
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

    def __complete_rows_from_msg(self, msg: str):
        msg = self._rest_last_msg + msg
        rows = msg.split('\n')

        # Separate last element because if it is incomplete
        self._rest_last_msg = rows[-1]
        return rows[:-1]

    def __filter_positions(self, rows: list, positions: list):
        rows = map(lambda x: x.split(','), rows)
        positions_filtered = [
            [elem for i, elem in enumerate(row) if i in positions]
            for row in rows
        ]
        rows_fields_filtered = map(lambda x: ','.join(x), positions_filtered)
        new_msg = '\n'.join(rows_fields_filtered)
        return new_msg

    def process(self, data: bytes):
        msg = data.decode('utf-8')
        if not self._header:
            msg = self.__set_headers(msg)

        rows = self.__complete_rows_from_msg(msg)
        msg_to_time_filter = self.__filter_positions(rows, self._field_positions_to_time_filter)
        msg_to_mont_royal_filter = self.__filter_positions(rows, self._field_positions_to_mont_royal_filter)

        logging.info(f'action: stations_processor_processing | result: success | msg_to_time_filter: {msg_to_time_filter}')
        logging.info(f'action: stations_processor_processing | result: success | msg_to_mont_royal_filter: {msg_to_mont_royal_filter}')

    # To improve, minimizing the last send
    def end_of_file(self):
        rows = [self._rest_last_msg]
        msg_to_time_filter = self.__filter_positions(rows, self._field_positions_to_time_filter)
        msg_to_mont_royal_filter = self.__filter_positions(rows, self._field_positions_to_mont_royal_filter)

        logging.info(f'action: stations_processor_processing_last | result: success | msg_to_time_filter: {msg_to_time_filter}')
        logging.info(f'action: stations_processor_processing_last | result: success | msg_to_mont_royal_filter: {msg_to_mont_royal_filter}')

    def get_next_processor(self):
        return WeatherProcessor()
