import logging
from .processor import Processor
RAIN_FILTER_FIELDS = ['start_date', 'duration_sec']
TIME_FILTER_FIELDS = ['yearid', 'start_station_code']
MONT_ROYAL_FILTER_FIELDS = ['start_station_code', 'end_station_code']


class TripsProcessor(Processor):
    def __init__(self):
        super().__init__()

    def get_next_processor(self):
        return None

    def process(self, data: bytes):
        msgs = super().get_msgs_filtered(
            data,
            [RAIN_FILTER_FIELDS, TIME_FILTER_FIELDS, MONT_ROYAL_FILTER_FIELDS]
        )

        if len(msgs) == 0:
            return

        logging.info(f'action: trips_processor_processing | result: success | msg_to_rain_filter: {msgs[0]}')
        logging.info(f'action: trips_processor_processing | result: success | msg_to_time_filter: {msgs[1]}')
        logging.info(f'action: trips_processor_processing | result: success | msg_to_mont_royal_filter: {msgs[2]}')

    def end_of_file(self):
        last_msgs = super().get_last_msgs_filtered()

        logging.info(f'action: trips_processor_processing_last | result: success | msg_to_rain_filter: {last_msgs[0]}')
        logging.info(f'action: trips_processor_processing_last | result: success | msg_to_time_filter: {last_msgs[1]}')
        logging.info(f'action: trips_processor_processing_last | result: success | msg_to_mont_royal_filter: {last_msgs[2]}')
