import pika
import logging
import pandas as pd
from datetime import datetime, timedelta
EOF = b'EOF'
EOD = b'EOD'
LOADING = 0
PROCESSING = 1
COLUMNS_WEATHER = ['date', 'prectot']
COLUMNS_TRIPS = ['start_date', 'duration_sec']


class RainFilter():
    def __init__(self):
        self._state = LOADING
        self._weather = pd.DataFrame(columns=COLUMNS_WEATHER)

        self._connection = pika.BlockingConnection(
            pika.ConnectionParameters(host='weather_queue'))
        self._channel = self._connection.channel()

        self._channel.queue_declare(queue='task_queue', durable=True)

    def _persist(self, rows):
        for row in rows:
            self._weather.loc[len(self._weather)] = row

    def filter_msg(self, ch, method, properties, body):
        ch.basic_ack(delivery_tag=method.delivery_tag)
        if self._state == LOADING:
            if body == EOF:
                self._state = PROCESSING
                self._weather['date'] = pd.to_datetime(self._weather['date'])
                logging.info(f'action: EOF_detected | result: success | data: {self._weather}')
                return

            rows_str = body.decode('utf-8')
            rows = [row.split(',') for row in rows_str.split('\n')]
            rows_filtered = filter(lambda x: float(x[1]) > 0.3, rows)
            rows_date_cured = list(map(
                lambda row: [(datetime.strptime(row[0], "%Y-%m-%d") - timedelta(days=1)).date(), row[1]],
                rows_filtered
            ))

            self._persist(rows_date_cured)

            logging.debug(f'action: rain_filter_loading | result: success | loaded: {body}')
        elif self._state == PROCESSING:
            if body == EOF:
                logging.info(f'action: EOF_detected | result: success')
                return

            rows_str = body.decode('utf-8')
            rows = [row.split(',') for row in rows_str.split('\n')]
            trips = pd.DataFrame(rows, columns=COLUMNS_TRIPS)
            trips.loc[trips['duration_sec'].astype(float) < 0, 'duration_sec'] = 0
            trips['start_date'] = pd.to_datetime(trips['start_date'], format='%Y-%m-%d %H:%M:%S')
            trips['start_date'] = pd.to_datetime(trips['start_date'].dt.strftime('%Y-%m-%d'), format='%Y-%m-%d')
            trips_rain = trips.merge(self._weather, left_on='start_date', right_on='date')[['start_date', 'duration_sec']]
            if trips_rain.empty:
                return

            msg = trips_rain.to_csv(None, index=False, header=False)
            logging.debug(f'action: rain_filter_filtering | result: success | msg_filtered: {msg}')

    def run(self):
        self._channel.basic_qos(prefetch_count=1)
        self._channel.basic_consume(queue='task_queue', on_message_callback=self.filter_msg)

        self._channel.start_consuming()
