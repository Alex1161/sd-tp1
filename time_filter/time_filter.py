import pika
import logging
import pandas as pd
EOF = b'EOF'
EOD = b'EOD'
LOADING = 0
PROCESSING = 1
COLUMNS_STATION = ['code', 'name']
COLUMNS_TRIPS = ['code', 'yearid']


class TimeFilter():
    def __init__(self):
        self._state = LOADING
        self._stations = pd.DataFrame(columns=COLUMNS_STATION)

        self._connection = pika.BlockingConnection(
            pika.ConnectionParameters(host='time_queue'))
        self._channel = self._connection.channel()

        self._channel.queue_declare(queue='task_queue', durable=True)

    def _persist(self, rows):
        for row in rows:
            self._stations.loc[len(self._stations)] = row

    def _filter_msg(self, ch, method, properties, body):
        ch.basic_ack(delivery_tag=method.delivery_tag)
        if self._state == LOADING:
            if body == EOF:
                self._state = PROCESSING
                self._stations['code'] = self._stations['code'].astype(str)
                self._stations['name'] = self._stations['name'].astype(str)
                logging.info(f'action: EOF_detected | result: success | data: {self._stations}')
                return

            rows_str = body.decode('utf-8')
            rows = [row.split(',') for row in rows_str.split('\n')]

            self._persist(rows)

            logging.debug(f'action: time_filter_loading | result: success | msg_filtered: {body}')
        elif self._state == PROCESSING:
            if body == EOF:
                logging.info(f'action: EOF_detected | result: success')
                return

            rows_str = body.decode('utf-8')
            rows = [row.split(',') for row in rows_str.split('\n')]
            trips = pd.DataFrame(rows, columns=COLUMNS_TRIPS)
            trips['code'] = trips['code'].astype(str)
            trips['yearid'] = trips['yearid'].astype(int)
            trips_filtered = trips.loc[(trips['yearid'] == 2016) | (trips['yearid'] == 2017)]
            trips_filtered_merged = trips_filtered.merge(self._stations, left_on='code', right_on='code')
            if trips_filtered_merged.empty:
                return

            msg = trips_filtered_merged.to_csv(None, index=False, header=False)
            logging.debug(f'action: time_filter_filtering | result: success | msg_filtered: {msg}')


    def run(self):
        self._channel.basic_qos(prefetch_count=1)
        self._channel.basic_consume(queue='task_queue', on_message_callback=self._filter_msg)

        self._channel.start_consuming()
