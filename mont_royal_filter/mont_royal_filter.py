import pika
import logging
import pandas as pd
EOF = b'EOF'
EOD = b'EOD'
LOADING = 0
PROCESSING = 1
COLUMNS_MONT_ROYAL = ['code', 'name', 'latitude', 'longitude']
COLUMNS_TRIPS = ['start_ts', 'duration']


class MontRoyalFilter():
    def __init__(self):
        self._state = LOADING
        self._stations = pd.DataFrame(columns=COLUMNS_MONT_ROYAL)

        self._connection = pika.BlockingConnection(
            pika.ConnectionParameters(host='mont_royal_queue'))
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
                logging.info(f'action: EOF_detected | result: success | data: {self._stations}')
                return

            rows_str = body.decode('utf-8')
            rows = [row.split(',') for row in rows_str.split('\n')]

            self._persist(rows)

            logging.debug(f'action: mont_royal_filter_loading | result: success | msg_filtered: {body}')
        elif self._state == PROCESSING:
            if body == EOD:
                logging.info(f'action: EOD_detected | result: success')
                return

            # rows_str = body.decode('utf-8')
            # rows = [row.split(',') for row in rows_str.split('\n')]
            # trips = pd.DataFrame(rows, columns=COLUMNS_TRIPS)
            # logging.info(f'{trips}')

    def run(self):
        self._channel.basic_qos(prefetch_count=1)
        self._channel.basic_consume(queue='task_queue', on_message_callback=self._filter_msg)

        self._channel.start_consuming()
