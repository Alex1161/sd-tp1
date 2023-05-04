import pika
import logging
import pandas as pd
EOF = b'EOF'
EOD = b'EOD'
LOADING = 0
PROCESSING = 1
COLUMNS_STATIONS = ['code', 'name', 'latitude', 'longitude']
COLUMNS_TRIPS = ['start_station_code', 'end_station_code']


class MontRoyalFilter():
    def __init__(self):
        self._state = LOADING
        self._stations = pd.DataFrame(columns=COLUMNS_STATIONS)

        self._connection = pika.BlockingConnection(
            pika.ConnectionParameters(host='mont_royal_queue'))
        self._channel = self._connection.channel()

        self._channel.queue_declare(queue='task_queue', durable=True)

        self._worker3_connection = pika.BlockingConnection(
            pika.ConnectionParameters(host='worker3_queue'))
        self._channel_worker3 = self._worker3_connection.channel()

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
                self._stations['latitude'] = pd.to_numeric(self._stations['latitude'], errors='coerce').fillna(0)
                self._stations['longitude'] = pd.to_numeric(self._stations['longitude'], errors='coerce').fillna(0)
                logging.info(f'action: EOF_detected | result: success | data: {self._stations}')
                return

            rows_str = body.decode('utf-8')
            rows = [row.split(',') for row in rows_str.split('\n')]

            self._persist(rows)

            logging.debug(f'action: mont_royal_filter_loading | result: success | msg_filtered: {body}')
        elif self._state == PROCESSING:
            if body == EOF:
                self._channel_worker3.basic_publish(
                    exchange='',
                    routing_key='task_queue',
                    body=EOF,
                    properties=pika.BasicProperties(
                        delivery_mode=2,  # make message persistent
                    )
                )
                logging.info(f'action: EOF_detected | result: success')
                return

            rows_str = body.decode('utf-8')
            rows = [row.split(',') for row in rows_str.split('\n')]
            trips = pd.DataFrame(rows, columns=COLUMNS_TRIPS)
            trips['start_station_code'] = trips['start_station_code'].astype(str)
            trips['end_station_code'] = trips['end_station_code'].astype(str)
            trips_merged = trips.merge(self._stations, left_on='start_station_code', right_on='code')
            trips_merged = trips_merged.merge(self._stations, left_on='end_station_code', right_on='code')[['name_y', 'latitude_x', 'longitude_x', 'latitude_y', 'longitude_y']]
            filtered_trips = trips_merged[trips_merged['name_y'].str.contains('mont-royal', case=False)]

            if filtered_trips.empty:
                return

            msg = filtered_trips.to_csv(None, index=False, header=False)[:-1]
            self._channel_worker3.basic_publish(
                exchange='',
                routing_key='task_queue',
                body=msg,
                properties=pika.BasicProperties(
                    delivery_mode=2,  # make message persistent
                )
            )
            logging.debug(f'action: mont_royal_filter_filtering | result: success | msg_filtered: {msg}')

    def run(self):
        self._channel.basic_qos(prefetch_count=1)
        self._channel.basic_consume(queue='task_queue', on_message_callback=self._filter_msg)

        self._channel.start_consuming()
