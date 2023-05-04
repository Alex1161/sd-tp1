import pika
import logging
import pandas as pd
from haversine import haversine
EOF = b'EOF'
EOD = b'EOD'
COLUMNS_TRIPS = ['name', 'start_latitude', 'start_longitude', 'end_latitude', 'end_longitude']
RESULT_COLUMNS = ['name', 'total_km', 'count']


class Worker3():
    def __init__(self):
        self._result = pd.DataFrame(columns=RESULT_COLUMNS)

        self._connection = pika.BlockingConnection(
            pika.ConnectionParameters(host='worker3_queue'))
        self._channel = self._connection.channel()

        self._channel.queue_declare(queue='task_queue', durable=True)

    def _persist(self, partial_results):
        self._result = pd.concat([self._result, partial_results], ignore_index=True)
        self._result = self._result.groupby('name').agg({'total_km': 'sum', 'count': 'sum'}).reset_index()
        self._result.columns = RESULT_COLUMNS

    def _process_msg(self, ch, method, properties, body):
        ch.basic_ack(delivery_tag=method.delivery_tag)
        if body == EOF:
            msg = self._result.to_csv(None, index=False, header=False)[:-1]
            logging.info(f'action: EOF_detected | result: success | result: {msg}')
            return

        rows_str = body.decode('utf-8')
        rows = [row.split(',') for row in rows_str.split('\n')]
        trips = pd.DataFrame(rows, columns=COLUMNS_TRIPS)
        trips['name'] = trips['name'].astype(str)
        trips['start_latitude'] = trips['start_latitude'].astype(float)
        trips['start_longitude'] = trips['start_longitude'].astype(float)
        trips['end_latitude'] = trips['end_latitude'].astype(float)
        trips['end_longitude'] = trips['end_longitude'].astype(float)
        station_distances_array = trips.apply(lambda r : [r['name'], haversine((r['start_latitude'], r['start_longitude']), (r['end_latitude'], r['end_longitude']))], axis = 1)
        trips = pd.DataFrame(station_distances_array.values.tolist(), columns=['name', 'distance'])
        result = trips.groupby('name').agg({'distance': ['sum', 'count']}).reset_index()
        result.columns = RESULT_COLUMNS
        self._persist(result)

        logging.debug(f'action: worker3_processing | result: success | msg_processed: {result}')


    def run(self):
        self._channel.basic_qos(prefetch_count=1)
        self._channel.basic_consume(queue='task_queue', on_message_callback=self._process_msg)

        self._channel.start_consuming()
