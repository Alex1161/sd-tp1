import pika
import logging
import pandas as pd
EOF = b'EOF'
EOD = b'EOD'
COLUMNS_TRIPS = ['start_date', 'duration_sec']
RESULT_COLUMNS = ['start_date', 'sum', 'count']


class Worker1():
    def __init__(self):
        self._result = pd.DataFrame(columns=RESULT_COLUMNS)

        self._connection = pika.BlockingConnection(
            pika.ConnectionParameters(host='worker1_queue'))
        self._channel = self._connection.channel()

        self._channel.queue_declare(queue='task_queue', durable=True)

    def _persist(self, partial_results):
        self._result = pd.concat([self._result, partial_results], ignore_index=True)
        self._result = self._result.groupby('start_date').agg({'sum': 'sum', 'count': 'sum'}).reset_index()
        self._result.columns = RESULT_COLUMNS

    def _process_msg(self, ch, method, properties, body):
        ch.basic_ack(delivery_tag=method.delivery_tag)
        if body == EOF:
            msg = self._result.to_csv(None, index=False, header=False)
            logging.info(f'action: EOF_detected | result: success | result: {msg}')
            return

        rows_str = body.decode('utf-8')
        rows = [row.split(',') for row in rows_str.split('\n')]
        trips = pd.DataFrame(rows, columns=COLUMNS_TRIPS)
        trips['start_date'] = pd.to_datetime(trips['start_date'])
        trips['duration_sec'] = trips['duration_sec'].astype(float)
        result = trips.groupby('start_date').agg({'duration_sec': ['sum', 'count']}).reset_index()
        result.columns = RESULT_COLUMNS
        self._persist(result)

        logging.debug(f'action: worker1_processing | result: success | msg_processed: {result}')


    def run(self):
        self._channel.basic_qos(prefetch_count=1)
        self._channel.basic_consume(queue='task_queue', on_message_callback=self._process_msg)

        self._channel.start_consuming()
