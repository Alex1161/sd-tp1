import pika
import logging
import pandas as pd
EOF = b'EOF'
EOD = b'EOD'
COLUMNS_TRIPS = ['name', 'yearid']
RESULT_COLUMNS = ['yearid', 'name', 'count']


class Worker2():
    def __init__(self):
        self._result = pd.DataFrame(columns=RESULT_COLUMNS)

        self._connection = pika.BlockingConnection(
            pika.ConnectionParameters(host='worker2_queue'))
        self._channel = self._connection.channel()

        self._channel.queue_declare(queue='task_queue', durable=True)

    def _persist(self, partial_results):
        self._result = pd.concat([self._result, partial_results], ignore_index=True)
        self._result = self._result.groupby(['yearid', 'name']).agg({'count': 'sum'}).reset_index()
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
        trips['yearid'] = trips['yearid'].astype(int)
        trips['name'] = trips['name'].astype(str)
        result = trips.groupby(['yearid', 'name']).size().reset_index(name='count')
        self._persist(result)

        logging.debug(f'action: worker2_processing | result: success | msg_processed: {result}')


    def run(self):
        self._channel.basic_qos(prefetch_count=1)
        self._channel.basic_consume(queue='task_queue', on_message_callback=self._process_msg)

        self._channel.start_consuming()
