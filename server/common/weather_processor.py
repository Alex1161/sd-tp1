import logging
from .trips_processor import TripsProcessor
from .processor import Processor
import pika
RAIN_FILTER_FIELDS = ['date', 'prectot']


class WeatherProcessor(Processor):
    def __init__(self):
        # Create RabbitMQ communication channel
        self._connection = pika.BlockingConnection(
            pika.ConnectionParameters(host='rabbitmq'))
        self._channel = self._connection.channel()
        self._channel.queue_declare(queue='task_queue', durable=True)
        super().__init__()

    def get_next_processor(self):
        return TripsProcessor()

    def process(self, data: bytes):
        msgs = super().get_msgs_filtered(
            data,
            [RAIN_FILTER_FIELDS]
        )

        # Atipic case, when the last msg is a chunk of the last line
        if msgs[0] == '':
            return

        self._channel.basic_publish(
            exchange='',
            routing_key='task_queue',
            body=msgs[0],
            properties=pika.BasicProperties(
                delivery_mode=2,  # make message persistent
            )
        )

        logging.debug(f'action: weather_processor_processing | result: success | msg_to_rain_filter: {msgs[0]}')

    def end_of_file(self):
        last_msgs = super().get_last_msgs_filtered()
        self._channel.basic_publish(
            exchange='',
            routing_key='task_queue',
            body=last_msgs[0],
            properties=pika.BasicProperties(
                delivery_mode=2,  # make message persistent
            )
        )

        logging.debug(f'action: weather_processor_processing_last | result: success | msg_to_rain_filter: {last_msgs[0]}')
