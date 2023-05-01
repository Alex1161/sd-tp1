import pika
import logging


def filter_msg(ch, method, properties, body):
    ch.basic_ack(delivery_tag=method.delivery_tag)
    logging.info(f'action: rain_filter_filtering | result: success | msg_filtered: {body}')


class RainFilter():
    def __init__(self):
        self._connection = pika.BlockingConnection(
            pika.ConnectionParameters(host='rabbitmq'))
        self._channel = self._connection.channel()

        self._channel.queue_declare(queue='task_queue', durable=True)

    def run(self):
        self._channel.basic_qos(prefetch_count=1)
        self._channel.basic_consume(queue='task_queue', on_message_callback=filter_msg)

        self._channel.start_consuming()
