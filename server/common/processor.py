import pika
EOF = b'EOF'


class Processor():
    def __init__(self):
        self._header = None
        self._rest_last_msg = ''

    def __complete_rows_from_msg(self, msg: str):
        msg = self._rest_last_msg + msg
        rows = msg.split('\n')

        # Separate last element because it could be incomplete
        self._rest_last_msg = rows[-1]
        return rows[:-1]

    def __filter_positions(self, rows: list, positions: list):
        rows = map(lambda x: x.split(','), rows)
        positions_filtered = [
            [elem for i, elem in enumerate(row) if i in positions]
            for row in rows
        ]
        rows_fields_filtered = map(lambda x: ','.join(x), positions_filtered)
        new_msg = '\n'.join(rows_fields_filtered)
        return new_msg

    def __get_header_positions(self, fields):
        return [
            index for index,
            field in enumerate(self._header.split(','))
            if field in fields
        ]

    # fields_to_filter = [[queue_to_send_data, fields_to_filter]]
    def __set_headers(self, msg: str, fields_to_filter: list):
        header, rest = msg.split('\n', 1)
        self._header = header

        self._fields_positions_to_filter = list(map(
            lambda x: [x[0], self.__get_header_positions(x[1])],
            fields_to_filter
        ))

        return rest

    def process(self, data: bytes, fields_to_filter: list):
        msg = data.decode('utf-8')
        if not self._header:
            msg = self.__set_headers(msg, fields_to_filter)

        rows = self.__complete_rows_from_msg(msg)

        msgs = []
        for queue, filter in self._fields_positions_to_filter:
            msg = self.__filter_positions(rows, filter)

            # Atipic case, when the last msg is a chunk of the last line
            if msg == '':
                return msgs

            queue.basic_publish(
                exchange='',
                routing_key='task_queue',
                body=msg,
                properties=pika.BasicProperties(
                    delivery_mode=2,  # make message persistent
                )
            )

            msgs.append(msg)

        return msgs

    # To improve, minimizing the last send
    def end_of_file(self):
        rows = [self._rest_last_msg]
        msgs = []
        for queue, filter in self._fields_positions_to_filter:
            msg = self.__filter_positions(rows, filter)

            queue.basic_publish(
                exchange='',
                routing_key='task_queue',
                body=msg,
                properties=pika.BasicProperties(
                    delivery_mode=2,  # make message persistent
                )
            )

            queue.basic_publish(
                exchange='',
                routing_key='task_queue',
                body=EOF,
                properties=pika.BasicProperties(
                    delivery_mode=2,  # make message persistent
                )
            )

            msgs.append(msg)

        self._header = None
        self._rest_last_msg = ''
        return msgs
