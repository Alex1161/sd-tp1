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

    def __set_headers(self, msg: str, fields_to_filter: list):
        header, rest = msg.split('\n', 1)
        self._header = header

        self._fields_positions_to_filter = list(map(
            lambda x: self.__get_header_positions(x), fields_to_filter
        ))

        return rest

    def get_msgs_filtered(self, data: bytes, fields_to_filter: list):
        msg = data.decode('utf-8')
        if not self._header:
            msg = self.__set_headers(msg, fields_to_filter)

        rows = self.__complete_rows_from_msg(msg)

        msgs = map(
            lambda x: self.__filter_positions(rows, x),
            self._fields_positions_to_filter
        )

        return list(msgs)

    # To improve, minimizing the last send
    def get_last_msgs_filtered(self):
        rows = [self._rest_last_msg]
        msgs = map(
            lambda x: self.__filter_positions(rows, x),
            self._fields_positions_to_filter
        )

        self._header = None
        self._rest_last_msg = ''
        return list(msgs)
