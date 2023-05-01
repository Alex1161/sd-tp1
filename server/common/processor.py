class Processor():
    def complete_rows_from_msg(self, msg: str):
        msg = self._rest_last_msg + msg
        rows = msg.split('\n')

        # Separate last element because if it is incomplete
        self._rest_last_msg = rows[-1]
        return rows[:-1]

    def filter_positions(self, rows: list, positions: list):
        rows = map(lambda x: x.split(','), rows)
        positions_filtered = [
            [elem for i, elem in enumerate(row) if i in positions]
            for row in rows
        ]
        rows_fields_filtered = map(lambda x: ','.join(x), positions_filtered)
        new_msg = '\n'.join(rows_fields_filtered)
        return new_msg
