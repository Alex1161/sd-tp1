import socket
import struct
import logging
import signal
import sys
from .stations_processor import StationsProcessor
HEADER_SIZE = 4
ACK = b'0'
ERROR = b'1'
EOD = b'EOD'
EOF = b'EOF'


class Server:
    def __init__(self, port, listen_backlog, chunk_size):
        self._chunk_size = chunk_size
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.bind(('', port))
        self._server_socket.listen(listen_backlog)
        self._processor = StationsProcessor()
        signal.signal(signal.SIGTERM, self.__exit_gracefully)

    def __exit_gracefully(self, *args):
        self.close_server()
        sys.exit(0)

    def close_server(self):
        self._server_socket.close()
        logging.info('action: close_server | result: success')

    def run(self):
        while True:
            client_sock = self.__accept_new_connection()
            self.__handle_client_connection(client_sock)

    def __recv(self, conn, size):
        data = b''

        while len(data) < size:
            packet = conn.recv(size - len(data))

            if not packet:
                return None

            data += packet

        return data

    def __receive_data(self, conn):
        # Receive msg_size
        msg_size = self.__recv(conn, HEADER_SIZE)
        msg_size = struct.unpack('<I', msg_size)[0]

        # Receive message data
        data = self.__recv(conn, msg_size)

        conn.sendall(ACK)

        return data

    def __send_msg(self, conn, msg):
        # Pack message size into header
        data = msg.encode('utf-8')
        data_size = len(data)
        header = struct.pack('<I', data_size)

        # Send header and message
        conn.sendall(header + data)

    def __handle_client_connection(self, client_sock):
        """
        Read message from a specific client socket and closes the socket
        If a problem arises in the communication with the client, the
        client socket will also be closed
        """
        while True:
            try:
                data = self.__receive_data(client_sock)
                addr = client_sock.getpeername()
                if data == EOF:
                    logging.info(f'action: EOF | result: received | ip: {addr[0]}')
                    self._processor.end_of_file()
                    self._processor = self._processor.get_next_processor()
                    continue
                elif data == EOD:
                    logging.info(f'action: EOD | result: received | ip: {addr[0]}')
                    break

                self._processor.process(data)
            except OSError as e:
                logging.error(f"action: receive_message | result: fail | error: {e}")
                client_sock.close()
                break

    def __accept_new_connection(self):
        """
        Accept new connections
        Function blocks until a connection to a client is made.
        Then connection created is printed and returned
        """

        # Connection arrived
        logging.info('action: accept_connections | result: in_progress')
        c, addr = self._server_socket.accept()

        logging.info(f'action: accept_connections | result: success | ip: {addr[0]}')
        return c
