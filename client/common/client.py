import socket
import struct
import logging
HEADER_SIZE = 4


class Client:
    def __init__(self, client_id, host, port):
        self.__client_id = client_id
        self.__client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.__client_socket.connect((host, port))

    def __close_client(self):
        self._server_socket.close()
        logging.info('action: close_server | result: success')

    def run(self):
        self.__handle_connection()

    def __recv(self, conn, size):
        data = b''

        while len(data) < size:
            packet = conn.recv(size - len(data))

            if not packet:
                return None

            data += packet

        return data

    def __receive_msg(self, conn):
        # Receive msg_size
        msg_size = self.__recv(conn, HEADER_SIZE)
        msg_size = struct.unpack('<I', msg_size)[0]

        # Receive message data
        data = self.__recv(conn, msg_size)
        msg = data.decode('utf-8')

        return msg

    def __send_msg(self, conn, msg):
        # Pack message size into header
        data = msg.encode('utf-8')
        data_size = len(data)
        header = struct.pack('<I', data_size)
        # Send header and message
        conn.sendall(header + data)

    def __handle_connection(self):
        try:
            msg = 'Hello server from ' + self.__client_id
            self.__send_msg(self.__client_socket, msg)
            new_msg = self.__receive_msg(self.__client_socket)
            logging.info(f'action: receive_message | result: success | id: {self.__client_id} | msg: {new_msg}')
        except OSError as e:
            logging.error(f"action: receive_message | result: fail | error: {e}")
        finally:
            self.__client_socket.close()
