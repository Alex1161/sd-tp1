from .send_error import SendError
import socket
import struct
import logging
HEADER_SIZE = 4
ACK_SIZE = 1
ACK = b'0'
EOD_SIZE = 3
EOD = b'EOD'


class Client:
    def __init__(self, client_id, host, port):
        self.__client_id = client_id
        self.__client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.__client_socket.connect((host, port))

    def close_client(self):
        self.__client_socket.close()
        logging.info('action: close_client | result: success')

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

    def __send_data(self, data):
        # Pack message size into header
        data_size = len(data)
        header = struct.pack('<I', data_size)

        # Send header and message
        self.__client_socket.sendall(header + data)

    def send_msg(self, msg):
        self.__send_data(msg)

        # Receiving ACK or ERROR from the server
        self.__recv_ack()

    def __recv_ack(self):
        ack = self.__recv(self.__client_socket, ACK_SIZE)
        if ack != ACK:
            raise SendError

    def send_eod(self):
        self.__send_data(EOD)
