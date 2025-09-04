import socket
import time
from CommonServices.Logger import Logger
from CommonServices.logging_utils import log_function_call

class TCPConnection:
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.socket = None
        self.logger = Logger()
        self.connection_count = 0

    @log_function_call
    def start_server(self):
        try:
            # Create a new server socket and bind to the specified host and port
            self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.server_socket.bind((self.host, self.port))
            self.server_socket.listen(1)
            
            self.logger.log_connection_event(
                "server_started", 
                self.host, 
                self.port, 
                success=True
            )
        except Exception as e:
            self.logger.log_connection_event(
                "server_started", 
                self.host, 
                self.port, 
                success=False, 
                error=e
            )
            if hasattr(self, 'server_socket') and self.server_socket:
                self.server_socket.close()

    def accept_connection(self):
        try:
            conn, addr = self.server_socket.accept()
            self.connection_count += 1
            # Log at DEBUG only, minimal payload
            self.logger.log_connection_event(
                "accepted",
                addr[0],
                addr[1],
                success=True,
                connection_id=self.connection_count
            )
            return conn
        except Exception as e:
            # Log failures at INFO with error context
            self.logger.log_connection_event(
                "accept_failed",
                self.host,
                self.port,
                success=False,
                error=e
            )
            return None

    def receive_message(self, conn):
        try:
            data = conn.recv(2048 * 10)
            if data:
                message = data.decode()
                # Reduce noise: don't log full payloads by default
                self.logger.debug("Message received", message_length=len(message))
                return message
            else:
                self.logger.warning("Connection closed by the client")
                return None
        except Exception as e:
            self.logger.log_exception(e, context={'operation': 'receive_message'})
            return None

    def send_message(self, conn, message):
        try:
            conn.sendall(message.encode())
            self.logger.debug("Message sent", message_length=len(message))
        except BrokenPipeError:
            self.logger.warning("Connection closed by the client during send")
        except Exception as e:
            self.logger.log_exception(e, context={'operation': 'send_message'})

    @log_function_call
    def close_connection(self, conn):
        try:
            conn.close()
            self.logger.debug("Connection closed successfully")
        except Exception as e:
            self.logger.log_exception(e, context={'operation': 'close_connection'})