import socket
from CommonServices.Logger import Logger

class TCPConnection:
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.socket = None
        self.logger=Logger()

    def start_server(self):
        try:
            # Create a new server socket and bind to the specified host and port
            self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.server_socket.bind((self.host, self.port))
            self.server_socket.listen(1)
            print(f"[INFO] Listening for connections on {self.host}:{self.port}")
            self.logger.info(f"Listening for connections on {self.host}:{self.port}")
        except Exception as e:
            print("[ERROR] Error creating server socket :", e)
            self.logger.error("Error creating server socket :", e)
            if self.server_socket:
                self.server_socket.close()

    def accept_connection(self):
        try:
            conn, addr = self.server_socket.accept()
            # self.logger.info(f"[INFO] Connection established with {addr}")
            #print(f"Connection established with {addr}")
            #self.logger.info(f"Connection established with {addr}")
            return conn
        except Exception as e:
            print("[ERROR] Error establishing connection : ", e)
            self.logger.error("Error establishing connection : ", e)
            return None

    def receive_message(self, conn):
        try:
            data = conn.recv(2048 * 10)
            if data:
                #print("Received message:", data.decode())
                return data.decode()
            else:
                print("[WARNING] Connection closed by the client")
                self.logger.warning("Connection closed by the client.")
                return None
        except Exception as e:
            print(f"[ERROR] Error receiving message: {e}")
            return None

    def send_message(self, conn, message):
        try:
            # encoded_message = message.encode('utf-8')  # Encode message using UTF-8
            conn.sendall(message.encode())
            # self.logger.info(f"[INFO] Message sent: {message}")
            #print("Message sent:", message)
        # except BrokenPipeError:
        #     print("[WARNING] Connection closed by the client.")
        #     self.logger.warning("Connection closed by the client.")
        except Exception as e:
            # self.logger.error(f"Error sending message: {e}")
            print(f"[ERROR] Error sending message: {e}")

    def close_connection(self, conn):
        try:
            conn.close()
            # self.logger.info("Connection closed.")
            # print("[INFO] Connection closed.")
        except Exception as e:
            self.logger.error(f"[ERROR] Error closing connection: {e}")
            print(f"[ERROR] Error closing connection: {e}")