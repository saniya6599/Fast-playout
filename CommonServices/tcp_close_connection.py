class TCPConnection:
    def close_connection(self, conn):
        try:
            conn.close()
            print("Connection closed successfully.")
        except Exception as e:
            print("Error closing connection:", e)
