import asyncio


class ATCPConnection:
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.connected = False
        self.reader = None
        self.writer = None

    async def connect(self):
        try:
            self.reader, self.writer = await asyncio.open_connection(
                self.host, self.port
            )
            self.connected = True
            print(f"Connected to {self.host}:{self.port}")
        except Exception as e:
            print("Connection error:", e)

    async def send_message(self, message):
        try:
            if not self.connected:
                await self.connect()

            self.writer.write(message.encode())
            await self.writer.drain()
            #print("Message sent:", message)
        except Exception as e:
            print("Error sending message:", e)

    async def receive_message(self):
        try:
            if not self.connected:
                await self.connect()

            data = await self.reader.read(2048)
            message = data.decode()
            #print("Received message:", message)
            return message
        except Exception as e:
            print("Error receiving message:", e)
            return None

    async def close(self):
        try:
            if self.connected:
                self.writer.close()
                await self.writer.wait_closed()
                print("Connection closed")
                self.connected = False
        except Exception as e:
            print("Error closing connection:", e)
