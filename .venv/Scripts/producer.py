import socket
import json
import time
from datetime import datetime

class ProducerClient:
    def __init__(self, server_host, server_port, client_id, topic):
        self.server_host = server_host
        self.server_port = server_port
        self.client_id = client_id
        self.topic = topic
        self.client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    def connect(self):
        self.client_socket.connect((self.server_host, self.server_port))
        print(f'Połączono z serwerem {self.server_host}:{self.server_port}')
        self.register()

    def register(self):
        register_message = {
            "type": "register",
            "id": self.client_id,
            "topic": self.topic,
            "mode": "producer",
            "timestamp": datetime.now().isoformat(),
            "payload": {}
        }
        self.send_message(register_message)

    def send_message(self, message):
        self.client_socket.sendall(json.dumps(message).encode())

    def send_test_message(self, payload):
        test_message = {
            "type": "message",
            "id": self.client_id,
            "topic": self.topic,
            "mode": "producer",
            "timestamp": datetime.now().isoformat(),
            "payload": payload
        }
        self.send_message(test_message)

    def close(self):
        self.client_socket.close()

if __name__ == "__main__":
    producer = ProducerClient('127.0.0.1', 12345, 'Producer1', 'test_topic')
    producer.connect()
    time.sleep(15)  # Poczekaj, aby umożliwić subskrybentom rejestrację
    producer.send_test_message({"content": "Hello, this is a test message"})
    time.sleep(15)  # Poczekaj, aby umożliwić subskrybentom rejestrację
    producer.close()
