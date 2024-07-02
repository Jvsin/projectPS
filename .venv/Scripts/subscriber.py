import socket
import json
import threading
from datetime import datetime

class SubscriberClient:
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
        threading.Thread(target=self.listen_for_messages).start()

    def register(self):
        register_message = {
            "type": "register",
            "id": self.client_id,
            "topic": self.topic,
            "mode": "subscriber",
            "timestamp": datetime.now().isoformat(),
            "payload": {}
        }
        self.send_message(register_message)

    def send_message(self, message):
        self.client_socket.sendall(json.dumps(message).encode())

    def listen_for_messages(self):
        try:
            while True:
                message = self.client_socket.recv(1024).decode()
                if not message:
                    break
                print(f'Otrzymano wiadomość: {message}')
        except socket.error as e:
            print(f'Błąd komunikacji z serwerem: {e}')
        finally:
            self.client_socket.close()

if __name__ == "__main__":
    subscriber = SubscriberClient('127.0.0.1', 12345, 'Subscriber1', 'test_topic')
    subscriber.connect()
