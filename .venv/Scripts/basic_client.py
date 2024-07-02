import socket
import json
from datetime import datetime
import threading

class Client:
    def __init__(self, host, port, client_id):
        self.host = host
        self.port = port
        self.client_id = client_id
        self.client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    def create_message(self, msg_type, topic, mode, payload):
        message = {
            "type": msg_type,
            "id": self.client_id,
            "topic": topic,
            "mode": mode,
            "timestamp": datetime.now().isoformat(),
            "payload": payload
        }
        return json.dumps(message)

    def connect(self):
        try:
            self.client_socket.connect((self.host, self.port))
            print(f'Client {self.client_id} połączony z serwerem {self.host}:{self.port}')
        except socket.error as e:
            print(f'Błąd połączenia: {e}')

    def send_message(self, msg_type, topic, mode, payload):
        message = self.create_message(msg_type, topic, mode, payload)
        try:
            self.client_socket.sendall(message.encode())
            response = self.client_socket.recv(1024)
            print(f'Odpowiedź z serwera dla {self.client_id}: {response.decode()}')
        except socket.error as e:
            print(f'Błąd komunikacji: {e}')
        finally:
            self.client_socket.close()

def test_client(client_id, msg_type, topic, mode, payload):
    client = Client('127.0.0.1', 12345, client_id)
    client.connect()
    client.send_message(msg_type, topic, mode, payload)

if __name__ == "__main__":
    clients = [
        # {"client_id": "Client1", "msg_type": "register", "topic": "lunch call", "mode": "producer", "payload": {"content": "Lunch call initiated."}},
        # {"client_id": "Client2", "msg_type": "register", "topic": "lunch call", "mode": "subscriber", "payload": {"content": "Subscribed to lunch call."}},
        {"client_id": "Client3", "msg_type": "register", "topic": "dinner call", "mode": "producer", "payload": {"content": "Dinner call initiated."}},
        # {"client_id": "Client4", "msg_type": "register", "topic": "dinner call", "mode": "subscriber", "payload": {"content": "Subscribed to dinner call."}},
        # {"client_id": "Client2", "msg_type": "withdraw", "topic": "lunch call", "mode": "subscriber", "payload": {"content": "Unsubscribed from lunch call."}},
    ]

    threads = []
    for client in clients:
        t = threading.Thread(target=test_client, args=(client['client_id'], client['msg_type'], client['topic'], client['mode'], client['payload']))
        threads.append(t)
        t.start()

    for t in threads:
        t.join()
