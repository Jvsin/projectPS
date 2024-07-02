import socket
import threading
import json
import time
from datetime import datetime


class SPClientAPI:
    def __init__(self):
        self.server_ip = None
        self.server_port = None
        self.client_id = None
        self.client_socket = None
        self.connected = False
        self.topics_produced = set()
        self.topics_subscribed = {}
        self.lock = threading.Lock()

    def start(self, server_ip, server_port, client_id):
        self.server_ip = server_ip
        self.server_port = server_port
        self.client_id = client_id
        self.client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            self.client_socket.connect((self.server_ip, self.server_port))
            self.connected = True
            print(f'Connected to server {self.server_ip}:{self.server_port}')
            threading.Thread(target=self.listen_to_server, daemon=True).start()
        except socket.error as e:
            print(f'Error connecting to server: {e}')
            self.connected = False

    def is_connected(self):
        return self.connected

    def get_status(self):
        status = {
            "produced_topics": list(self.topics_produced),
            "subscribed_topics": list(self.topics_subscribed.keys())
        }
        return json.dumps(status, indent=2)

    def get_server_status(self, callback):
        status_message = {
            "type": "status",
            "id": self.client_id,
            "topic": "logs",
            "mode": "producer",
            "timestamp": datetime.now().isoformat(),
            "payload": {}
        }
        self.send_message(status_message)
        threading.Thread(target=self.await_server_status_response, args=(callback,), daemon=True).start()

    def create_producer(self, topic_name):
        register_message = {
            "type": "register",
            "id": self.client_id,
            "topic": topic_name,
            "mode": "producer",
            "timestamp": datetime.now().isoformat(),
            "payload": {}
        }
        self.send_message(register_message)
        self.topics_produced.add(topic_name)

    def produce(self, topic_name, payload):
        if topic_name in self.topics_produced:
            message = {
                "type": "message",
                "id": self.client_id,
                "topic": topic_name,
                "mode": "producer",
                "timestamp": datetime.now().isoformat(),
                "payload": payload
            }
            self.send_message(message)
        else:
            print(f'Error: Not producing topic {topic_name}')

    def withdraw_producer(self, topic_name):
        if topic_name in self.topics_produced:
            withdraw_message = {
                "type": "withdraw",
                "id": self.client_id,
                "topic": topic_name,
                "mode": "producer",
                "timestamp": datetime.now().isoformat(),
                "payload": {}
            }
            self.send_message(withdraw_message)
            self.topics_produced.remove(topic_name)
        else:
            print(f'Error: Not producing topic {topic_name}')

    def create_subscriber(self, topic_name, callback):
        register_message = {
            "type": "register",
            "id": self.client_id,
            "topic": topic_name,
            "mode": "subscriber",
            "timestamp": datetime.now().isoformat(),
            "payload": {}
        }
        self.send_message(register_message)
        self.topics_subscribed[topic_name] = callback

    def withdraw_subscriber(self, topic_name):
        if topic_name in self.topics_subscribed:
            withdraw_message = {
                "type": "withdraw",
                "id": self.client_id,
                "topic": topic_name,
                "mode": "subscriber",
                "timestamp": datetime.now().isoformat(),
                "payload": {}
            }
            self.send_message(withdraw_message)
            del self.topics_subscribed[topic_name]
        else:
            print(f'Error: Not subscribed to topic {topic_name}')

    def stop(self):
        self.connected = False
        self.client_socket.close()
        self.topics_produced.clear()
        self.topics_subscribed.clear()
        print('Client stopped and disconnected from server')

    def send_message(self, message):
        with self.lock:
            try:
                self.client_socket.sendall(json.dumps(message).encode())
            except socket.error as e:
                print(f'Error sending message: {e}')
                self.connected = False

    def await_server_status_response(self, callback):
        try:
            while True:
                response = self.client_socket.recv(1024).decode()
                if response:
                    response_message = json.loads(response)
                    if response_message["type"] == "status" and response_message["topic"] == "logs":
                        callback(response_message["payload"])
                        break
        except socket.error as e:
            print(f'Error receiving message: {e}')

    def listen_to_server(self):
        try:
            while self.connected:
                message = self.client_socket.recv(1024).decode()
                if message:
                    message_data = json.loads(message)
                    if message_data["type"] == "status" and message_data["topic"] == "logs":
                        print(f'Received server status: {message_data["payload"]}')
                    else:
                        topic = message_data.get("topic")
                        if topic in self.topics_subscribed:
                            callback = self.topics_subscribed[topic]
                            callback(message_data["payload"])
                        else:
                            print(f'Received message on unregistered topic: {topic}')
        except socket.error as e:
            print(f'Error receiving message from server: {e}')
            self.connected = False
        finally:
            self.client_socket.close()
            self.connected = False


# Przykład użycia API z obsługą terminala
if __name__ == "__main__":
    def status_callback(payload):
        print(f'Server status: {json.dumps(payload, indent=2)}')


    def message_callback(payload):
        print(f'Received message: {json.dumps(payload, indent=2)}')


    client = SPClientAPI()
    client.start('127.0.0.1', 12345, 'Client1')

    time.sleep(2)  # Czekaj na połączenie

    while True:
        command = input(
            "Enter command (start, stop, status, create_producer, produce, withdraw_producer, create_subscriber, withdraw_subscriber, server_status): ")

        if command == "start":
            if not client.is_connected():
                server_ip = input("Enter server IP: ")
                server_port = int(input("Enter server port: "))
                client_id = input("Enter client ID: ")
                client.start(server_ip, server_port, client_id)
            else:
                print("Client already connected.")

        elif command == "stop":
            client.stop()
            break

        elif command == "status":
            print(client.get_status())

        elif command == "create_producer":
            topic_name = input("Enter topic name: ")
            client.create_producer(topic_name)

        elif command == "produce":
            topic_name = input("Enter topic name: ")
            payload_content = input("Enter payload: ")
            payload = {"content": payload_content}
            client.produce(topic_name, payload)

        elif command == "withdraw_producer":
            topic_name = input("Enter topic name: ")
            client.withdraw_producer(topic_name)

        elif command == "create_subscriber":
            topic_name = input("Enter topic name: ")
            client.create_subscriber(topic_name, message_callback)

        elif command == "withdraw_subscriber":
            topic_name = input("Enter topic name: ")
            client.withdraw_subscriber(topic_name)

        elif command == "server_status":
            time.sleep(1)
            client.get_server_status(status_callback)

        else:
            print("Unknown command. Please try again.")
