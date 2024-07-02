import json
import socket
import threading
import queue
import time
from datetime import datetime

class Server:
    def __init__(self, config_file):
        self.config = self.load_config(config_file)
        self.server_id = self.config['ServerID']
        self.host = self.config['Host']
        self.port = self.config['Port']
        self.LT = {}  # Lista Tematów: {topic: {'producer': (conn, client_id), 'subscribers': [(conn, client_id)]}}
        self.KKO = queue.Queue()  # Kolejka Komunikatów Odebranych
        self.KKW = queue.Queue()  # Kolejka Komunikatów do Wysłania
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    def load_config(self, config_file):
        with open(config_file, 'r') as file:
            config = json.load(file)
        return config

    def start(self):
        print(f'Serwer {self.server_id} uruchomiony na {self.host}:{self.port}')
        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen()
        threading.Thread(target=self.server_communication_thread).start()
        threading.Thread(target=self.monitor_thread).start()
        threading.Thread(target=self.user_interface_thread).start()

    def server_communication_thread(self):
        try:
            while True:
                conn, addr = self.server_socket.accept()
                print(f'Połączono z {addr}')
                threading.Thread(target=self.handle_client, args=(conn, addr)).start()
        except socket.error as e:
            print(f'Błąd gniazda: {e}')
        finally:
            self.server_socket.close()

    def handle_client(self, conn, addr):
        try:
            with conn:
                while True:
                    try:
                        data = conn.recv(1024)
                        if not data:
                            break
                        print(f'Otrzymano od {addr}: {data.decode()}')
                        self.KKO.put((conn, data))
                    except socket.error as e:
                        print(f'Błąd komunikacji z klientem {addr}: {e}')
                        break
        finally:
            self.handle_client_disconnect(conn, addr)
            conn.close()

    def handle_client_disconnect(self, conn, addr):
        print(f'Klient {addr} rozłączony.')
        self.remove_client_references(conn)
        conn.close()

    def remove_client_references(self, conn):
        to_remove = []
        for topic, info in self.LT.items():
            producer_conn, _ = info['producer']
            subscribers = info['subscribers']
            if producer_conn == conn:
                to_remove.append(topic)
            else:
                info['subscribers'] = [(sub_conn, sub_id) for sub_conn, sub_id in subscribers if sub_conn != conn]

        for topic in to_remove:
            self.remove_topic(topic)

    def remove_topic(self, topic):
        if topic in self.LT:
            topic_info = self.LT.pop(topic)
            producer_conn, _ = topic_info['producer']
            subscribers = topic_info['subscribers']

            for sub_conn, _ in subscribers:
                if not self.is_client_active(sub_conn):
                    sub_conn.close()

            if not self.is_client_active(producer_conn):
                producer_conn.close()

    def is_client_active(self, conn):
        for info in self.LT.values():
            if conn == info['producer'][0] or conn in [sub[0] for sub in info['subscribers']]:
                return True
        return False

    def validate_message(self, message):
        try:
            message = json.loads(message)
            required_fields = {"type", "id", "topic", "mode", "timestamp", "payload"}
            if not all(field in message for field in required_fields):
                return False

            if message["type"] not in {"register", "withdraw", "reject", "acknowledge", "message", "file", "config", "status"}:
                return False

            if message["mode"] not in {"producer", "subscriber"}:
                return False

            datetime.fromisoformat(message["timestamp"])  # Sprawdzenie poprawności formatu ISO 8601

            return True
        except (ValueError, KeyError, json.JSONDecodeError):
            return False

    def manage_message(self, conn, message):
        message = json.loads(message)
        msg_type = message["type"]

        if msg_type == "register":
            self.handle_register(conn, message)
        elif msg_type == "withdraw":
            self.handle_withdraw(conn, message)

    def handle_register(self, conn, message):
        topic = message["topic"]
        client_id = message["id"]
        mode = message["mode"]

        if mode == "producer":
            if topic not in self.LT:
                self.LT[topic] = {'producer': (conn, client_id), 'subscribers': []}
                print(f'Temat {topic} zarejestrowany przez producenta {client_id}')
            else:
                existing_producer_conn, existing_producer_id = self.LT[topic]['producer']
                if existing_producer_id != client_id:
                    print(f'Odrzucono rejestrację tematu {topic} przez producenta {client_id}. Temat już istnieje.')
                    self.send_response(conn, "reject", topic, client_id)
        elif mode == "subscriber":
            if topic in self.LT:
                self.LT[topic]['subscribers'].append((conn, client_id))
                print(f'Subskrybent {client_id} zarejestrowany do tematu {topic}')
            else:
                print(f'Odrzucono subskrypcję tematu {topic} przez subskrybenta {client_id}. Temat nie istnieje.')
                self.send_response(conn, "reject", topic, client_id)

    def handle_withdraw(self, conn, message):
        topic = message["topic"]
        client_id = message["id"]
        mode = message["mode"]

        if mode == "producer":
            if topic in self.LT and self.LT[topic]['producer'][1] == client_id:
                self.remove_topic(topic)
                print(f'Producent {client_id} wycofał temat {topic}')
        elif mode == "subscriber":
            if topic in self.LT:
                self.LT[topic]['subscribers'] = [(sub_conn, sub_id) for sub_conn, sub_id in self.LT[topic]['subscribers'] if sub_id != client_id]
                print(f'Subskrybent {client_id} wycofał subskrypcję tematu {topic}')

    def send_response(self, conn, response_type, topic, client_id):
        response = {
            "type": response_type,
            "id": self.server_id,
            "topic": topic,
            "mode": "",
            "timestamp": datetime.now().isoformat(),
            "payload": {
                "message": f"{response_type.capitalize()} {client_id} for topic {topic}"
            }
        }
        conn.sendall(json.dumps(response).encode())

    def monitor_thread(self):
        while True:
            if self.KKO.empty():
                time.sleep(0.001)  # Uśpienie na 1 ms
                continue

            conn, raw_message = self.KKO.get()
            message = raw_message.decode()

            if self.validate_message(message):
                self.manage_message(conn, message)
            else:
                print(f'Nieprawidłowy komunikat: {message}')

    def user_interface_thread(self):
        while True:
            command = input("Komenda: ")
            if command == "exit":
                print("Zamykanie serwera...")
                break
            # Tutaj można dodać więcej komend

if __name__ == "__main__":
    server = Server('config.json')
    server.start()
