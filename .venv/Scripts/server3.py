import socket
import json
import threading
import time
from datetime import datetime

# Dane do przechowywania stanu serwera
topics = {}  # Słownik przechowujący tematy
clients = {}  # Słownik przechowujący referencje do gniazd klientów-producentów
subscribers = {}  # Słownik przechowujący subskrybentów dla każdego tematu
KKO = []  # Kolejka komunikatów odebranych
KKW = []  # Kolejka komunikatów do wysłania


class Server:
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen(5)
        print(f'Serwer nasłuchuje na {self.host}:{self.port}')

    def start(self):
        threading.Thread(target=self.communication_thread).start()
        threading.Thread(target=self.monitoring_thread).start()
        threading.Thread(target=self.user_interface_thread).start()

    def communication_thread(self):
        while True:
            try:
                client_socket, addr = self.server_socket.accept()
                if client_socket not in clients:
                    clients[client_socket] = 'new user'
                threading.Thread(target=self.client_handler, args=(client_socket,)).start()
            except socket.error as e:
                print(f'Błąd gniazda: {e}')

    def client_handler(self, client_socket):
        try:
            while True:
                message = client_socket.recv(1024).decode()
                if not message:
                    break
                self.handle_message(message, client_socket)
                # print(message)
                # self.manage_message(message)
        except socket.error as e:
            print(f'Błąd komunikacji z klientem: {e}')
        finally:
            self.disconnect_client(client_socket)

    def handle_message(self, message, client_socket):
        try:
            message_data = json.loads(message)
            print(message_data)
            if message_data['type'] == 'register':
                self.handle_register(message_data, client_socket)
            elif message_data['type'] == 'withdraw':
                self.handle_withdraw(message_data, client_socket)
            elif message_data['type'] == 'message':
                self.handle_message_type(message_data, client_socket)
            elif message_data['type'] == 'status':
                self.handle_status(message_data, client_socket)
            else:
                print(f'Nieobsługiwany typ komunikatu: {message_data["type"]}')
        except json.JSONDecodeError as e:
            print(f'Błąd dekodowania komunikatu JSON: {e}')
        except KeyError as e:
            print(f'Brakujące pole w komunikacie: {e}')

    def handle_register(self, message_data, client_socket):
        topic = message_data['topic']
        client_id = message_data['id']
        mode = message_data['mode']

        if topic not in topics:
            topics[topic] = {'producers': {}, 'subscribers': []}

        if mode == 'producer':
            if client_id not in topics[topic]['producers']:
                topics[topic]['producers'][client_id] = client_socket
                clients[client_socket] = client_id
                print(f'Zarejestrowano producenta {client_id} dla tematu {topic}')
            else:
                print(f'Temat {topic} już istnieje i został utworzony przez innego producenta')
                self.send_response(client_socket, 'rejected', 'Temat już istnieje')
        elif mode == 'subscriber':
            if client_socket not in topics[topic]['subscribers']:
                clients[client_socket] = client_id
                topics[topic]['subscribers'].append(client_socket)
                print(f'Zarejestrowano subskrybenta dla tematu {topic}')
            else:
                print(f'Klient już jest subskrybentem tematu {topic}')
        else:
            print(f'Nieobsługiwany tryb: {mode}')

    def handle_withdraw(self, message_data, client_socket):
        topic = message_data['topic']
        client_id = message_data['id']
        mode = message_data['mode']

        if topic in topics:
            if mode == 'producer':
                if client_id in topics[topic]['producers'] and topics[topic]['producers'][client_id] == client_socket:
                    del topics[topic]['producers'][client_id]
                    if not topics[topic]['producers']:
                        del topics[topic]
                    # self.disconnect_client(client_socket)
                    print(f'Usunięto temat {topic}')
                else:
                    print(f'Klient {client_id} nie jest producentem tematu {topic}')
            elif mode == 'subscriber':
                if client_socket in topics[topic]['subscribers']:
                    topics[topic]['subscribers'].remove(client_socket)
                    print(f'Usunięto subskrypcję klienta dla tematu {topic}')
                else:
                    print(f'Klient nie jest subskrybentem tematu {topic}')
            else:
                print(f'Nieobsługiwany tryb: {mode}')
        else:
            print(f'Temat {topic} nie istnieje')

    def handle_message_type(self, message_data, client_socket):
        topic = message_data['topic']
        if topic in topics:
            for subscriber_socket in topics[topic]['subscribers']:
                KKW.append({
                    'socket': subscriber_socket,
                    'message': message_data
                })
            print(f'Dodano komunikat do KKW dla tematu {topic}')
        else:
            print(f'Temat {topic} nie istnieje')

    def handle_status(self, message_data, client_socket):
        print(message_data)
        status_message = {
            "registered_topics": {},
        }
        for topic, data in topics.items():
            status_message["registered_topics"][topic] = {
                "producers": list(data["producers"].keys()) if data["producers"] else ["brak"],
                "subscribers": len(data["subscribers"]) if data["subscribers"] else ["brak"]
            }
        print(status_message)
        KKW.append({
            'socket': client_socket,
            'message': {
                'type': 'status',
                'id': clients[client_socket],
                'topic': 'logs',
                'mode': '',
                'timestamp': datetime.now().isoformat(),
                'payload': status_message
            }
        })
        print(KKW)
        print(f'Dodano status do KKW dla klienta {clients[client_socket]}')

    def send_response(self, client_socket, response_type, message):
        response = {
            'type': response_type,
            'message': message
        }
        client_socket.sendall(json.dumps(response).encode())

    def disconnect_client(self, client_socket):
        if client_socket in clients:
            del clients[client_socket]
        for topic, data in topics.items():
            print(topic)
            print(data)
            if client_socket in data['subscribers']:
                data['subscribers'].remove(client_socket)
            if client_socket in data['producers'].values():
                producers_to_remove = [producer_id for producer_id, socket in data['producers'].items() if
                                       socket == client_socket]
                for producer_id in producers_to_remove:
                    del data['producers'][producer_id]
                    # self.send_response(client_socket, 'withdraw', f'Usunięto producenta {producer_id} z tematu {topic}')
            if not data['producers'] and not data['subscribers']:
                del topics[topic]
                print(f'Usunięto temat {topic}')
        client_socket.close()

    def monitoring_thread(self):
        while True:
            if not KKO and not KKW:
                time.sleep(0.001)
                continue

            if KKO:
                message = KKO.pop(0)
                if self.validate_message(message):
                    self.manage_message(message)

            if KKW:
                item = KKW.pop(0)
                print(item)
                client_socket = item['socket']
                message = item['message']
                try:
                    client_socket.sendall(json.dumps(message).encode())
                    print(f'Wysłano wiadomość do klienta {clients[client_socket]}: {message}')
                except socket.error as e:
                    print(f'Błąd wysyłania wiadomości do klienta: {e}')

    def validate_message(self, message):
        try:
            message_data = message['message']
            # Dodaj walidację formatu komunikatu
            return True
        except Exception as e:
            print(f'Błąd walidacji komunikatu: {e}')
            return False

    def manage_message(self, message):
        print(message)
        message_data = message['message']
        message_type = message_data['type']
        if message_type == 'register':
            self.handle_register(message_data, message['socket'])
        elif message_type == 'withdraw':
            self.handle_withdraw(message_data, message['socket'])
        elif message_type == 'message':
            self.handle_message_type(message_data, message['socket'])
        elif message_type == 'status':
            self.handle_status(message_data, message['socket'])
        else:
            print(f'Nieobsługiwany typ komunikatu: {message_type}')

    def user_interface_thread(self):
        while True:
            command = input("Wpisz komendę (np. 'show topics', 'show clients'): ")
            if command.lower() == 'show topics':
                self.show_registered_topics()
            if command.lower() == 'show clients':
                self.show_connected_clients()
            ## zamykanie serwera

    def show_registered_topics(self):
        print("Zarejestrowane tematy:")
        for topic, data in topics.items():
            producers = list(data['producers'].keys())
            if len(data['subscribers']):
                subscribers = list(data['subscribers'])
            else:
                subscribers = 0
            print(f"Temat: {topic}, Producent(ów): {producers}, Subskrybentów: {subscribers}")

    def show_connected_clients(self):
        print("Połączeni klienci:")
        for client, data in clients.items():
            print(f'{data}: {client}')


if __name__ == "__main__":
    server = Server('127.0.0.1', 12345)
    server.start()