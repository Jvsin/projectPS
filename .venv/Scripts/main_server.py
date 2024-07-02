import json
import socket
import threading
import queue
import time
from datetime import datetime

# Wczytywanie konfiguracji
def load_config(config_file):
    with open(config_file, 'r') as file:
        config = json.load(file)
    return config

# Wątek komunikacji serwera
def server_communication_thread(server_socket, KKO, KKW):
    try:
        while True:
            conn, addr = server_socket.accept()
            print(f'Połączono z {addr}')
            threading.Thread(target=handle_client, args=(conn, addr, KKO, KKW)).start()
    except socket.error as e:
        print(f'Błąd gniazda: {e}')
    finally:
        server_socket.close()

def handle_client(conn, addr, KKO, KKW):
    try:
        with conn:
            while True:
                try:
                    data = conn.recv(1024)
                    if not data:
                        break
                    print(f'Otrzymano od {addr}: {data.decode()}')
                    KKO.put((conn, data))
                except socket.error as e:
                    print(f'Błąd komunikacji z klientem {addr}: {e}')
                    break
    finally:
        handle_client_disconnect(conn, addr, KKO, KKW)
        conn.close()

def handle_client_disconnect(conn, addr, KKO, KKW):
    print(f'Klient {addr} rozłączony.')

# Wątek monitorujący
def monitor_thread(LT, KKO, KKW):
    while True:
        if KKO.empty():
            time.sleep(0.001)
            continue

        conn, raw_message = KKO.get()
        message = raw_message.decode()

        if validate_message(message):
            manage_message(message)
        else:
            print(f'Nieprawidłowy komunikat: {message}')

# Wątek interfejsu użytkownika
def user_interface_thread():
    while True:
        command = input("Komenda: ")
        if command == "exit":
            print("Zamykanie serwera...")
            break
        # Tutaj można dodać więcej komend

def validate_message(message):
    try:
        message = json.loads(message)
        required_fields = {"type", "id", "topic", "mode", "timestamp", "payload"}
        if not all(field in message for field in required_fields):
            return False

        if message["type"] not in {"register", "withdraw", "message", "status"}:
            return False

        if message["mode"] not in {"producer", "subscriber"}:
            return False

        datetime.fromisoformat(message["timestamp"])  # Sprawdzenie poprawności formatu ISO 8601

        return True
    except (ValueError, KeyError, json.JSONDecodeError):
        return False


def manage_message(message):
    print(f'Zarządzanie komunikatem: {message}')

def main():
    # Wczytywanie konfiguracji
    config = load_config('config.json')
    server_id = config['ServerID']
    host = config['Host']
    port = config['Port']

    print(f'Serwer {server_id} uruchomiony na {host}:{port}')

    topics_list = []  # Lista Tematów
    mess_received_queue = queue.Queue()  # Kolejka Komunikatów Odebranych
    mess_to_send_queue = queue.Queue()  # Kolejka Komunikatów do Wysłania

    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind((host, port))
    server_socket.listen()

    # Wątek do ustawienia komunikacji serwera
    threading.Thread(target=server_communication_thread,
                     args=(server_socket, mess_received_queue, mess_to_send_queue)).start()
    # Wątek monitorujący
    threading.Thread(target=monitor_thread,
                     args=(topics_list, mess_received_queue, mess_to_send_queue)).start()

    threading.Thread(target=user_interface_thread).start()

if __name__ == "__main__":
    main()
