import json
import socket
import threading
import queue

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
        if not KKW.empty():
            message, recipients = KKW.get()
            for recipient in recipients:
                try:
                    recipient.sendall(message)
                except socket.error as e:
                    print(f'Błąd wysyłania do {recipient.getpeername()}: {e}')
            KKW.task_done()

# Wątek interfejsu użytkownika
def user_interface_thread():
    while True:
        command = input("Komenda: ")
        if command == "exit":
            print("Zamykanie serwera...")
            break
        # Tutaj można dodać więcej komend

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
