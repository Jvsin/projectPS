import socket

def main():
    # Konfiguracja klienta
    host = '127.0.0.1'
    port = 12345

    # Tworzenie gniazda klienta
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        # Łączenie z serwerem
        client_socket.connect((host, port))
        print(f'Połączono z serwerem {host}:{port}')

        # Wysłanie wiadomości
        message = "Hello, Server!"
        client_socket.sendall(message.encode())

        # Odbieranie odpowiedzi
        response = client_socket.recv(1024)
        print(f'Odpowiedź z serwera: {response.decode()}')

    except socket.error as e:
        print(f'Błąd komunikacji: {e}')
    finally:
        client_socket.close()

if __name__ == "__main__":
    main()
