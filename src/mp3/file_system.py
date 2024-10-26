import threading
import socket
import sys
import os

from src.shared.constants import FILE_SYSTEM_PORT, HOSTS, MAX_CLIENTS
from src.mp3.shared import read_file_to_socket

def handle_get(file_name, socket):
    read_file_to_socket(file_name, socket)
    return

def handle_merge(file_name, socket):
    return

def handle_append(file_name, socket):
    return

def handle_create(file_name, socket):
    path = f"src/mp3/fs/{file_name}"
    if os.path.exists(path):
        socket.sendall("ERROR".encode())
    else:
        with open(path, "w") as f:
            f.write("")
        socket.sendall("OK".encode())
    return

    
def handle_client(client_socket: socket.socket, machine_id: str):

    mode = client_socket.recv(1).decode('utf-8')
    print(mode)
    # File size represented with one bytes (max file size: 255)
    file_length = int.from_bytes(client_socket.recv(1), byteorder="little")
    print(file_length)
    file_name = client_socket.recv(file_length).decode('utf-8')
    print(file_name)

    # GET
    if (mode == "G"):
        handle_get(file_name, client_socket)
    # MERGE
    elif (mode == "M"):
        handle_merge(file_name, client_socket)
    # Append
    elif (mode == "A"):
        handle_append(file_name, client_socket)
    # Create
    elif (mode == "C"):
        handle_create(file_name, client_socket)
    
    client_socket.close()

def start_server(machine_id: str):
    
    """
    
    Creates a server that listens on a specified port and handles client connections.
    It constantly waits for new connections and creates a new thread to handle each client connection.

    Parameters:
        machine_id (str): The ID of the machine.
    
    """
    
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    
    # Lets server reuse address so that it can relaunch quickly
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((HOSTS[machine_id - 1], FILE_SYSTEM_PORT))
    server.listen(MAX_CLIENTS)
    
    while True:
        print("Getting client")
        client_socket, _ = server.accept()
        
        # Creates a new thread for each client
        client_handler = threading.Thread(target=handle_client, args=(client_socket, machine_id,))
        
        # sets daemon to true so that there is no need of joining threads once thread finishes
        client_handler.daemon = True
        client_handler.start()

print("a")
machine_id = int(sys.argv[1])
start_server(machine_id)
