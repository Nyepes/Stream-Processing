import threading
import socket
import sys
import os

from src.shared.constants import FILE_SYSTEM_PORT, HOSTS, MAX_CLIENTS
from src.shared.DataStructures import Dict
from src.mp3.shared import read_file_to_socket, generate_sha1, id_from_ip
from src.mp3.mem_table import MemTable

memtable = None
# merge_counters = None # filename: counter (string, int)

def handle_get(file_name, socket):
    read_file_to_socket(file_name, socket)
    return

def handle_merge(file_name, socket, ip_address):
    # file_hash = generate_sha1(file_name)
    # 
    # sender_id = id_from_ip(ip_address)

    while (1):
        data = memtable.get(file_name)
        for chunk in data:
            socket.sendall(chunk.encode())

    with open(f"fs/{file_name}", "a") as f:
        while (1):
            data = socket.recv(1024 * 1024)
            if (data == b''): return
            f.write(data.decode('utf-8'))

    memtable.clear(file_name)
    socket.sendall("OK".encode())
    return

def handle_append(file_name, socket): 
    while (1):
        data = socket.recv(1024 * 1024)
        if (data == b''): return
        should_merge = memtable.add(file_name, data)
    socket.sendall("OK".encode())

def handle_create(file_name, socket):
    path = f"src/mp3/fs/{file_name}"
    if os.path.exists(path):
        socket.sendall("ERROR".encode())
    else:
        with open(path, "w") as f:
            f.write("")
        socket.sendall("OK".encode())
    return
 
def handle_client(client_socket: socket.socket, machine_id: str, ip_address: str):

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
        handle_merge(file_name, client_socket, ip_address)
    # Append
    elif (mode == "A"):
        handle_append(file_name, client_socket)
    # Create
    elif (mode == "C"):
        handle_create(file_name, client_socket)
    # Start Merge
    elif (mode == "P"):
        merge_file(file_name)
    
    client_socket.close()

def request_merge(machine_id):
    try:    
        server= socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.settimeout(RECEIVE_TIMEOUT)
        server.connect((HOSTS[machine_id - 1], FILE_SYSTEM_PORT))
        server.sendall(b"M")  
        return socket
    except (ConnectionRefusedError, socket.timeout):
        return -1
    except (OSError):
        return -2

def merge_file(file_name):
    file_id = generate_sha1(file_name)
    # At 0 we have id 0 mem table at 1 we have 1 memtable and at 2 we have 2 memtable
    buffer = [""] * (REPLICATION_FACTOR - 1)
    sockets = []
    for i in range(REPLICATION_FACTOR):
        replica_id = (file_id + i) % 10 + 1
        if (my_id == replica_id): 
            continue
        sockets.append(request_merge(replica_id))
    
    for i, socket in enumerate(sockets):
        while (1):
            data = socket.recv(1024 * 1024)
            if (data == b''): return
            buffer[i] += data
        
    for i, socket in enumerate(sockets):
        for chunk in buffer:
            socket.sendall(chunk)
    
    for socket in sockets:
        try:
            status = socket.recvfrom(2).decode('utf-8')
            if (status != "OK"):
                raise ConnectionRefusedError
        except (ConnectionRefusedError, socket.timeout):
            print("Merge Failed")
            return -1

    with open(f"fs/{file_name}", "a") as file:
        for chunk in self.data:
                file.write(chunk)
    
def start_server(machine_id: int):
    
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
    
    global memtable
    memtable = MemTable()

    while True:
        client_socket, ip_address = server.accept()
        print(f"connecting with: {ip_address}")
        
        # Creates a new thread for each client
        client_handler = threading.Thread(target=handle_client, args=(client_socket, machine_id, ip_address,))
        
        # sets daemon to true so that there is no need of joining threads once thread finishes
        client_handler.daemon = True
        client_handler.start()

machine_id = int(sys.argv[1])
start_server(machine_id)

