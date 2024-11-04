import threading
import socket
import sys
import os
from time import sleep

from src.shared.constants import FILE_SYSTEM_PORT, HOSTS, MAX_CLIENTS, RECEIVE_TIMEOUT
from src.shared.DataStructures import Dict
from src.mp3.shared import read_file_to_socket, generate_sha1, id_from_ip, get_machines, send_file
from src.mp3.constants import REPLICATION_FACTOR
from src.mp3.mem_table import MemTable

memtable = None
member_list = None
machine_id = int(sys.argv[1])

# merge_counters = None # filename: counter (string, int)

def handle_get(file_name, socket):
    read_file_to_socket(file_name, socket)
    return

def handle_merge(file_name, s, ip_address):
    print("handle_merge")
    data = memtable.get(file_name)
    for chunk in data:
        if (not chunk): continue
        s.sendall(chunk)
    s.shutdown(socket.SHUT_WR)
    with open(f"src/mp3/fs/{file_name}", "a") as f:
        while (1):
            data = s.recv(1024 * 1024)
            print("a")
            if (data == b''): break
            print("1")
            f.write(data.decode('utf-8'))

    memtable.clear(file_name)
    return

def handle_append(file_name, socket): 
    while (1):
        print("loopy")
        data = socket.recv(1024 * 1024)
        if (data == b''): return
        memtable.add(file_name, data)
    print("DOne")
    socket.sendall("OK".encode())
    socket.close()

def handle_create(file_name, socket):
    path = f"src/mp3/fs/{file_name}"
    if os.path.exists(path):
        socket.sendall("ERROR".encode())
    else:
        with open(path, "w") as f:
            f.write("")
        socket.sendall("OK".encode())
    return

def in_range(start, end, val):

    if start < end: 
        return start < val <= end
    else: # loop around ring
        return start < val or 0 <= val <= end

def send_files_by_id(id_to_send, client_socket):
    hashes = get_files_hash()

    machines = get_machines() + [machine_id]
    machines.sort()

    predecessor = (machines.index(machine_id) - 1) % len(machines)
    predecessor_id = machines[predecessor]

    for file, hash in hashes.items():
        h = hash % 10 + 1
        print(f"{predecessor_id} < {h} <= {machine_id}")
        if (in_range(predecessor_id, machine_id, h)):
            file_path = "src/mp3/fs/" + file
            client_socket.sendall(len(file).to_bytes(1, byteorder="little"))
            client_socket.sendall(file.encode())
            file_size = os.path.getsize(file_path).to_bytes(8, byteorder="little")
            client_socket.sendall(file_size)
            send_file(client_socket, file_path)        
    

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
    elif (mode == "J"):
        send_files_by_id(int(file_name), client_socket)
    client_socket.close()

def request_merge(machine_id, file_name):
    try:
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.settimeout(RECEIVE_TIMEOUT)
        server.connect((HOSTS[machine_id - 1], FILE_SYSTEM_PORT))
        length = len(file_name).to_bytes(1, byteorder='little')
        server.sendall(b"M" + length + file_name.encode())
        return server
    except (ConnectionRefusedError, socket.timeout):
        print("Connection Refucse")
        return -1
    except (OSError):
        print("OS ERROR")
        return -2

def merge_file(file_name):
    file_id = generate_sha1(file_name)
    # At 0 we have id 0 mem table at 1 we have 1 memtable and at 2 we have 2 memtable
    buffer = [""] * (REPLICATION_FACTOR - 1)
    sockets = []
    for i in range(REPLICATION_FACTOR):
        replica_id = (file_id + i) % 10 + 1
        if (machine_id == replica_id): 
            continue
        sockets.append(request_merge(replica_id, file_name))
    
    for i, s in enumerate(sockets):
        while (1):
            data = s.recv(1024 * 1024)
            print(data)
            if (data == b'' or not data): break
            buffer[i] += data.decode('utf-8')
        
    print(buffer)
    for i, s in enumerate(sockets):
        for chunk in memtable.get(file_name):
            s.sendall(chunk)
        for chunk in buffer:
            print("send")
            s.sendall(chunk.encode())
    
    s.shutdown(socket.SHUT_WR)

    with open(f"src/mp3/fs/{file_name}", "a") as file:
        for chunk in memtable.get(file_name):
            file.write(chunk.decode('utf-8'))
        for chunk in buffer:
            file.write(chunk)
        file.write('\n')
    memtable.clear(file_name)

def handle_failed():
    # TODO
    return

def handle_joined():
    if (len(member_list) <= 0): return
    print('a')

    mem_set = list(member_list)
    print(mem_set)
    mem_set.sort()
    succesor = 0
    for i, id in enumerate(mem_set):
        if (id >= machine_id):
            succesor = i
    print(f"req: {succesor}")
    request_files_by_id(machine_id, mem_set[succesor])
    
    predecessor_1 = (succesor - 1) % len(member_list)
    print(f"req: {predecessor_1}")
    if (predecessor_1 != succesor):
        request_files_by_id(machine_id, mem_set[predecessor_1])

    if (len(mem_set) >= 2):
        predecessor_2 = (succesor - 2) % len(member_list)
        request_files_by_id(machine_id, mem_set[predecessor_2])
        print(f"req: {2}")


def get_files_hash():
    file_hashes = {}
    for filename in os.listdir("src/mp3/fs"):
        file_path = os.path.join("src/mp3/fs", filename)
        if os.path.isfile(file_path):  # Only process files, not directories
            file_hashes[filename] = generate_sha1(filename)
    return file_hashes

def check_memlist():
    global member_list
    new_members = set(get_machines())
    
    failed = member_list - new_members
    joined = new_members - member_list


    handle_failed(failed)

    member_list = new_members


def request_files_by_id(from_id, to):
    try:    
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
            server.settimeout(RECEIVE_TIMEOUT)
            server.connect((HOSTS[to - 1], FILE_SYSTEM_PORT))
            val = 1
            length = val.to_bytes(1, byteorder='little')
            server.sendall(b"J" + length + (str(from_id)).encode())
            print("sent")
            write_requested_files(server)
    except (ConnectionRefusedError, socket.timeout):
        return -1
    except (OSError):
        return -2

def write_requested_files(sock):

    while (1):
        more_files = sock.recv(1) # filename size

        if (more_files == b''): return

        filename_size = int.from_bytes(more_files, byteorder="little")

        filename = sock.recv(filename_size).decode()
        print(f"filename: {filename}")

        file_content_size = int.from_bytes(sock.recv(8), byteorder="little")

        with open(f"src/mp3/fs/{filename}", "ab") as file:

            read = 0

            while (1):
                
                content = sock.recv(min(1024 * 1024, file_content_size - read))
                print(content)
                read += len(content)
                file.write(content)

                if read == file_content_size:
                    break



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
    # server.settimeout(RECEIVE_TIMEOUT)

    server.bind((HOSTS[machine_id - 1], FILE_SYSTEM_PORT))
    server.listen(MAX_CLIENTS)
    
    global memtable
    memtable = MemTable()

    global member_list
    sleep(7)
    member_list = set(get_machines())
    handle_joined()
    print("ok")
    while True:
    
        client_socket, ip_address = server.accept()

        print(f"connecting with: {ip_address}")
        
        # Creates a new thread for each client
        client_handler = threading.Thread(target=handle_client, args=(client_socket, machine_id, ip_address,))
        
        # sets daemon to true so that there is no need of joining threads once thread finishes
        client_handler.daemon = True
        client_handler.start()

start_server(machine_id)

