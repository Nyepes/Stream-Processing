import threading
import socket
import sys
import os
from time import sleep

from src.shared.constants import FILE_SYSTEM_PORT, HOSTS, MAX_CLIENTS, RECEIVE_TIMEOUT
from src.shared.DataStructures.mem_table import MemTable
from src.shared.DataStructures.Dict import Dict

from src.mp3.shared import generate_sha1, id_from_ip, get_machines, request_append_file, send_file, get_receiver_id_from_file, get_replica_ids, get_server_file_path, get_server_file_metadata, write_server_file_metadata, request_create_file
from src.mp3.constants import REPLICATION_FACTOR, INIT_FILE_METADATA

memtable = None
member_list = None
machine_id = int(sys.argv[1])

ownership_list = None

# merge_counters = None # filename: counter (string, int)

def handle_get(file_name, socket, client_version = 0):

    file_version = memtable.get_file_version(file_name)
    if (file_version is None):
        file_version = get_server_file_metadata(file_name)["version"]
    
    print(f"versions {client_version} {file_version}")
    
    if (client_version == file_version):
        socket.sendall(client_version.to_bytes(4, byteorder="little"))
        return
    
    send_file(socket, get_server_file_path(file_name), file_version)
    in_memory = memtable.get(file_name)
    
    for chunk, _ in in_memory:
        if (chunk is None):
            continue
        socket.sendall(chunk)

def handle_merge(file_name, s, ip_address):
    
    data = memtable.get(file_name)
    file_version = memtable.get_file_version(file_name)

    if file_version is None: 
        file_version = 0

    s.sendall(file_version.to_bytes(4, byteorder="little")) # send file version

    for chunk, status in data: # send memtable data
        if not chunk: continue
        s.sendall(len(chunk).to_bytes(8, byteorder="little") + chunk + status.encode())
    
    s.shutdown(socket.SHUT_WR) # close write end

    new_version = int.from_bytes(s.recv(4), byteorder="little") # recieve new version
    
    with open(get_server_file_path(file_name), "a") as f: # append new data

        for chunk, status in data:
            print(chunk, status)
            if not chunk or status == "N": continue
            f.write(chunk[:-1].decode('utf-8'))
            f.write("\n")
        
        while (1):
            data = s.recv(1024 * 1024)
            if (data == b''): break
            f.write(data.decode('utf-8'))
        
    memtable.set_file_version(file_name, new_version)
    metadata = get_server_file_metadata(file_name)
    metadata["version"] = new_version
    write_server_file_metadata(file_name, metadata)
    memtable.clear(file_name)

def handle_append(file_name, socket, status): 
    
    while (1):
        data = socket.recv(1024 * 1024)
        if (data == b''): break
        memtable.add(file_name, data, status)

    version = memtable.get_file_version(file_name)

    if (version is None):
        version = get_server_file_metadata(file_name)["version"]
    
    memtable.set_file_version(file_name, version + 1)

    socket.sendall("OK".encode())
    socket.close()

def handle_create(file_name, socket):
    
    path = get_server_file_path(file_name)
    
    if os.path.exists(path):
        
        socket.sendall("ERROR".encode())
    
    else:
       
        with open(path, "w") as f:
            f.write("")
        
        metadata = get_server_file_metadata(file_name)
        metadata = INIT_FILE_METADATA
        metadata["version"] += 1
        
        write_server_file_metadata(file_name, metadata)

        ownership = get_receiver_id_from_file(0, file_name)
        ownership_list.increment_list(ownership, file_name)
        print(f"Ownership: {ownership_list.items()}")

        socket.sendall("OK".encode())

def in_range(start, end, val):

    if start < end: 
        return start < val <= end
    else: # loop around ring
        return start < val or 0 <= val <= end

def send_files_by_id(id_to_send, client_socket):

    my_files = ownership_list.get(machine_id)
    my_files_updated = []
    
    for file in my_files:
        
        file_hash = generate_sha1(file)
        client_socket.sendall(len(file).to_bytes(1, byteorder="little"))
        client_socket.sendall(file.encode())
        file_path = get_server_file_path(file)
        file_size = os.path.getsize(file_path).to_bytes(8, byteorder="little")
        client_socket.sendall(file_size)
        file_version = get_server_file_metadata(file)["version"]
        client_socket.sendall(file_version)
        
        send_file(client_socket, file_path)  
        
        if (in_range(file_hash, machine_id, id_to_send)):
            ownership_list.increment_list(id_to_send, file)
        else:
            my_files_updated.append(file)

    ownership_list.add(machine_id, my_files_updated)
        
def handle_client(client_socket: socket.socket, machine_id: str, ip_address: str):

    global member_list

    mode = client_socket.recv(1).decode('utf-8')
    file_length = int.from_bytes(client_socket.recv(1), byteorder="little")
    file_name = client_socket.recv(file_length).decode('utf-8')

    # GET
    if (mode == "G"):
        file_version = int.from_bytes(client_socket.recv(4), byteorder="little")
        handle_get(file_name, client_socket, client_version = file_version)
    
    # MERGE
    elif (mode == "M"):
        handle_merge(file_name, client_socket, ip_address)
    
    # Append
    elif (mode == "A"):
        status = client_socket.recv(1).decode('utf-8')
        handle_append(file_name, client_socket, status)
   
    # Create
    elif (mode == "C"):
        handle_create(file_name, client_socket)
    
    # Start Merge
    elif (mode == "P"):
        merge_file(file_name)
    
    elif (mode == "J"):
        member_list = get_machines() # Update member list as new node joined
        send_files_by_id(int(file_name), client_socket)

    client_socket.close()

def request_merge(id, file_name):
    
    try:
        
        # init connection
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.settimeout(RECEIVE_TIMEOUT)
        server.connect((HOSTS[id - 1], FILE_SYSTEM_PORT))
        
        # create and send payload
        length = len(file_name).to_bytes(1, byteorder='little')
        server.sendall(b"M" + length + file_name.encode())
        
        return server # return socket
    
    except (ConnectionRefusedError, socket.timeout):
        print("Connection Refused")
        return -1

    except (OSError):
        print("OS ERROR")
        return -2

def merge_file(file_name):
    
    file_id = generate_sha1(file_name) # Num between 1 and 10

    buffer = [] * (REPLICATION_FACTOR - 1)
    sockets = []

    replicas = get_replica_ids(file_id)

    for replica_id in replicas:

        if (machine_id == replica_id):
            continue

        print(f"Replica id: {replica_id}")

        replica_socket = request_merge(replica_id, file_name)
        if (replica_socket != -1 and replica_socket != -2):
            sockets.append(replica_socket)

    max_version = memtable.get_file_version(file_name) # get current version

    if (max_version is None):
        max_version = get_server_file_metadata(file_name)["version"]

    for i, s in enumerate(sockets):
        
        max_version = max(max_version, int.from_bytes(s.recv(4), byteorder="little")) # recieve version and update
        
        while (1): # recieve memtable data
            
            read = s.recv(8)
            
            if (read == b''): break
            
            chunk_size = int.from_bytes(read, byteorder="little")
            content = s.recv(chunk_size)
            status = s.recv(1)
            
            buffer.append((content.decode('utf-8'), status.decode('utf-8')))
            
    new_version = max_version + 1
    for s in sockets:

        s.sendall(new_version.to_bytes(4, byteorder="little")) # send new version
        
        for chunk, status in memtable.get(file_name): # head replica
            if not chunk or status == "F": continue
            s.sendall(chunk[:-1] + "\n".encode())
        
        for chunk, status in buffer: # other replicas
            if not chunk or status == "F": continue
            s.sendall(chunk.encode() + "\n".encode())
    
        s.shutdown(socket.SHUT_WR) # close current socket

    wrote = False
    with open(get_server_file_path(file_name), "a") as file:
        
        for chunk, status in memtable.get(file_name):
            if not chunk or status == "F": continue
            file.write(chunk.decode('utf-8'))

        for chunk, status in buffer:
            if not chunk or status == "F": continue
            file.write(chunk)
            file.write('\n')

    memtable.set_file_version(file_name, max_version + 1)
    metadata = get_server_file_metadata(file_name)
    metadata["version"] = new_version
    write_server_file_metadata(file_name, metadata)
    memtable.clear(file_name)

def handle_failed(failed_nodes, member_list):

    # Handle failed 

    machines_sorted = list(member_list) + [machine_id]
    machines_sorted.sort()
    
    my_idx = machines_sorted.index(machine_id)
    prev_idx = (my_idx - 1) % len(machines_sorted)
    prev_id = machines_sorted[prev_idx]
    
    # Head replica fails

    slave_files = []
    for node in failed_nodes:

        if node == prev_id:
            
            slave_files += ownership_list.get(node)
            
        idx = machines_sorted.index(node)
        new_owner = machines_sorted[(idx + 1) % len(machines_sorted)]
        print(ownership_list.get(node))
        ownership_list.increment_list(new_owner, ownership_list.get(node))
        ownership_list.delete(node)

    for file_name in slave_files:
        
        receiver_id = machines_sorted[(my_idx + REPLICATION_FACTOR - 1) % len(machines_sorted)]
        
        request_create_file(receiver_id, file_name)
        request_append_file(receiver_id, file_name, get_server_file_path(file_name), "F")

    # Other replica fails

def handle_joined():
    if (len(member_list) <= 0): return
    mem_set = list(member_list)
    mem_set.sort()
    succesor = 0
    for i, id in enumerate(mem_set):
        if (id >= machine_id):
            succesor = i
    request_files_by_id(machine_id, mem_set[succesor])
    
    predecessor_1 = (succesor - 1) % len(member_list)
    if (predecessor_1 != succesor):
        request_files_by_id(machine_id, mem_set[predecessor_1])

    if (len(mem_set) >= 2):
        predecessor_2 = (succesor - 2) % len(member_list)
        request_files_by_id(machine_id, mem_set[predecessor_2])

def get_files_hash():
    file_hashes = {}
    for filename in os.listdir("src/mp3/fs"):
        file_path = get_server_file_path(filename)
        if os.path.isfile(file_path):  # Only process files, not directories
            file_hashes[filename] = generate_sha1(filename)
    return file_hashes

def check_memlist():

    global member_list

    member_list = set(member_list)
    new_members = set(get_machines())
    
    failed = member_list - new_members
    print(f"failed: {failed}")
    print(f"ownership: {ownership_list.items()}")
    print(f"memtable: {memtable.items()}")

    if (len(failed) > 0):
        handle_failed(failed, member_list)

    member_list = new_members

def request_files_by_id(from_id, to):
    
    try:    
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
            
            server.settimeout(RECEIVE_TIMEOUT)
            server.connect((HOSTS[to - 1], FILE_SYSTEM_PORT))
            
            val = 1
            length = val.to_bytes(1, byteorder='little')
            
            server.sendall(b"J" + length + (str(from_id)).encode())
            
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
        print(f"sending filename: {filename} ...")

        if (get_receiver_id_from_file(0, filename) != machine_id):
            ownership_list.increment_list(id_from_ip(sock.getpeername()[0]), filename)
        else:
            ownership_list.increment_list(machine_id, filename)

        file_content_size = int.from_bytes(sock.recv(8), byteorder="little")
        file_version = int.from_bytes(sock.recv(4), byteorder="little")
        metadata = INIT_FILE_METADATA
        metadata["version"] = file_version
        write_server_file_metadata(filename, metadata)

        with open(get_server_file_path(filename), "ab") as file:

            read = 0

            while (1):
                
                content = sock.recv(min(1024 * 1024, file_content_size - read))
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
    server.settimeout(RECEIVE_TIMEOUT)

    server.bind((HOSTS[machine_id - 1], FILE_SYSTEM_PORT))
    server.listen(MAX_CLIENTS)
    
    global memtable
    memtable = MemTable()
    
    sleep(7)
    
    global member_list
    
    member_list = set(get_machines())
    handle_joined()

    global ownership_list
    ownership_list = Dict(t=list) # int -> [str]
    
    print("starting server...")

    while True:
        try:
            client_socket, ip_address = server.accept()
        except (ConnectionRefusedError, socket.timeout):
            check_memlist()
            continue

        print(f"connecting with: {ip_address}")
        
        # Creates a new thread for each client
        client_handler = threading.Thread(target=handle_client, args=(client_socket, machine_id, ip_address,))
        
        # sets daemon to true so that there is no need of joining threads once thread finishes
        client_handler.daemon = True
        client_handler.start()

if __name__ == "__main__":
    start_server(machine_id)
