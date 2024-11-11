from collections import defaultdict
import threading
import socket
import sys
import os
from time import sleep
from datetime import datetime

from src.shared.constants import FILE_SYSTEM_PORT, HOSTS, MAX_CLIENTS, RECEIVE_TIMEOUT
from src.shared.DataStructures.mem_table import MemTable
from src.shared.DataStructures.Dict import Dict

from src.mp3.shared import generate_sha1, id_from_ip, get_machines, request_append_file, send_file, get_receiver_id_from_file, get_replica_ids, get_server_file_path, get_server_file_metadata, write_server_file_metadata, request_create_file
from src.mp3.constants import REPLICATION_FACTOR, INIT_FILE_METADATA, GET, MERGE, APPEND, CREATE, START_MERGE, JOIN, MULTIAPPEND_REQUEST

memtable = None  # Global variable to hold the memory table
member_list = None  # Global variable to hold the list of members in the system
machine_id = int(sys.argv[1])  # Machine ID is passed as a command line argument

ownership_list = None  # Global variable to hold the ownership list of files

# merge_counters = None # filename: counter (string, int)

def handle_get(file_name, socket, client_version=0):
    """
    Handles the GET request for a file.

    Parameters:
        file_name (str): The name of the file to retrieve.
        socket (socket.socket): The socket to send data back to the client.
        client_version (int): The version of the file requested by the client.
    """
    file_version = memtable.get_file_version(file_name)
    if (file_version is None):
        file_version = get_server_file_metadata(file_name)["version"]
    
    # Check if the client's version matches the server's version
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
    """
    Handles the MERGE request for a file.

    Parameters:
        file_name (str): The name of the file to merge.
        s (socket.socket): The socket to communicate with the client.
        ip_address (str): The IP address of the client.
    """
    data = memtable.get(file_name)
    file_version = memtable.get_file_version(file_name)

    if file_version is None: 
        file_version = 0

    s.sendall(file_version.to_bytes(4, byteorder="little"))  # send file version

    for chunk, status in data:  # send memtable data
        if not chunk: continue
        s.sendall(len(chunk).to_bytes(8, byteorder="little") + chunk + status.encode())
    
    s.shutdown(socket.SHUT_WR)  # close write end

    new_version = int.from_bytes(s.recv(4), byteorder="little")  # receive new version
    
    with open(get_server_file_path(file_name), "a") as f:  # append new data
        for chunk, status in data:
            if not chunk or status == "N": continue
            f.write(chunk.decode('utf-8'))
        
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
    """
    Handles the APPEND request for a file.

    Parameters:
        file_name (str): The name of the file to append data to.
        socket (socket.socket): The socket to receive data from the client.
        status (str): The status of the append operation.
    """
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
    """
    Handles the CREATE request for a file.

    Parameters:
        file_name (str): The name of the file to create.
        socket (socket.socket): The socket to send responses back to the client.
    """
    path = get_server_file_path(file_name)
    
    if os.path.exists(path):
        socket.sendall("ERROR".encode())
    else:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w") as f:
            f.write("")
        
        metadata = get_server_file_metadata(file_name)
        metadata = INIT_FILE_METADATA
        metadata["version"] += 1
        
        write_server_file_metadata(file_name, metadata)

        ownership = get_receiver_id_from_file(0, file_name)
        ownership_list.increment_list(ownership, file_name)

        socket.sendall("OK".encode())

def in_range(start, end, val):
    """
    Checks if a value is in a specified range, considering wrap-around.

    Parameters:
        start (int): The start of the range.
        end (int): The end of the range.
        val (int): The value to check.

    Returns:
        bool: True if the value is in the range, False otherwise.
    """
    if start < end: 
        return start < val <= end
    else:  # loop around ring
        return start < val or 0 <= val <= end

def send_files_by_id(id_to_send, client_socket, succ):
    """
    Sends files to a client based on their ID.

    Parameters:
        id_to_send (int): The ID to send files to.
        client_socket (socket.socket): The socket to send files through.
        succ (bool): Indicates if the sending is to a successor.
    """
    my_files = ownership_list.get(machine_id)
    my_files_updated = []
    
    for file in my_files:
        file_hash = generate_sha1(file)
        
        if ((succ and file_hash <= id_to_send) or not succ):
            client_socket.sendall(len(file).to_bytes(1, byteorder="little"))
            client_socket.sendall(file.encode())

            file_path = get_server_file_path(file)
            file_size = os.path.getsize(file_path).to_bytes(8, byteorder="little")
            
            client_socket.sendall(file_size)
            file_version = get_server_file_metadata(file)["version"]

            client_socket.sendall(file_version.to_bytes(4, byteorder="little"))
            
            send_file(client_socket, file_path)  
        
        if (file_hash <= id_to_send):  # belong to the new node
            ownership_list.increment_list(id_to_send, file)
        else:
            my_files_updated.append(file)  # still belong to me

    ownership_list.add(machine_id, my_files_updated)  # remove files that now belong to the new node
        
def handle_client(client_socket: socket.socket, machine_id: str, ip_address: str):
    """
    Handles incoming client connections and requests.

    Parameters:
        client_socket (socket.socket): The socket for the client connection.
        machine_id (str): The ID of the machine handling the request.
        ip_address (str): The IP address of the client.
    """
    global member_list

    mode = client_socket.recv(1).decode('utf-8')
    file_length = int.from_bytes(client_socket.recv(1), byteorder="little")
    file_name = client_socket.recv(file_length).decode('utf-8')

    # GET
    if (mode == GET):
        file_version = int.from_bytes(client_socket.recv(4), byteorder="little")
        handle_get(file_name, client_socket, client_version=file_version)
    
    # MERGE
    elif (mode == MERGE):
        handle_merge(file_name, client_socket, ip_address)
    
    # Append
    elif (mode == APPEND):
        status = client_socket.recv(1).decode('utf-8')
        handle_append(file_name, client_socket, status)
   
    # Create
    elif (mode == CREATE):
        handle_create(file_name, client_socket)
    
    # Start Merge
    elif (mode == START_MERGE):
        merge_file(file_name)
    
    elif (mode == MULTIAPPEND_REQUEST):
        
        member_list = get_machines() # Update member list as new node joined
        
        nodes = list(member_list) + [machine_id]
        nodes.sort()

        ip_address = socket.gethostbyaddr(ip_address[0])[0]
        node_id = id_from_ip(ip_address)
        
        succ = nodes[(nodes.index(node_id) + 1) % len(nodes)] == machine_id
        
        send_files_by_id(int(file_name), client_socket, succ)
    
    elif (mode == "Q"):
        server_file = file_name
        local_file_length = int.from_bytes(client_socket.recv(1), byteorder="little")
        
        local_file_name = client_socket.recv(local_file_length).decode('utf-8')

        server_id = get_receiver_id_from_file(machine_id, file_name)
        res = request_append_file(server_id, server_file, local_file_name, "N")

    client_socket.close()

def request_merge(id, file_name):
    """
    Requests to merge a file with a specified ID.

    Parameters:
        id (int): The ID of the machine to merge with.
        file_name (str): The name of the file to merge.
    
    Returns:
        socket.socket: The socket connection to the server, or -1/-2 on error.
    """
    try:
        # init connection
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.settimeout(RECEIVE_TIMEOUT)
        server.connect((HOSTS[id - 1], FILE_SYSTEM_PORT))
        
        # create and send payload
        length = len(file_name).to_bytes(1, byteorder='little')
        server.sendall(b"M" + length + file_name.encode())
        
        return server  # return socket
    
    except (ConnectionRefusedError, socket.timeout):
        return -1

    except (OSError):
        return -2

def merge_file(file_name):
    """
    Merges a file with its replicas.

    Parameters:
        file_name (str): The name of the file to merge.
    """
    file_id = generate_sha1(file_name)  # Num between 1 and 10

    buffer = [] * (REPLICATION_FACTOR - 1)
    sockets = []

    replicas = get_replica_ids(file_id)
    for replica_id in replicas:
        if (machine_id == replica_id):
            continue

        replica_socket = request_merge(replica_id, file_name)
        if (replica_socket != -1 and replica_socket != -2):
            sockets.append(replica_socket)

    max_version = memtable.get_file_version(file_name)  # get current version

    if (max_version is None):
        max_version = get_server_file_metadata(file_name)["version"]

    for i, s in enumerate(sockets):
        max_version = max(max_version, int.from_bytes(s.recv(4), byteorder="little"))  # receive version and update
        
        while (1):  # receive memtable data
            read = s.recv(8)
            if (read == b''): break
            
            chunk_size = int.from_bytes(read, byteorder="little")
            content = s.recv(chunk_size)
            status = s.recv(1)
            
            buffer.append((content.decode('utf-8'), status.decode('utf-8')))
            
    new_version = max_version + 1
    for s in sockets:
        s.sendall(new_version.to_bytes(4, byteorder="little"))  # send new version
        
        for chunk, status in memtable.get(file_name):  # head replica
            if not chunk or status == "F": continue
            s.sendall(chunk)  # REVISE chunk[:-1]
        
        for chunk, status in buffer:  # other replicas
            if not chunk or status == "F": continue
            s.sendall(chunk.encode())
    
        s.shutdown(socket.SHUT_WR)  # close current socket

    wrote = False
    with open(get_server_file_path(file_name), "a") as file:
        for chunk, status in memtable.get(file_name):
            if not chunk or status == "F": continue
            file.write(chunk.decode('utf-8'))

        for chunk, status in buffer:
            if not chunk or status == "F": continue
            file.write(chunk)

    memtable.set_file_version(file_name, max_version + 1)
    metadata = get_server_file_metadata(file_name)
    metadata["version"] = new_version
    write_server_file_metadata(file_name, metadata)
    memtable.clear(file_name)

def handle_failed(failed_nodes, member_list):
    """
    Handles the failure of nodes in the system.

    Parameters:
        failed_nodes (list): The list of failed nodes.
        member_list (set): The current list of members in the system.
    """
    time_init = datetime.now()

    machines_sorted = list(member_list) + [machine_id]
    machines_sorted.sort()

    my_idx = machines_sorted.index(machine_id)

    # Head replica fails
    prev_idx = (my_idx - 1) % len(machines_sorted)
    prev_id = machines_sorted[prev_idx]

    slave_files = []
    for node in failed_nodes:
        if node == prev_id:
            slave_files += ownership_list.get(node)
            
        idx = machines_sorted.index(node)
        new_owner = machines_sorted[(idx + 1) % len(machines_sorted)]
        ownership_list.increment_list(new_owner, ownership_list.get(node))
        ownership_list.delete(node)

    for file_name in slave_files:
        receiver_id = machines_sorted[(my_idx + REPLICATION_FACTOR - 1) % len(machines_sorted)]
        
        request_create_file(receiver_id, file_name)
        request_append_file(receiver_id, file_name, get_server_file_path(file_name), "F")

    # Other replica fails
    for node in failed_nodes:
        idx = machines_sorted.index(node)

        affected_replica_heads = [machines_sorted[(idx - 1 - i) % len(machines_sorted)] for i in range(REPLICATION_FACTOR - 1)]
        
        if machine_id in affected_replica_heads:
            for file_name in ownership_list.get(machine_id):
                receiver_id = machines_sorted[(my_idx + REPLICATION_FACTOR) % len(machines_sorted)]  # don't subtract 1 as failed node still in machines sorted
                
                request_create_file(receiver_id, file_name)
                request_append_file(receiver_id, file_name, get_server_file_path(file_name), "F")
    time_end = datetime.now()
    print(f"start: {time_end - time_init}")

def handle_joined_initial():
    """
    Handles the initial joining of a new node to the system.
    """
    if (len(member_list) <= 0): return

    mem_set = list(member_list)
    mem_set.sort()

    succesor = 0
    for i, id in enumerate(mem_set):
        if (id >= machine_id):
            succesor = i
            break  # F my life

    request_files_by_id(machine_id, mem_set[succesor])

def handle_joined(joined, member_set): 
    """
    Handles the joining of new nodes to the system.

    Parameters:
        joined (list): The list of nodes that have joined.
        member_set (set): The current set of members in the system.
    """
    machines_sorted = list(member_set) + [machine_id] + list(joined)
    machines_sorted.sort()

    my_idx = machines_sorted.index(machine_id)

    # POST JOIN NODE
    for node in joined:
        node_idx = machines_sorted.index(node)
        post_affected_nodes = [machines_sorted[(node_idx + 1 + i) % len(machines_sorted)] for i in range(REPLICATION_FACTOR)]

        if machine_id in post_affected_nodes:
            if machine_id == post_affected_nodes[-1]:
                prev_head = machines_sorted[(my_idx - REPLICATION_FACTOR + 1) % len(machines_sorted)]
            else:
                prev_head = machines_sorted[(my_idx - REPLICATION_FACTOR) % len(machines_sorted)]

            prev_node = machines_sorted[(my_idx - 1) % len(machines_sorted)]
            
            files_to_send = ownership_list.get(prev_head)
            ownership_list.delete(prev_head)

            for file_name in files_to_send:
                if prev_node == node:
                    request_create_file(prev_node, file_name)

                file = open(get_server_file_path(file_name), "wb")
                for chunk, status in memtable.get(file_name):
                    if status == "N":
                        file.write(chunk)
                
                file.close()

                memtable.delete(file_name)

                request_append_file(prev_node, file_name, get_server_file_path(file_name), "N")

                os.remove(get_server_file_path(file_name))
                os.remove(get_server_file_path(f"metadata/{file_name}"))

    # PRE JOIN NODE
    for node in joined:
        node_idx = machines_sorted.index(node)
        pre_affected_nodes = [machines_sorted[(node_idx - 1 - i) % len(machines_sorted)] for i in range(REPLICATION_FACTOR - 1)]

        if machine_id in pre_affected_nodes:
            for file_name in ownership_list.get(machine_id):
                request_create_file(node, file_name)
                request_append_file(node, file_name, get_server_file_path(file_name), "N")

    # Update ownership list
    updated_files = defaultdict(list)
    new_node_files = defaultdict(list)

    for owner, files in ownership_list.items():
        for file in files:
            file_hash = generate_sha1(file)

            flag = False
            for node in joined:
                if file_hash <= node:
                    new_node_files[node].append(file)
                    flag = True
                    break

            if not flag:
                updated_files[owner].append(file)
            else:
                new_files = ownership_list.get(owner)
                if file in new_files:
                    new_files.remove(file)
                ownership_list.add(owner, new_files if new_files else [])

    for node, files in updated_files.items():
        ownership_list.add(node, files)

    for node, files in new_node_files.items():
        ownership_list.add(node, files)

def check_memlist():
    """
    Checks the membership list for failed or newly joined nodes.
    """
    global member_list

    member_list = set(member_list)
    new_members = set(get_machines())
    
    failed = member_list - new_members
    joined = new_members - member_list

    if (len(failed) > 0):
        handle_failed(failed, member_list)
    if (len(joined) > 0):
        handle_joined(joined, member_list)

    member_list = new_members

def request_files_by_id(from_id, to):
    """
    Requests files from a specified node by ID.

    Parameters:
        from_id (int): The ID of the requesting node.
        to (int): The ID of the node to request files from.
    
    Returns:
        int: -1 on connection error, -2 on OS error.
    """
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
    """
    Writes requested files received from a socket.

    Parameters:
        sock (socket.socket): The socket to read data from.
    """
    while (1):
        more_files = sock.recv(1)  # filename size

        if (more_files == b''): return

        filename_size = int.from_bytes(more_files, byteorder="little")

        filename = sock.recv(filename_size).decode()

        if (get_receiver_id_from_file(0, filename) != machine_id):
            to = id_from_ip(sock.gethostbyaddr(sock.getpeername()[0])[0])
            ownership_list.increment_list(to, filename)
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

    global ownership_list
    ownership_list = Dict(t=list)  # int -> [str]

    handle_joined_initial()
    
    while True:
        try:
            client_socket, ip_address = server.accept()
        except (ConnectionRefusedError, socket.timeout):
            check_memlist()
            continue

        # Creates a new thread for each client
        client_handler = threading.Thread(target=handle_client, args=(client_socket, machine_id, ip_address,))
        
        # sets daemon to true so that there is no need of joining threads once thread finishes
        client_handler.daemon = True
        client_handler.start()

if __name__ == "__main__":
    start_server(machine_id)
