import hashlib 
import socket
import json
import os

from src.shared.constants import HOSTS, FILE_SYSTEM_PORT, RECEIVE_TIMEOUT
from src.shared.shared import get_machines
from src.mp3.constants import REPLICATION_FACTOR, INIT_FILE_METADATA

BUFFER_SIZE = 1024

def generate_sha1(input_string):
    """Generate a SHA-1 hash of the input string."""
    sha1_hash = hashlib.sha1()
    sha1_hash.update(input_string.encode('utf-8'))
    return int(sha1_hash.hexdigest(), 16) % 10 + 1 # Num between 1 and 10

def read_file_to_socket(file_name, sock = None):
    """
    Reads a file and sends its content to a socket or prints it to the console.
    
    Parameters:
        file_name (str): The name of the file to read.
        sock (socket.socket, optional): The socket to send data to. If None, print to console.
    """
    try:
        with open(get_server_file_path(file_name), 'r') as file:
            for line in file:
                if sock is None:
                    print(line.strip())
                else: 
                    sock.sendall(line.encode())
    except:
        sock.sendall("Error".encode())

def request_file(machine_id, file_name, output_file, version = 0):
    """
    Requests a file from a specified machine and saves it to the output file.
    
    Parameters:
        machine_id (int): The ID of the machine to request the file from.
        file_name (str): The name of the file to request.
        output_file (str): The path to save the received file.
        version (int, optional): The version of the file requested. Defaults to 0.
    
    Returns:
        int: The version of the file received or an error code.
    """
    try:    
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
            
            # Init connection
            server.settimeout(RECEIVE_TIMEOUT)
            server.connect((HOSTS[machine_id - 1], FILE_SYSTEM_PORT))
            
            # Send payload (G + length of file_name + file_name)    
            length = len(file_name).to_bytes(1, byteorder='little')
            server.sendall(b"G" + length + file_name.encode())
            server.sendall(version.to_bytes(4, byteorder = "little"))
            
            # Receive response
            received_version = int.from_bytes(server.recv(4), byteorder="little")
            # print(version, received_version)
            
            if (version is not None and received_version == version): 
                # print("cache hit")
                return received_version
            
            # Receive file
            with open(output_file, "wb") as output:
                while (1):
                    data = server.recv(BUFFER_SIZE)
                    if (data == b'' or data == "OK"): return received_version #TODO: Maybe Change so that a file that has OK does not fail
                    output.write(data) 

            return received_version

    except (ConnectionRefusedError, socket.timeout):
        return -1
    except (OSError):
        return -2 

def request_create_file(machine_to_request, file_name):
    """
    Requests the creation of a file on a specified machine.
    
    Parameters:
        machine_to_request (int): The ID of the machine to request the file creation.
        file_name (str): The name of the file to create.
    
    Returns:
        int: 0 if successful, -1 if there was an error.
    """
    try:    
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
            
            # Init connection
            server.settimeout(RECEIVE_TIMEOUT)
            server.connect((HOSTS[machine_to_request - 1], FILE_SYSTEM_PORT))
            
            # Send payload (C + length of file_name + file_name)
            length = len(file_name).to_bytes(1, byteorder='little')
            server.sendall(b"C" + length + file_name.encode())
            
            # Receive response
            data = server.recv(BUFFER_SIZE).decode("utf-8")
            
            if (data == b'' or data == "ERROR"): # Expects OK
                return -1   

    except (ConnectionRefusedError, socket.timeout):
        return -1
    except (OSError):
        return -2
    
    return 0

def create(file_name, server_file_name):
    server_id = get_receiver_id_from_file(0, server_file_name)
    my_id = id_from_ip(socket.gethostname())
    
    machines = get_machines() + [my_id]
    machines.sort()

    for i, machine_id in enumerate(machines):
        if (machine_id >= server_id):
            break
    
    res = 0
    for j in range(min(REPLICATION_FACTOR, len(machines))):
        res += request_create_file(machines[(i + j) % len(machines)] , server_file_name) # Creates empty file on all replicas
    
    # server_id = get_receiver_id_from_file(0, file_name)
    if (res != 0): # If any of the replicas already has the file, exit
        print("File already Created")
        return
    
    receiver_id = get_receiver_id_from_file(my_id, server_file_name) # This is replica I'm going to send the actual file content to
    # print(f"append to: {receiver_id}")
    res = request_append_file(receiver_id, server_file_name, file_name, "N")
    receiver_id = get_receiver_id_from_file(0, server_file_name)
    request_merge_file(receiver_id, server_file_name)

    return res

def request_append_file(receiver_id, server_file_name, local_file_name, status):
    """
    Requests to append a file to a specified receiver.
    
    Parameters:
        receiver_id (int): The ID of the receiver machine.
        server_file_name (str): The name of the file on the server.
        local_file_name (str): The name of the local file to append.
        status (str): The status of the file (e.g., 'N' for new).
    
    Returns:
        int: 0 if successful, -1 if there was an error.
    """
    try:    
        
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
            
            # Init connection
            server.settimeout(RECEIVE_TIMEOUT)
            server.connect((HOSTS[receiver_id - 1], FILE_SYSTEM_PORT))
            
            # Send payload (A + length of server_file_name + server_file_name + status)
            length = len(server_file_name).to_bytes(1, byteorder='little')
            server.sendall(b"A" + length + server_file_name.encode() + status.encode())
            
            # Send file
            send_file(server, local_file_name)
            
            # Shutdown connection
            server.shutdown(socket.SHUT_WR)
            
            # Receive response
            data = server.recv(BUFFER_SIZE).decode("utf-8") # Expects OK
            print("DATA RECV APPEND", data)
            if (data == b'' or data == "ERROR"):
                return -1

    except (ConnectionRefusedError, socket.timeout):
        return -1
    except (OSError):
        return -2
    
    return 0

def request_merge_file(receiver_id, server_file_name):
    """
    Requests to merge a file on a specified receiver.
    
    Parameters:
        receiver_id (int): The ID of the receiver machine.
        server_file_name (str): The name of the file to merge.
    
    Returns:
        int: 0 if successful, -1 if there was an error.
    """
    try:    
        
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
            
            # Initialize connection
            server.settimeout(RECEIVE_TIMEOUT)
            server.connect((HOSTS[receiver_id - 1], FILE_SYSTEM_PORT))
            
            # Payload
            length = len(server_file_name).to_bytes(1, byteorder='little')
            server.sendall(b"P" + length + server_file_name.encode())

            # Response
            data = server.recv(BUFFER_SIZE).decode("utf-8")
            # print("DATA MERGE", bin(data))
            # print("DATA RECEIVED", data)
            if (data == b'' or data == "ERROR"): # Expected OK
                return -1   
    
    except (ConnectionRefusedError, socket.timeout):
        return -1
    except (OSError):
        return -2
    
    return 0

def send_file(receiver_socket, file_name, file_version=None):
    """
    Sends a file to a specified socket.
    
    Parameters:
        receiver_socket (socket.socket): The socket to send the file to.
        file_name (str): The name of the file to send.
        file_version (int, optional): The version of the file being sent.
    """
    try:
        # print("sending")
        if (file_version is not None):
            receiver_socket.sendall(file_version.to_bytes(4, byteorder="little"))
        with open(file_name, 'rb') as file:
            while True:
                chunk = file.read(BUFFER_SIZE)      
                if not chunk:
                    break 
                receiver_socket.sendall(chunk)
            # print(f"File {file_name} sent successfully.")
    except FileNotFoundError as e:
        print(f"File {file_name} not found.")
    except (ConnectionRefusedError, socket.timeout) as e:
        print(f"Error: {e}")
    except OSError as e:
        print(f"OS error: {e}")

def get_receiver_id_from_file(my_id, file_name):
    """
    Determines the receiver ID to send a file to based on the file's ID and the current machine's ID.
    
    Parameters:
        my_id (int): The ID of the current machine.
        file_name (str): The name of the file to send.
    
    Returns:
        int: The ID of the receiver machine.
    """
    machines = get_machines() + [id_from_ip(socket.gethostname())]
    machines.sort()

    file_id = generate_sha1(file_name)

    """
    
    if (file_id <= my_id < file_id + REPLICATION_FACTOR and my_id in machines):
        
        return my_id
    
    else:
        
        for i, machine_id in enumerate(machines):
            if (machine_id >= file_id):
                return machines[(i + my_id % REPLICATION_FACTOR) % len(machines)]
                
    """

    head = 0
    for i, machine_id in enumerate(machines):
        if (machine_id >= file_id):
            head = i
            break
    
    replicas = []
    for j in range(REPLICATION_FACTOR):
        replicas.append(machines[(head + j) % len(machines)])

    if (my_id in replicas): # Node is replica itself
        return my_id
    
    return replicas[my_id % REPLICATION_FACTOR] # Arbitrary formula for replica selection (helps with load balancing)

def get_replica_ids(file_id):
    """
    Retrieves the IDs of the replicas for a given file ID.
    
    Parameters:
        file_id (int): The ID of the file.
    
    Returns:
        list: A list of replica IDs.
    """
    machines = get_machines() + [id_from_ip(socket.gethostname())]
    machines.sort()
    
    head = 0
    for i, machine_id in enumerate(machines):
        if (machine_id >= file_id):
            head = i 
            break
    
    replicas = []
    for i in range(REPLICATION_FACTOR):
        replicas.append(machines[(head + i) % len(machines)])
    
    return replicas
    
def get_server_file_path(filename):
    """
    Constructs the server file path for a given filename.
    
    Parameters:
        filename (str): The name of the file.
    
    Returns:
        str: The constructed server file path.
    """
    return f"src/mp3/fs/{filename}"

def get_client_file_path(filename):
    """
    Constructs the client file path for a given filename.
    
    Parameters:
        filename (str): The name of the file.
    
    Returns:
        str: The constructed client file path.
    """
    return f"src/mp3/local_cache/{filename}"

def get_server_file_metadata(filename):
    """
    Retrieves the metadata for a given file from the server.
    
    Parameters:
        filename (str): The name of the file.
    
    Returns:
        dict: The metadata of the file or default metadata if not found.
    """
    try:
        metadata_file_path = f"metadata/{filename}"
        with open(get_server_file_path(metadata_file_path), "r") as f:
            data = json.load(f)
        return data
    except:
        return INIT_FILE_METADATA

def write_server_file_metadata(filename, data):
    """
    Writes metadata for a given file to the server.
    
    Parameters:
        filename (str): The name of the file.
        data (dict): The metadata to write.
    
    Returns:
        int: -1 if there was an error, otherwise returns 0.
    """
    metadata_file_path = f"metadata/{filename}"
    file_path = get_server_file_path(metadata_file_path)
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    try:
        with open(file_path, "w") as f:
            data = json.dump(data, f)
    except:
        return -1

def get_client_file_metadata(filename):
    """
    Retrieves the metadata for a given file from the client.
    
    Parameters:
        filename (str): The name of the file.
    
    Returns:
        dict: The metadata of the file or default metadata if not found.
    """
    try:
        metadata_file_path = f"metadata/{filename}"
        with open(get_client_file_path(metadata_file_path), "r") as f:
            data = json.load(f)
        return data
    except:
        return INIT_FILE_METADATA

def write_client_file_metadata(filename, data):
    """
    Writes metadata for a given file to the client.
    
    Parameters:
        filename (str): The name of the file.
        data (dict): The metadata to write.
    
    Returns:
        int: -1 if there was an error, otherwise returns 0.
    """
    metadata_file_path = f"metadata/{filename}"
    try:
        with open(get_client_file_path(metadata_file_path), "w") as f:
            data = json.dump(data, f)
    except:
        return -1

def append(my_id, local_file, file_name):
    server_id = get_receiver_id_from_file(my_id, file_name)
    res = request_append_file(server_id , file_name, local_file, "N")
    return res

def merge(file_name):
    receiver_id = get_receiver_id_from_file(0, file_name)    
    res = request_merge_file(receiver_id, file_name)
    return res

def id_from_ip(ip):
    """
    Extracts the machine ID from an IP address.
    
    Parameters:
        ip (str): The IP address of the machine.
    
    Returns:
        int: The extracted machine ID.
    """
    return int(ip[13:15])
