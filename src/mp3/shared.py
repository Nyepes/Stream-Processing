import hashlib 
import socket
import json

from src.shared.constants import HOSTS, FILE_SYSTEM_PORT, RECEIVE_TIMEOUT
from src.shared.shared import get_machines
from src.mp3.constants import REPLICATION_FACTOR, INIT_FILE_METADATA

BUFFER_SIZE = 1024

def generate_sha1(input_string):
    """Generate a SHA-1 hash of the input string."""
    sha1_hash = hashlib.sha1()
    sha1_hash.update(input_string.encode('utf-8'))
    return int(sha1_hash.hexdigest(), 16)

def read_file_to_socket(file_name, sock = None):
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
    try:    
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
            server.settimeout(RECEIVE_TIMEOUT)
            server.connect((HOSTS[machine_id - 1], FILE_SYSTEM_PORT))
            length = len(file_name).to_bytes(1, byteorder='little')
            server.sendall(b"G" + length + file_name.encode())
            server.sendall(version.to_bytes(4, byteorder = "little"))
            received_version = int.from_bytes(server.recv(4), byteorder="little")
            print(version, received_version)
            if (version is not None and received_version == version): return received_version
            with open(output_file, "wb") as output:
                while (1):
                    data = server.recv(BUFFER_SIZE)
                    print(data)
                    if (data == b'' or data == "OK"): return received_version #TODO: Maybe Change so that a file that has OK does not fail
                    output.write(data)  
            return received_version  
    except (ConnectionRefusedError, socket.timeout):
        return -1
    except (OSError):
        return -2

def request_create_file(machine_to_request, file_name):
    try:    
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
            server.settimeout(RECEIVE_TIMEOUT)
            server.connect((HOSTS[machine_to_request - 1], FILE_SYSTEM_PORT))
            length = len(file_name).to_bytes(1, byteorder='little')
            server.sendall(b"C" + length + file_name.encode())
            data = server.recv(BUFFER_SIZE).decode("utf-8")
            if (data == b''): return
            if (data == "ERROR"):
                return -1   
    except (ConnectionRefusedError, socket.timeout):
        return -1
    except (OSError):
        return -2
    return 0

def send_file(receiver_socket, file_name, file_version=None):
    try:
        print("sending")
        if (file_version is not None):
            receiver_socket.sendall(file_version.to_bytes(4, byteorder="little"))
        with open(file_name, 'rb') as file:
            while True:
                chunk = file.read(BUFFER_SIZE)
                print(chunk)        
                if not chunk:
                    break 
                receiver_socket.sendall(chunk)
            print(f"File {file_name} sent successfully.")
    except FileNotFoundError as e:
        print(f"File {file_name} not found.")
    except (ConnectionRefusedError, socket.timeout) as e:
        print(f"Error: {e}")
    except OSError as e:
        print(f"OS error: {e}")

def request_append_file(receiver_id, server_file_name, local_file_name):
    try:    
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
            server.settimeout(RECEIVE_TIMEOUT)
            server.connect((HOSTS[receiver_id - 1], FILE_SYSTEM_PORT))
            print(HOSTS[receiver_id - 1])
            length = len(server_file_name).to_bytes(1, byteorder='little')
            server.sendall(b"A" + length + server_file_name.encode())
            send_file(server, local_file_name)
            server.shutdown(socket.SHUT_WR)
            data = server.recv(BUFFER_SIZE).decode("utf-8")
            if (data == b''): return
            if (data == "ERROR"):
                return -1
    except (ConnectionRefusedError, socket.timeout):
        return -1
    except (OSError):
        return -2
    return 1

def get_receiver_id_from_file(my_id, file_name):

    machines = get_machines() + [id_from_ip(socket.gethostname())]
    machines.sort()

    value = generate_sha1(file_name)
    file_id = value % 10 + 1
    print(file_id)
    if (my_id < file_id + REPLICATION_FACTOR and my_id >= file_id and my_id in machines):
        print("a")
        return my_id
    else:
        print(machines)
        for i, machine_id in enumerate(machines):
            if (machine_id >= file_id):
                return machines[(i + my_id % REPLICATION_FACTOR) % len(machines)]
    return min(machines)

def get_file_head(file_id):
    machines = get_machines() + [id_from_ip(socket.gethostname())]
    for i, machine_id in enumerate(machines):
        if (machine_id >= file_id):
            return machines[i % len(machines)]
    return min(machines)
    
def get_server_file_path(filename):
    return f"src/mp3/fs/{filename}"

def get_client_file_path(filename):
    return f"src/mp3/local_cache/{filename}"

def get_server_file_metadata(filename):
    try:
        metadata_file_path = f"metadata/{filename}"
        with open(get_server_file_path(metadata_file_path), "r") as f:
            data = json.load(f)
        return data
    except:
        return INIT_FILE_METADATA

def write_server_file_metadata(filename, data):
    metadata_file_path = f"metadata/{filename}"
    try:
        with open(get_server_file_path(metadata_file_path), "w") as f:
            data = json.dump(data, f)
    except:
        return -1

def get_client_file_metadata(filename):
    try:
        metadata_file_path = f"metadata/{filename}"
        with open(get_client_file_path(metadata_file_path), "r") as f:
            data = json.load(f)
        return data
    except:
        print("error")
        return INIT_FILE_METADATA

def write_client_file_metadata(filename, data):
    metadata_file_path = f"metadata/{filename}"
    try:
        with open(get_client_file_path(metadata_file_path), "w") as f:
            data = json.dump(data, f)
    except:
        return -1















def id_from_ip(ip):
    return int(ip[13:15])



