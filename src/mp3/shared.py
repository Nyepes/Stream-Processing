import hashlib 
import socket

from src.shared.constants import HOSTS, FILE_SYSTEM_PORT, RECEIVE_TIMEOUT
from src.shared.shared import get_machines

def generate_sha1(input_string):
    """Generate a SHA-1 hash of the input string."""
    sha1_hash = hashlib.sha1()
    sha1_hash.update(input_string.encode('utf-8'))
    return int(sha1_hash.hexdigest(), 16)


def read_file_to_socket(file_name, sock = None):
    try:
        with open(f"src/mp3/fs/{file_name}", 'r') as file:
            for line in file:
                if sock is None:
                    print(line.strip())
                else: 
                    sock.sendall(line.encode())
    except:
        sock.sendall("Error".encode())


BUFFER_SIZE = 1024
def request_file(machine_id, file_name, output_file):
    try:    
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
            server.settimeout(RECEIVE_TIMEOUT)
            server.connect((HOSTS[machine_id - 1], FILE_SYSTEM_PORT))
            length = len(file_name).to_bytes(1, byteorder='little')
            server.sendall(b"G" + length + file_name.encode())
            with open(output_file, "wb") as output:
                while (1):
                    data = server.recv(BUFFER_SIZE)
                    if (data == b''): return
                    output.write(data)    
    except (ConnectionRefusedError, socket.timeout):
        return -1
    except (OSError):
        return -2



def send_file(receiver_socket, file_name):
    try:
        with open(file_name, 'rb') as file:
            while True:
                chunk = file.read(BUFFER_SIZE)        
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


def get_receiver_id_from_file(my_id, file_name):

    machines = get_machines()
    value = generate_sha1(file_name)
    file_id = value % 10 + 1
    server_id = -1

    if (my_id < file_id + REPLICATION_FACTOR and my_id >= file_id and my_id in machines):
        server_id = my_id
    else:
        machines.sort()
        for machine_id in machines:
            if (machine_id >= file_id):
                server_id = machine_id
        server_id = (server_id + my_id) % 10 + 1
    return server_id

def id_from_ip(ip):
    return int(ip[13:15])



