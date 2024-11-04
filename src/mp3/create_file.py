import socket
import sys

from src.shared.constants import HOSTS, FILE_SYSTEM_PORT, RECEIVE_TIMEOUT
from src.shared.shared import get_machines
from src.mp3.shared import generate_sha1, get_receiver_id_from_file, id_from_ip
from src.mp3.constants import REPLICATION_FACTOR

BUFFER_SIZE = 1024

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

    

if __name__ == "__main__":

    file_name = sys.argv[1]
    server_id = get_receiver_id_from_file(0, file_name)
    machines = get_machines() + [id_from_ip(socket.gethostname())] # TODO: Fix others
    machines.sort()

    for i in range(len(machines)):
        if (machines[i] >= server_id):
            break
    res = 0
    print(machines)
    for j in range(REPLICATION_FACTOR):
        print(machines[(i+j)%len(machines)])
        res += request_create_file(machines[(i + j) % len(machines)] , file_name)
    
    if (res != 0):
        print(res)
        print("Failed Creating File")
    else:
        print("File Created")


