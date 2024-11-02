import socket
import sys

from src.shared.constants import HOSTS, FILE_SYSTEM_PORT, RECEIVE_TIMEOUT
from src.shared.shared import get_machines
from src.mp3.shared import generate_sha1

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
    return 1

    

if __name__ == "__main__":
    my_id = int(socket.gethostname()[13:15])
    file_name = sys.argv[1]
    machines = get_machines()
    value = generate_sha1(file_name)
    machine_with_file = 1 # value % 10 + 1


    # if (id < machine_with_file + REPLICATION_FACTOR and id >= machine_with_file):
    #     res = create_local_file()
    #     if (res < 0):
    #         print("Failed Creating File")
    #     else:
    #         print("File Created")
    # else:
    # TODO: 1 -> machine_with_file + id % REPLICATION_FACTOR
    res = request_create_file(1 , file_name)
    if (res < 0):
         print("Failed Creating File")
    else:
        print("File Created")


