import socket
import sys

from src.shared.constants import HOSTS, FILE_SYSTEM_PORT, RECEIVE_TIMEOUT
from src.shared.shared import get_machines, get_my_id
from src.mp3.shared import generate_sha1, send_file, get_receiver_id_from_file

BUFFER_SIZE = 1024

def request_merge_file(receiver_id, server_file_name):
    try:    
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
            server.settimeout(RECEIVE_TIMEOUT)
            server.connect((HOSTS[receiver_id - 1], FILE_SYSTEM_PORT))
            length = len(server_file_name).to_bytes(1, byteorder='little')
           
            server.sendall(b"P" + length + server_file_name.encode())
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
    
    file_name = sys.argv[1]
    machines = get_machines()

    value = generate_sha1(file_name)
    receiver_id = get_receiver_id_from_file(0, file_name)
    print(f"r {receiver_id}")
    res = request_merge_file(receiver_id, file_name)
    res = 0
    if (res < 0):
         print("Merge Failed")
    else:
        print("Merges :>")