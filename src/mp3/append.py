import socket
import sys

from src.shared.constants import HOSTS, FILE_SYSTEM_PORT, RECEIVE_TIMEOUT
from src.shared.shared import get_machines, get_my_id
from src.mp3.shared import generate_sha1, send_file, id_from_ip, get_receiver_id_from_file
from src.mp3.constants import REPLICATION_FACTOR

BUFFER_SIZE = 1024

def request_append_file(receiver_id, server_file_name, local_file_name):
    try:    
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
            server.settimeout(RECEIVE_TIMEOUT)
            server.connect((HOSTS[receiver_id - 1], FILE_SYSTEM_PORT))
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

    

if __name__ == "__main__":
    

    my_id = id_from_ip(socket.gethostname())
    local_file = sys.argv[1]
    file_name = sys.argv[2]

    server_id = get_receiver_id_from_file(my_id, file_name)
    print(server_id)
    res = request_append_file(server_id , file_name, local_file)
    if (res < 0):
         print("Append Failed")
    else:
        print("Appended")


