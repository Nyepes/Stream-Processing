import socket
import sys

from src.shared.constants import HOSTS, FILE_SYSTEM_PORT, RECEIVE_TIMEOUT
from src.shared.shared import get_machines, get_my_id
from src.mp3.shared import generate_sha1, send_file

BUFFER_SIZE = 1024

def request_append_file(sender_id, receiver_id, server_file_name, local_file_name):
    try:    
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
            server.settimeout(RECEIVE_TIMEOUT)
            server.connect((HOSTS[receiver_id - 1], FILE_SYSTEM_PORT))
            length = len(server_file_name).to_bytes(1, byteorder='little')
            sender = sender_id.to_bytes(1, byteorder='little')
            
            server.sendall(b"A" + length + server_file_name.encode() + sender)
            send_file(server, local_file_name)
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
    

    my_id = get_my_id() % 10

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
    res = request_append_file(1, 1, "hola.txt", file_name)
    if (res < 0):
         print("Append Failed")
    else:
        print("Appended")


