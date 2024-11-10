import socket
import sys

from src.shared.constants import HOSTS, FILE_SYSTEM_PORT, RECEIVE_TIMEOUT
from src.shared.shared import get_machines, get_my_id
from src.mp3.shared import generate_sha1, send_file, id_from_ip, get_receiver_id_from_file, request_append_file
from src.mp3.constants import REPLICATION_FACTOR

def request_multiappend(receiver_id, server_file_name, local_file_name):
    try:    
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server: 
            # Init connection
            server.settimeout(RECEIVE_TIMEOUT)
            server.connect((HOSTS[receiver_id - 1], FILE_SYSTEM_PORT))
            
            # Send payload (A + length of server_file_name + server_file_name + status)
            length = len(server_file_name).to_bytes(1, byteorder='little')
            local_file_length = len(local_file_name).to_bytes(1, byteorder="little")
            server.sendall(b"Q" + length + server_file_name.encode() + local_file_length + local_file_name.encode())
            
    except (ConnectionRefusedError, socket.timeout):
        return -1
    except (OSError):
        return -2
    
    return 0


if __name__ == "__main__":
    print (sys.argv)
    if (len(sys.argv) % 2 != 0):
        print("incorrect argumnets")

    server_file = sys.argv[1]
    num_files = (len(sys.argv) - 2) // 2

    vm_id = sys.argv[2:num_files + 2]
    local_files = sys.argv[num_files + 2:]

    print(vm_id)
    print(local_files)
    for i in range(len(local_files)):
        request_multiappend(int(vm_id[i]), server_file, local_files[i])


