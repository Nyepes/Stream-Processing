import socket
import sys
from time import sleep

from src.shared.constants import HOSTS, FILE_SYSTEM_PORT, RECEIVE_TIMEOUT
from src.shared.shared import get_machines
from src.mp3.shared import generate_sha1, get_receiver_id_from_file, id_from_ip, request_append_file, request_create_file
from src.mp3.constants import REPLICATION_FACTOR

BUFFER_SIZE = 1024
    

if __name__ == "__main__":

    file_name = sys.argv[1]
    server_file_name = sys.argv[2]

    server_id = get_receiver_id_from_file(0, server_file_name)
    my_id = id_from_ip(socket.gethostname())
    machines = get_machines() + [my_id] # TODO: Fix others
    machines.sort()

    for i in range(len(machines)):
        if (machines[i] >= server_id):
            break
    res = 0
    for j in range(min(REPLICATION_FACTOR, len(machines))):
        res += request_create_file(machines[(i + j) % len(machines)] , server_file_name)
    if (res != 0):
        print("File already Created")
    receiver_id = get_receiver_id_from_file(my_id, server_file_name)
    print(receiver_id)
    res = request_append_file(receiver_id, server_file_name, file_name)
    if (res < 0):
        print("Failed Creating File")
    else:
        print("Created File")




