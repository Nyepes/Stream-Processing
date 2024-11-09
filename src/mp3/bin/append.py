import socket
import sys

from src.shared.constants import HOSTS, FILE_SYSTEM_PORT, RECEIVE_TIMEOUT
from src.shared.shared import get_machines, get_my_id
from src.mp3.shared import generate_sha1, send_file, id_from_ip, get_receiver_id_from_file, request_append_file
from src.mp3.constants import REPLICATION_FACTOR

BUFFER_SIZE = 1024


if __name__ == "__main__":
    

    my_id = id_from_ip(socket.gethostname())
    local_file = sys.argv[1]
    file_name = sys.argv[2]

    server_id = get_receiver_id_from_file(my_id, file_name)
    res = request_append_file(server_id , file_name, local_file, "N")
    if (res < 0):
         print("Append Failed")
    else:
        print("Append Completed")

