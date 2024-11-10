import socket
import sys

from src.shared.constants import HOSTS, FILE_SYSTEM_PORT, RECEIVE_TIMEOUT
from src.shared.shared import get_machines, get_my_id
from src.mp3.shared import generate_sha1, send_file, get_receiver_id_from_file, request_merge_file

BUFFER_SIZE = 1024

if __name__ == "__main__":
    
    file_name = sys.argv[1]
    machines = get_machines()

    value = generate_sha1(file_name)
    receiver_id = get_receiver_id_from_file(0, file_name)
    
    print(f"Head replica: {receiver_id}")
    
    res = request_merge_file(receiver_id, file_name)
    
    if (res < 0):
         print("Merge Failed")
    else:
        print("Merges :>")