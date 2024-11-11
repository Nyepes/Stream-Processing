import sys
import os
import socket
import random

from src.shared.constants import HOSTS
from src.mp3.shared import generate_sha1, request_file, id_from_ip, get_receiver_id_from_file, get_client_file_metadata, get_client_file_path, write_client_file_metadata
from src.mp3.constants import INIT_FILE_METADATA, CACHE_PATH, MAX_CACHE_FILES
"""
Script to run when calling ./run.sh list_mem
"""

import os
import glob

EXCLUDE_DIR = os.path.join(CACHE_PATH, 'metadata')

def remove_oldest_file():
    files = glob.glob(os.path.join(CACHE_PATH, '*'))
    files = [f for f in files if not f.startswith(EXCLUDE_DIR)]
    random_file = random.choice(files)
    os.remove(random_file)
    os.remove(get_client_file_path(f"metadata/{os.path.basename(random_file)}"))

def get_machines():
    machines = []
    with open("src/member_list.txt", "r") as member_list_file:
        for line in member_list_file:
            machine_id = int(line.strip())
            machines.append(machine_id)
    return machines

if __name__ == "__main__":
    
    my_id = id_from_ip(socket.gethostname())
    file_name = sys.argv[1]
    output_file = sys.argv[2]
    
    server_id = get_receiver_id_from_file(my_id, file_name)

    file_version = get_client_file_metadata(file_name)["version"]
    # print(f"getting from: {server_id}")
    res = request_file(server_id, file_name, get_client_file_path(output_file), version = file_version)
    
    version = INIT_FILE_METADATA
    version["version"] = res
    write_client_file_metadata(output_file,version)
    num_files = len(os.listdir(CACHE_PATH))
    if (num_files > MAX_CACHE_FILES):
        remove_oldest_file()
    if (res >= 0):
        print ("File received")
    else:
        print("Failed Operation")



