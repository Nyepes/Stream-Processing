import sys
import os
import socket

from src.shared.constants import HOSTS
from src.mp3.shared import generate_sha1, request_file, id_from_ip, get_receiver_id_from_file, get_client_file_metadata, get_client_file_path, write_client_file_metadata
from src.mp3.constants import INIT_FILE_METADATA
"""
Script to run when calling ./run.sh list_mem
"""



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
    print("f")
    print(file_version)
    res = request_file(server_id, file_name, get_client_file_path(output_file), version = file_version)
    print(res)
    version = INIT_FILE_METADATA
    version["version"] = res
    write_client_file_metadata(output_file,version)
    
    if (res >= 0):
        print ("File received")
    else:
        print("Failed Operation")



