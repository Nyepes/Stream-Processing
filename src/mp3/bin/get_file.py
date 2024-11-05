import sys
import os
import socket

from src.shared.constants import HOSTS
from src.mp3.shared import generate_sha1, request_file, id_from_ip()
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

    res = request_file(server_id, file_name, output_file)
    if (res >= 0)
        print ("File received")
    else:
        print("Failed Operation")



