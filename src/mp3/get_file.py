import sys
import os
from src.shared.constants import HOSTS
from src.mp3.shared import generate_sha1, request_file
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
    
    file_name = sys.argv[1]
    output_file = sys.argv[2]
    machines = get_machines()

    value = int(generate_sha1(file_name), 16)

    machine_with_file = value % 10 + 1

    request_file(1, file_name, output_file)


