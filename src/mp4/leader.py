import sys
import socket
import threading
import os
import json

from src.shared.constants import RECEIVE_TIMEOUT, HOSTS, MAX_CLIENTS, LEADER_PORT, RAINSTORM_PORT
from src.mp4.constants import READ, EXECUTE, RUN
from src.shared.shared import get_machines
from src.mp3.shared import get_receiver_id_from_file, request_create_file, get_replica_ids, generate_sha1

machine_id = int(sys.argv[1])

def send_request(type, request_data, to):
    with open(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as server_sock:
        server_sock.settimeout(RECEIVE_TIMEOUT)
        server_sock.connect(HOSTS[to - 1], RAINSTORM_PORT)
        server_sock.sendall(type.encode('utf-8'))
        server_sock.sendall(json.dumps(request_data).encode('utf-8'))

def request_read(job_id, file, file_owners, next_stage, num_tasks):
    for i in range(num_tasks):
        request = {
            "FILE": file,
            "NUM_TASKS": num_tasks,
            "KEY": i,
            "VM": next_stage,
            "JOB_ID": job_id
        }
        send_request(READ, request, file_owners[i])

def request_intermediate_stage(job_id, vms, binary_path):
    for vm in vms:
        request = {
            "PATH": binary_path,
            "VM": vms,
            "JOB_ID": job_id,
        }
        send_request(EXECUTE, request, vm)


def start_leader(argv):
    job_id = 0 # TODO: Change
    op_1_path = argv[1]
    op_2_path = argv[2]
    hydfs_dir = argv[3]
    output_dir = argv[4]
    num_tasks = int(argv[5])
    # Maybe create file??? before starting

    file_owners = get_replica_ids(generate_sha1(hydfs_dir))
    
    vms = [1, 2, 3] # TODO: Actual VMS
    request_intermediate_stage(job_id, vms, op_1_path)
    request_read(job_id, file_owners, op_1_path, op_2_path, output_dir, num_tasks)
    

    

if __name__ == "__main__":
    start_leader(sys.argv)