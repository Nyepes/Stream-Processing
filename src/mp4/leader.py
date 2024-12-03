import sys
import socket
import threading
import os
import json
from time import sleep

from src.shared.constants import RECEIVE_TIMEOUT, HOSTS, MAX_CLIENTS, LEADER_PORT, RAINSTORM_PORT
from src.shared.DataStructures.Dict import Dict
from src.mp4.constants import READ, EXECUTE, RUN
from src.shared.shared import get_machines
from src.mp3.shared import get_receiver_id_from_file, request_create_file, get_replica_ids, generate_sha1

machine_id = int(sys.argv[1])
cur_jobs = None
member_jobs = None
max_job_id = 0

def send_request(type, request_data, to):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_sock:
        server_sock.settimeout(RECEIVE_TIMEOUT)
        server_sock.connect((HOSTS[to - 1], RAINSTORM_PORT))
        server_sock.sendall(type.encode('utf-8'))
        server_sock.sendall(json.dumps(request_data).encode('utf-8'))
        serve_sock.recv(1)

def request_read(job_id, file, readers, workers, num_tasks):
    for i in range(num_tasks):
        request = {
            "FILE": file,
            "NUM_TASKS": num_tasks,
            "KEY": i,
            "VM": workers[i],
            "JOB_ID": job_id
        }
        send_request(READ, request, readers[i])

def request_intermediate_stage(job_id, vms, binary_path):
    for vm in vms:
        request = {
            "PATH": binary_path,
            "VM": vms,
            "JOB_ID": job_id,
        }
        send_request(EXECUTE, request, vm)

def request_final_stage(job_id, output_file, binary_path):
    return

def get_readers(num_jobs, hydfs_dir):
    file_owners = get_replica_ids(generate_sha1(hydfs_dir))
    return file_owners[:num_jobs]

def get_workers(num_tasks):
    num_jobs = []
    global member_jobs
    jobs = member_jobs.items()
    for job in jobs:
        num_jobs.append((len(job[1]), job[0])) #Number of jobs and job number
    
    num_jobs.sort(reverse=False)
    ans = []
    for i in range(num_tasks):
        ans.append(num_jobs[i % len(num_jobs)][1]) # The node id with lowest amount of jobs
    return ans

def start_job(job_data):
    global max_job_id

    job_id = max_job_id
    max_job_id += 1 # TODO: Works if lucky!!!
    op_1_path = job_data["OP_1_PATH"]
    op_2_path = job_data["OP_2_PATH"]
    hydfs_dir = job_data["INPUT_FILE"]
    output_dir = job_data["OUTPUT_FILE"]
    num_tasks = int(job_data["NUM_TASKS"])
    # Maybe create file??? before starting

    readers = get_readers(num_tasks, hydfs_dir)

    for reader in readers:
        member_jobs.increment_list(reader, job_id)

    workers = get_workers(2 * num_tasks) # num_tasks per stage 2 stages

    print(f"workers: {workers}")
    print(f"readers: {readers}")


    job_data["READERS"] = readers
    job_data["WORKERS"] = workers

    cur_jobs.add(job_id, job_data)

    for worker in workers:
        member_jobs.increment_list(worker, job_id)



    request_intermediate_stage(job_id, workers, op_1_path)
    request_read(job_id, hydfs_dir, readers, workers[: len(workers) // 2], num_tasks)
    
def handle_client(client_sock, ip):
    mode = client_sock.recv(1)
    if (mode == b"S"):
        # Start Job
        data = client_sock.recv(1024 * 1024)
        job_data = json.loads(data.decode('utf-8'))
        print(f"Starting Job: {job_data}")
        start_job(job_data)

def start_server(machine_id):
    """
    Creates a server that listens on a specified port and handles client connections.
    It constantly waits for new connections and creates a new thread to handle each client connection.

    Parameters:
        machine_id (str): The ID of the machine.
    """
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    
    # Lets server reuse address so that it can relaunch quickly
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.settimeout(RECEIVE_TIMEOUT)

    server.bind((HOSTS[machine_id - 1], LEADER_PORT))
    server.listen(MAX_CLIENTS)
    
    global cur_jobs, member_jobs
    cur_jobs = Dict(dict)
    member_jobs = Dict(list)
    sleep(7)
    
    global member_list
    member_list = get_machines() + [machine_id]
    for member in member_list:
        member_jobs.add(member, [])
    print(member_list)
    print(member_jobs)
    while True:
        try:
            client_socket, ip_address = server.accept()
        except (ConnectionRefusedError, socket.timeout):
            # TODO: See if someone next or prev
            continue

        # Creates a new thread for each client
        client_handler = threading.Thread(target=handle_client, args=(client_socket, ip_address,))
        
        # sets daemon to true so that there is no need of joining threads once thread finishes
        client_handler.daemon = True
        client_handler.start()

if __name__ == "__main__":
    start_server(int(sys.argv[1]))