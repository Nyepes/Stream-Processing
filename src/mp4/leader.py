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
max_task_id = 0
member_list = []

def send_request(type, request_data, to):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_sock:
        server_sock.settimeout(RECEIVE_TIMEOUT)
        server_sock.connect((HOSTS[to - 1], RAINSTORM_PORT))
        server_sock.sendall(type.encode('utf-8'))
        server_sock.sendall(json.dumps(request_data).encode('utf-8'))
        server_sock.recv(1)

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

def request_intermediate_stage(job_id, stage_vms, next_stage, binary_path):
    for vm in stage_vms:
        request = {
            "PATH": binary_path,
            "VM": next_stage,
            "JOB_ID": job_id,
        }
        send_request(EXECUTE, request, vm)

def request_final_stage(job_id, writers, output_file, binary_path):
    for vm in writers:
        request = {
            "PATH": binary_path,
            "OUTPUT": output_file,
            "JOB_ID": job_id,
        }
        send_request(EXECUTE, request, vm)

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

def update_membership():
    global member_list
    member_set = set(member_list)
    new_memberlist = set(get_machines())
    new_members = new_memberlist - member_set
    for member in new_members:
        member_jobs.add(member, [])
    member_list = new_memberlist

def start_job(job_data):
    update_membership()
    global max_task_id

    cur_max = max_task_id
    task_id = max_task_id
    # job_id = max_job_id
    # max_job_id += 1 # TODO: Works if lucky!!!
    op_1_path = job_data["OP_1_PATH"]
    op_2_path = job_data["OP_2_PATH"]
    hydfs_dir = job_data["INPUT_FILE"]
    output_dir = job_data["OUTPUT_FILE"]
    num_tasks = int(job_data["NUM_TASKS"])
    # Maybe create file??? before starting

    readers = get_readers(num_tasks, hydfs_dir)

    for reader in readers:
        member_jobs.increment_list(reader, task_id)

    workers = get_workers(2 * num_tasks) # num_tasks per stage 2 stages

    print(f"workers: {workers}")
    print(f"readers: {readers}")

    stage_2_workers = workers[: len(workers) // 2]
    stage_3_workers = workers[len(workers) // 2:]

    job_data["READERS"] = readers
    job_data["STAGE2"] = stage_2_workers
    job_data["STAGE3"] = stage_3_workers


    cur_jobs.add(task_id, job_data)

    for worker in stage_2_workers:
        member_jobs.increment_list(worker, task_id + 1)
    
    for worker in stage_2_workers:
        member_jobs.increment_list(worker, task_id + 2)
    
    max_task_id = task_id + 3
    
    request_final_stage(task_id + 2, stage_3_workers, output_dir, op_2_path)
    # print('A')
    request_intermediate_stage(task_id + 1, stage_2_workers, stage_3_workers, op_1_path)
    # print('B')
    request_read(task_id, hydfs_dir, readers, stage_2_workers, num_tasks)
    # print('C')


def handle_failed(failed):
    new_vm = get_workers(1) # Find replacement id

    affected_jobs = member_jobs.get(failed)
    members = get_machines() + [machine_id]
    for stage in affected_jobs:

        update_message = {"VM": failed, "STAGE": stage, "NEW": new_vm}
        for member in members:
            send_request(UPDATE, member, update_message)
    

def poll_failures():
    print("MEMBERS", member_jobs.items())
    global member_list
    while (1):
        sleep(1)
        new_machines = set(get_machines())
        failed = set(member_list) - new_machines
        if (len(failed) > 0):
            handle_failed(failed)
            member_list = new_machines
        
 
def handle_client(client_sock, ip):
    mode = client_sock.recv(1)
    if (mode == b"S"):
        # Start Job
        data = client_sock.recv(1024 * 1024)
        job_data = json.loads(data.decode('utf-8'))
        print(f"Starting Job: {job_data}")
        start_job(job_data)
        poll_failures()




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
            continue

        client_handler = threading.Thread(target=handle_client, args=(client_socket, ip_address,))
        client_handler.daemon = True
        client_handler.start()

if __name__ == "__main__":
    start_server(int(sys.argv[1]))