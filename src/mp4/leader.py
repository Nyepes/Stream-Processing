import sys
import socket
import threading
import os
import json
from time import sleep
import queue
from queue import Queue

from src.shared.ThreadSock import ThreadSock
from src.shared.constants import RECEIVE_TIMEOUT, HOSTS, MAX_CLIENTS, LEADER_PORT, RAINSTORM_PORT
from src.shared.DataStructures.Dict import Dict
from src.mp4.constants import READ, EXECUTE, RUN, UPDATE
from src.shared.shared import get_machines
from src.mp3.shared import get_receiver_id_from_file, request_create_file, get_replica_ids, generate_sha1, append, merge
from src.mp4.worker import get_hydfs_log_name, from_bytes, decode_key_val


machine_id = int(sys.argv[1])
job_info = None # what we send in the request
cur_jobs = None
member_jobs = None
max_task_id = 3
member_list = []

def send_request(type, request_data, to):
    server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_sock.settimeout(RECEIVE_TIMEOUT + 5)
    server_sock.connect((HOSTS[to - 1], RAINSTORM_PORT))
    server_sock.sendall(type.encode('utf-8'))
    server_sock.sendall(json.dumps(request_data).encode('utf-8'))
    server_sock.recv(1)
    if (type == EXECUTE and "OUTPUT" in request_data):
        thread = threading.Thread(target=run_job, args=(request_data["JOB_ID"] - 2, server_sock, ))
        thread.daemon = True
        thread.start()
    else:
        server_sock.close()
def request_read(job_id, file, readers, workers, num_tasks):
    for i in range(num_tasks):
        vms = [0] * num_tasks
        vms[i] = workers[i]
        request = {
            "FILE": file,
            "NUM_TASKS": num_tasks,
            "KEY": i,
            "VM": vms,
            "JOB_ID": job_id
        }
        send_request(READ, request, readers[i])

def request_intermediate_stage(job_id, stage_vms, next_stage, binary_path):
    for start, vm in enumerate(stage_vms):
        request = {
            "PATH": binary_path,
            "VM": next_stage,
            "JOB_ID": job_id,
            "START": start,
            "NUM_TASKS": len(next_stage)
        }
        job_info.add((vm, job_id), request) # We need job id to avoid rewriting the job info if its different stages
        send_request(EXECUTE, request, vm)

def request_final_stage(job_id, writers, output_file, binary_path, is_stateful):
    for start, vm in enumerate(writers):
        request = {
            "PATH": binary_path,
            "OUTPUT": output_file,
            "JOB_ID": job_id,
            "START": start,
            "NUM_TASKS": len(writers),
            "STATEFUL": is_stateful == "1"
        }
        job_info.add((vm, job_id), request)
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
    # return
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
    op_1_path = job_data["OP_1_PATH"]
    op_2_path = job_data["OP_2_PATH"]
    hydfs_dir = job_data["INPUT_FILE"]
    output_dir = job_data["OUTPUT_FILE"]
    num_tasks = int(job_data["NUM_TASKS"])
    is_stateful = job_data["STATEFUL"]
    job_data["QUEUE"] = Queue(maxsize=10240)

    readers = get_readers(num_tasks, hydfs_dir)

    for reader in readers:
        member_jobs.increment_list(reader, task_id)

    workers = get_workers(2 * num_tasks) # num_tasks per stage 2 stages

    # print(f"workers: {workers}")
    # print(f"readers: {readers}")

    stage_2_workers = workers[: len(workers) // 2]
    stage_3_workers = workers[len(workers) // 2:]

    job_data["READERS"] = readers
    job_data["STAGE2"] = stage_2_workers
    job_data["STAGE3"] = stage_3_workers
    j = {"r": readers, "s2": stage_2_workers, "s3": stage_3_workers}
    file = open(f"job_data-{task_id}", "w")
    file.write(json.dumps(j))
    cur_jobs.add(task_id, job_data)

    for worker in stage_2_workers:
        member_jobs.increment_list(worker, task_id + 1)
    
    for worker in stage_3_workers:
        member_jobs.increment_list(worker, task_id + 2)
    
    max_task_id = task_id + 3
    
    request_final_stage(task_id + 2, stage_3_workers, output_dir, op_2_path, is_stateful)
    request_intermediate_stage(task_id + 1, stage_2_workers, stage_3_workers, op_1_path)
    request_read(task_id, hydfs_dir, readers, stage_2_workers, num_tasks)
    return task_id

processed_streams = None

def run_job(job_id, client: socket.socket):
    queue =  cur_jobs.get(job_id)["QUEUE"]
    while (1):
        try:
            data_length = client.recv(4)
        except (ConnectionResetError):
            print("CONNECTION RESET")
            # Previous Node crashed Wait for next to connect
            return
        except(ConnectionRefusedError, socket.timeout):
            sleep(1)
            continue

        if (data_length == b''):
            break
        data_length = from_bytes(data_length)
        data = client.recv(data_length).decode('utf-8')

        received_stream = decode_key_val(data)
        line_number = received_stream["key"]
        
        stream = received_stream["value"]

        if (processed_streams.get((job_id, line_number))): # CHANGE!!!!!
            # No neeed to ack
            print("Filtered", (job_id, line_number), file=sys.stderr)
            continue
        
        processed_streams.add((job_id, line_number), True) # CHANGE!!!!!
        queue.put(stream)

def write_output(job_id):
    cur_job = cur_jobs.get(job_id)
    q =  cur_jobs.get(job_id)["QUEUE"]
    output_name = cur_jobs.get(job_id)["OUTPUT_FILE"]
    file = open("o.txt", "w")
    processed_data = 0
    with open(f"{job_id}", "w") as output:
        while(1):
            try:
                val = q.get(timeout = 0.5)
            except (queue.Empty):
                output.flush()
                append(0, output.name, output_name)
                merge(output_name)
                output.truncate(0)
                output.seek(0,0)
                sleep(1)
                continue
            dict_data = decode_key_val(val)
            line = f"{dict_data['key']}:{dict_data['value']}"
            
            print(line, file=sys.stderr)
            output.write(line + '\n')
            processed_data += 1

            if (processed_data >= 50):
                output.flush()
                append(0, output.name, output_name)
                merge(output_name)
                output.truncate(0)
                output.seek(0,0)
    # process.stdin.close()    


def retransmit_failed_job_intermediate_stage(stage, failed_node_id, new_node_id):
    """
    Args:
        stage (int): 0 - reader, 1 - intermediate, 2 - final
        failed_node_id (int): The ID of the node that failed.
        new_node_id (int): The ID of the new node that will replace the failed node.
    """
    request = job_info.get((failed_node_id, stage))

    request["PREV"] = failed_node_id
    send_request(EXECUTE, request, new_node_id)

def handle_failed(failed_nodes):
    affected = {}
    
    for node in failed_nodes:
        affected[node] = member_jobs.get(node)
        member_jobs.delete(node)
    
    for failed in failed_nodes:
        new_vm = get_workers(1)[0] # Find replacement id
        affected_jobs = affected[failed]
        # print("AFFECTED_JOBS", affected_jobs)
        members = get_machines() + [machine_id]
        for stage in affected_jobs:
            retransmit_failed_job_intermediate_stage(stage, failed, new_vm)
            member_jobs.increment_list(new_vm, stage)
            update_message = {"VM": failed, "STAGE": stage, "NEW": new_vm}
            for member in members:
                send_request(UPDATE, update_message, member)
def poll_failures():
    # print("MEMBERS", member_jobs.items())
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
        task_id = start_job(job_data)
        
        thread = threading.Thread(target=write_output, args=(task_id, ))
        thread.daemon = True
        thread.start()

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
    
    global cur_jobs, member_jobs, job_info, processed_streams
    processed_streams = Dict(bool)
    cur_jobs = Dict(dict)
    member_jobs = Dict(list)
    job_info = Dict(dict)
    
    sleep(7)
    
    global member_list
    member_list = get_machines() + [machine_id]
    for member in member_list:
        member_jobs.add(member, [])
    
    # print(member_list)
    # print(member_jobs)

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