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
from src.Streaming.constants import READ, EXECUTE, RUN, UPDATE
from src.shared.shared import get_machines
from src.FileSystem.shared import get_receiver_id_from_file, request_create_file, get_replica_ids, generate_sha1, append, merge
from src.Streaming.worker import get_hydfs_log_name, from_bytes, decode_key_val


machine_id = int(sys.argv[1])
job_info = None # Dictionary mapping (vm_id, job_id) to job configuration
cur_jobs = None # Dictionary mapping job_id to job data including queues
member_jobs = None # Dictionary mapping member IDs to list of assigned job IDs
max_task_id = 3 # Counter for assigning unique task IDs
member_list = [] # List of active member machine IDs

def send_request(type, request_data, to):
    """
    Send a request to another node in the cluster.
    
    Args:
        type: Type of request (READ, EXECUTE, etc)
        request_data: Dictionary containing request parameters
        to: ID of destination machine
    """
    
    try:
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
    except:
        print("FAILED SENDING TO", to, file=sys.stderr)

def request_read(job_id, file, readers, workers, num_tasks):
    """
    Send read requests to nodes to read input file chunks.
    
    Args:
        job_id: ID of the job
        file: Input file path
        readers: List of reader node IDs
        workers: List of worker node IDs
        num_tasks: Number of tasks to split input into
    """
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
    """
    Send requests to set up intermediate processing stage.
    
    Args:
        job_id: ID of the job
        stage_vms: List of VMs for current stage
        next_stage: List of VMs for next stage
        binary_path: Path to executable for this stage
    """
    for start, vm in enumerate(stage_vms):
        request = {
            "PATH": binary_path,
            "VM": next_stage,
            "JOB_ID": job_id,
            "START": start,
            "NUM_TASKS": len(next_stage)
        }
        job_info.add((vm, job_id), request) # Store job info for failure recovery
        send_request(EXECUTE, request, vm)

def request_final_stage(job_id, writers, output_file, binary_path, is_stateful):
    """
    Send requests to set up final processing stage.
    
    Args:
        job_id: ID of the job
        writers: List of writer node IDs
        output_file: Output file path
        binary_path: Path to executable
        is_stateful: Whether processing maintains state
    """
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
    """Get list of reader nodes based on file location"""
    file_owners = get_replica_ids(generate_sha1(hydfs_dir))
    return file_owners[:num_jobs]

def get_workers(num_tasks):
    """
    Select worker nodes with least current load.
    
    Args:
        num_tasks: Number of workers needed
    Returns:
        List of selected worker node IDs
    """
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
    """Update membership list and initialize job tracking for new members"""
    global member_list
    member_set = set(member_list)
    new_memberlist = set(get_machines())
    new_members = new_memberlist - member_set
    for member in new_members:
        member_jobs.add(member, [])
    member_list = new_memberlist

def start_job(job_data):
    """
    Initialize and start a new job.
    
    Args:
        job_data: Dictionary containing job configuration
    Returns:
        task_id: ID assigned to the job
    """
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

    # Select nodes for each stage
    readers = get_readers(num_tasks, hydfs_dir)
    for reader in readers:
        member_jobs.increment_list(reader, task_id)

    workers = get_workers(2 * num_tasks) # num_tasks per stage 2 stages
    stage_2_workers = workers[: len(workers) // 2]
    stage_3_workers = workers[len(workers) // 2:]

    # Store job configuration
    job_data["READERS"] = readers
    job_data["STAGE2"] = stage_2_workers
    job_data["STAGE3"] = stage_3_workers
    j = {"r": readers, "s2": stage_2_workers, "s3": stage_3_workers}
    file = open(f"job_data-{task_id}", "w")
    file.write(json.dumps(j))
    cur_jobs.add(task_id, job_data)

    # Track job assignments
    for worker in stage_2_workers:
        member_jobs.increment_list(worker, task_id + 1)
    
    for worker in stage_3_workers:
        member_jobs.increment_list(worker, task_id + 2)
    
    max_task_id = task_id + 3
    
    # Start job stages
    request_final_stage(task_id + 2, stage_3_workers, output_dir, op_2_path, is_stateful)
    request_intermediate_stage(task_id + 1, stage_2_workers, stage_3_workers, op_1_path)
    request_read(task_id, hydfs_dir, readers, stage_2_workers, num_tasks)
    return task_id

processed_streams = None # Track processed data streams

def run_job(job_id, client: socket.socket):
    """
    Process data streams for a job.
    
    Args:
        job_id: ID of the job
        client: Socket connection to client
    """
    queue =  cur_jobs.get(job_id)["QUEUE"]
    while (1):
        try:
            data_length = client.recv(4)
        except (ConnectionResetError):
            print("CONNECTION RESET")
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

        # Filter duplicate streams
        if (processed_streams.get((job_id, line_number)) == True):
            print("Filtered", (job_id, line_number), file=sys.stderr)
            continue
        else:
            processed_streams.add((job_id, line_number), True)
            queue.put(stream)

def write_output(job_id):
    """
    Write job output to file.
    
    Args:
        job_id: ID of the job
    """
    cur_job = cur_jobs.get(job_id)
    q =  cur_jobs.get(job_id)["QUEUE"]
    output_name = cur_jobs.get(job_id)["OUTPUT_FILE"]
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

            # Periodically flush output
            if (processed_data >= 50):
                output.flush()
                append(0, output.name, output_name)
                merge(output_name)
                output.truncate(0)
                output.seek(0,0)

def retransmit_failed_job_intermediate_stage(stage, failed_node_id, new_node_id):
    """
    Retransmit job to new node after failure.
    
    Args:
        stage: Stage number (0=reader, 1=intermediate, 2=final)
        failed_node_id: ID of failed node
        new_node_id: ID of replacement node
    """
    print(f"{failed_node_id} at stage {stage} Was Replaced by {new_node_id}", file=sys.stderr)

    request = job_info.get((failed_node_id, stage))
    request["PREV"] = failed_node_id
    send_request(EXECUTE, request, new_node_id)

def handle_failed(failed_nodes):
    """
    Handle failed nodes by reassigning their jobs.
    
    Args:
        failed_nodes: List of failed node IDs
    """
    affected = {}
    
    # Track affected jobs
    for node in failed_nodes:
        affected[node] = member_jobs.get(node)
        member_jobs.delete(node)
    
    # Reassign jobs to new nodes
    for failed in failed_nodes:
        new_vm = get_workers(1)[0] # Find replacement id
        affected_jobs = affected[failed]
        members = get_machines() + [machine_id]
        for stage in affected_jobs:
            retransmit_failed_job_intermediate_stage(stage, failed, new_vm)
            member_jobs.increment_list(new_vm, stage)
            update_message = {"VM": failed, "STAGE": stage, "NEW": new_vm}
            print(members, file=sys.stderr)
            for member in members:
                send_request(UPDATE, update_message, member)

def poll_failures():
    """Continuously monitor for node failures"""
    global member_list
    while (1):
        sleep(1)
        new_machines = set(get_machines())
        failed = set(member_list) - new_machines
        if (len(failed) > 0):
            handle_failed(failed)
            member_list = new_machines
        
def handle_client(client_sock, ip):
    """
    Handle client connection.
    
    Args:
        client_sock: Socket connection to client
        ip: Client IP address
    """
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
    Start leader server to coordinate distributed processing.
    
    Args:
        machine_id: ID of this machine
    """
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    
    # Lets server reuse address so that it can relaunch quickly
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.settimeout(RECEIVE_TIMEOUT)

    server.bind((HOSTS[machine_id - 1], LEADER_PORT))
    server.listen(MAX_CLIENTS)
    
    # Initialize global state
    global cur_jobs, member_jobs, job_info, processed_streams
    processed_streams = Dict(bool)
    cur_jobs = Dict(dict)
    member_jobs = Dict(list)
    job_info = Dict(dict)
    
    sleep(7)
    
    # Initialize membership
    global member_list
    member_list = get_machines() + [machine_id]
    for member in member_list:
        member_jobs.add(member, [])

    # Main server loop
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