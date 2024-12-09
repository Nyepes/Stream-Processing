import socket
from time import sleep, time
import threading
import json
import subprocess
import sys
import random
import copy
import select
import os
from queue import Queue
from collections import defaultdict
from src.shared.DataStructures.mem_table import MemTable
from src.shared.constants import RECEIVE_TIMEOUT, HOSTS, RAINSTORM_PORT, MAX_CLIENTS
from src.mp4.constants import READ, EXECUTE, RUN, UPDATE
from src.mp3.shared import get_machines, generate_sha1, append, get_server_file_path, merge, id_from_ip, create, get_receiver_id_from_file, get_client_file_metadata, get_client_file_path, request_file
from src.shared.DataStructures.Dict import Dict
from src.shared.ThreadSock import ThreadSock
import fcntl

SYNC_PROBABILITY = 1/1000

## Plan

# Start Server
# If Read then read files and find next available node to send

member_list = None # All jobs
current_jobs = None # Job Id to configuration of the job (executable, next stage vms, keys, etc...)
machine_id = None # Id of the machine
processed_streams = None # Dict (<sender, line_num>)

open_sockets = None # Dict(<stage_id, vm_id, "R"/"S">) # Connection to which task and vm

state = None

def send_int(sock, int_val: int):
    sock.sendall(int_val.to_bytes(4, byteorder="little"))
def receive_int(sock):
    return int.from_bytes(sock.recv(4), byteorder="little")

def to_bytes(val, length = 4):
    return val.to_bytes(length, byteorder="little")
def from_bytes(val):
    return int.from_bytes(val, byteorder="little")

def get_hydfs_log_name(job):
    return f'{job["JOB_ID"]}-{machine_id}.log'

def encode_key_val(key:str, val:str, in_bytes = True):
    json_str = json.dumps({
        "key": key, 
        "value": val
    })
    return json_str

def decode_key_val(line):
    return json.loads(line)

def get_process_output(process, poller, timeout_sec = 5):
    if (poller is None):
        process.stdout.flush()
        line = process.stdout.readline() # <input: [(key,val), (key,val),...]
        return line
    
    events = poller.poll(timeout_sec * 1000)
    if not events:
        return ""
        
    # Check if there's data to read
    line = process.stdout.readline()
    if not line:
        # Process has closed stdout
        return ""
        
    return line

def randomized_sync_log(local_log, hydfs_log, processed: Queue, last_merge):
    cur_time = time()
    if (processed.qsize() >= 30 or last_merge + 0.1 < cur_time):
        local_log.flush()
        log_name = local_log.name
        append(machine_id, log_name, hydfs_log)
        merge(hydfs_log)
        qsize = processed.qsize()
        for i in range(qsize):
            val = processed.get()
            # print("QUEUE VALUE:", val, file=sys.stderr)
            sender_sock = open_sockets.get(val[0], copy=False) #Should already have what we need
            try:
                send_int(sender_sock, int(val[1]))
            except (ConnectionResetError, BrokenPipeError):
                processed.put(val[0], val[1]) # If error leave on queue and wait for retry or who to ack changes
        local_log.truncate(0)
        local_log.seek(0, 0)
        last_merge = time()

    return last_merge
      
def listen_acks(queue, socks, sock_idx, queue_lock):
    sock = socks[sock_idx]
    while(1):
        if (queue.empty()): 
            continue
        try:
            sleep(5)
            ack_num = sock.recv(4)
        except (ConnectionResetError):
            sleep(2)
            sock = socks[sock_idx]
            continue
        except (ConnectionRefusedError, socket.timeout):
            queue_len = queue.qsize()

            if (queue_len > 0):
                with queue_lock:
                    queue_copy = Queue(maxsize=10240)
                    for i in range(queue_len):
                        val = queue.get()
                        queue_copy.put(val)
                        queue.put(val)
                try:
                    resend_queue(queue_copy, sock)
                except:
                    continue
            continue

        if (ack_num == b""):
            break

        ack = from_bytes(ack_num)

        linenumber, value = queue.queue[0]
        if (linenumber == ack):
            queue.get()
        
def resend_queue(queue, sock): #(line_number, key_val)
    while(not queue.empty()):
        line_number, key_val = queue.get()
        json_encoded = encode_key_val(line_number, key_val).encode('utf-8')
        # send_data(sock_id, json_encoded)
        send_int(sock, len(json_encoded))
        sock.sendall(json_encoded)

def make_non_blocking(fd):
    """
    Set a file descriptor (fd) to non-blocking mode.
    """
    flags = fcntl.fcntl(fd, fcntl.F_GETFL)  # Get current flags
    fcntl.fcntl(fd, fcntl.F_SETFL, flags | os.O_NONBLOCK)  # Add non-blocking flag

def pipe_vms(job):
    # return
    process = job["PROCESS"] # subprocess popen
    vms = job["VM"] # next stage vms
    poller = None
    stage_id = job["JOB_ID"] # job_id

    log_name = get_hydfs_log_name(job)
    local_processed_log = open(log_name, "wb") # Log file
    create(log_name, log_name)
    
    socks = []
    queues = []
    threads = []
    queue_locks = []
    # Set UP
    for i, vm in enumerate(vms):
        # Create connection
        sock = setup_connection(vm, stage_id + 1)
        thread_safe = ThreadSock(sock)
        job["SOCKETS"][i] = thread_safe
        # sock_id = f"{stage_id + 1}{vm}S"
        # open_sockets.add(sock_id, thread_safe)

        q = Queue(maxsize = 10240) # Every next stage vm has size of 1024 before blocks
        queues.append(q)
        queue_lock = threading.Lock()
        queue_locks.append(queue_lock)

        thread = threading.Thread(target=listen_acks, args=(q, job["SOCKETS"], i, queue_lock))
        threads.append(thread)
        thread.start()
    
    # Pipe Data
    processed_data = Queue()
    line_number = job["START"]
    last_merge = time()
   
    while 1:
        process_state = process.poll() 
        if (process_state is not None):
            break
        new_line = get_process_output(process, poller)

        if (new_line == b""): # Timeout
            sleep(1)
            last_merge = randomized_sync_log(local_processed_log, log_name, processed_data, last_merge)
            continue
        local_processed_log.write(new_line)
        new_line = new_line.decode('utf-8') # Stdout
        # print(new_line, file=sys.stderr)
        
        dict_data = decode_key_val(new_line) # Get dict
        vm_id, stream_id = dict_data["key"].split(':')
        key_vals = dict_data["value"]

        processed_data.put((f"{stage_id - 1}{vm_id}R", stream_id)) # Already processed on sync return
        if (key_vals is None):
            continue
        for key_val in key_vals:
            output_idx = generate_sha1(str(key_val[0])) % len(vms)
            json_key_val = encode_key_val(key_val[0], key_val[1])
            queues[output_idx].put((line_number, json_key_val))
            json_string = encode_key_val(line_number, json_key_val)
            send_data(job["SOCKETS"], output_idx, json_string)
            line_number += job["NUM_TASKS"]
        last_merge = randomized_sync_log(local_processed_log, log_name, processed_data, last_merge)
    
    # Close socks
    for thread in threads:
        thread.join()

    local_processed_log.close()

def pipe_file(job):
    # return
    process = job["PROCESS"] # subprocess popen
    stage_id = job["JOB_ID"] # job_id
    leader = job["LEADER"]
    processed_data = Queue(maxsize = 10240)
    log_name = get_hydfs_log_name(job)
    local_processed_log = open(log_name, "wb") # Log file
    create(log_name, log_name)
    last_merge = time()
    line_number = job["START"]

    while 1:
        process_state = process.poll() 
        if (process_state is not None):
            break
        new_line = get_process_output(process, None)

        if (new_line == b""): # Timeout
            sleep(1)
            last_merge = randomized_sync_log(local_processed_log, log_name, processed_data, last_merge)
            continue
    
        local_processed_log.write(new_line)
        new_line = new_line.decode('utf-8') # Stdout
        
        dict_data = decode_key_val(new_line) # Get dict
        vm_id, stream_id = dict_data["key"].split(':')
        key_vals = dict_data["value"]

        processed_data.put((f"{stage_id - 1}{vm_id}R", stream_id)) # Already processed on sync return
        if (key_vals is None):
            continue
        for key_val in key_vals:
            json_key_val = encode_key_val(key_val[0], key_val[1])
            json_string = encode_key_val(line_number, json_key_val)
            send_data([leader], 0, json_string)
            line_number += job["NUM_TASKS"]
        last_merge = randomized_sync_log(local_processed_log, log_name, processed_data, last_merge)
    local_processed_log.close()

def handle_output(job_id):
    job = current_jobs.get(job_id, copy=False)
    if ("VM" in job):
        job["SOCKETS"] = [0] * len(job["VM"])
        pipe_vms(job)
    elif ("OUTPUT" in job):
        pipe_file(job)

def recover_log(job, stateful):

    global state
    job_id = job["JOB_ID"]
    failed_node_id = job["PREV"]

    file_name = f"{job_id}-{failed_node_id}.log"
    server_id = get_receiver_id_from_file(machine_id, file_name)
    file_version = get_client_file_metadata(file_name)["version"]
    local_cache_path = get_client_file_path(file_name)
    request_file(server_id, file_name, local_cache_path)
    self_own = get_hydfs_log_name(job)

    # creates or appends it to hdfs

    try:
        create(local_cache_path, self_own)
    except:
        append(machine_id, local_cache_path, file_name)

    # Assume stateful

    state = dict()

    # read line by line to modify bool dict
    with open(local_cache_path, "r") as file:
        for line in file:
            if line[0] == 'N':
                line = line[1:]
            if (line == ""):
                continue
            # print("LINE"line, file=sys.stderr)
            key_vals = decode_key_val(line)
            key = key_vals["key"]
            _, line_num = key.split(":")
            processed_streams.add((job_id, line_num), True)
            if stateful:
                values = key_vals["value"]
                if (values is None): continue
                for val in value:
                    state[val[0]] = val[1]
        pipe_input(job["PROCESS"], (encode_key_val("STATE", json.dumps(state)) + "\n").encode())


def prepare_execution(leader_socket):
    job_metadata = json.loads(leader_socket.recv(1024 * 1024))
    print("CHILD", job_metadata, file=sys.stderr)
    operation_exe = job_metadata["PATH"]
    job_id = int(job_metadata["JOB_ID"]) # Job id

    error_f = open("error.txt", "w")
    process = subprocess.Popen(
        operation_exe + [str(machine_id)],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=error_f
    )
    sleep(0.5)
    make_non_blocking(process.stdout.fileno())
    job_metadata["PROCESS"] = process

    stateful = process.stdout.readline()
    print("Stateul?", stateful)
    if ("PREV" in job_metadata):
        recover_log(job_metadata, stateful == "STATEFUL")

    if ("OUTPUT" in job_metadata):
        job_metadata["LEADER"] = ThreadSock(leader_socket)
    current_jobs.add(job_id, job_metadata)

    writer = threading.Thread(target=handle_output, args=(job_id,))
    writer.daemon = True
    writer.start()
    leader_socket.sendall('D'.encode())

def pipe_input(process, line):
    process.stdin.write(line)
    process.stdin.flush()

def run_job(job_id, client_id, client: socket.socket):
    # job_id = receive_int(client) # MODE + JOB_ID
    job = current_jobs.get(job_id) 
    print("RUNNING NEW JOB", file=sys.stderr)
    process = job["PROCESS"] # subprocess Popen()
    
    while (1):
        try:
            data_length = client.recv(4)
        except (ConnectionResetError):
            # Previous Node crashed Wait for next to connect
            return

        if (data_length == b''):
            break
        data_length = from_bytes(data_length)
        data = client.recv(data_length).decode('utf-8')

        received_stream = decode_key_val(data)
        line_number = received_stream["key"]
        
        stream = received_stream["value"]

        if (processed_streams.get((job_id, line_number))): # CHANGE!!!!!
            # If already processed
            send_int(client, line_number)
            continue
        
        processed_streams.add((job_id, line_number), True) # CHANGE!!!!!
        new_key = f"{client_id}:{line_number}"
        p_input = encode_key_val(new_key, stream) + '\n'
        pipe_input(process, p_input.encode())

    # process.stdin.close()    

def send_data(sockets, idx, data):
    while (1):
        socket = sockets[idx] #open_sockets.get(socket_key, copy=False)
        try:
            send_int(socket, len(data))
            socket.sendall(data.encode())
            return
        except (ConnectionResetError):
            sleep(5)

def partition_file(leader_socket: socket.socket):
    # We should ignore unmerged data so only bring next stage vm id
    leader_socket.sendall('D'.encode())
    job_metadata = json.loads(leader_socket.recv(1024 * 1024))
    filename = job_metadata["FILE"] # Read from
    num_tasks = int(job_metadata["NUM_TASKS"]) # How many nodes
    key = int(job_metadata["KEY"]) # If Hash % num_tasks send to vm_id
    vm_id = int(job_metadata["VM"][0]) # vm to send data to
    stage_id = int(job_metadata["JOB_ID"]) # job_id
        
    current_jobs.add(stage_id, job_metadata)
    queue = Queue(maxsize=10240)
    queue_lock = threading.Lock()
    next_stage = setup_connection(vm_id, stage_id + 1)
    client_id = get_ip_id(next_stage)
    # sock_id = sock_id = f"{stage_id + 1}{client_id}S"
    # open_sockets.add(sock_id, next_stage)
    job_metadata["SOCKETS"] = [None] * num_tasks
    job_metadata["SOCKETS"][key] = ThreadSock(next_stage)

    thread = threading.Thread(target=listen_acks, args=(queue, job_metadata["SOCKETS"], key, queue_lock))
    thread.daemon = True
    thread.start()
    
    # Suppose key can't have commas
    
    file_path = get_server_file_path(filename)
    
    with open(file_path, "r") as file:
        linenumber = 1
        while (1):
            line = file.readline()
            if (not line):
                sleep(1000)
                continue
            stream_id = f"{filename}:{linenumber}"
            hash_parition = generate_sha1(stream_id)
            
            if (hash_parition % num_tasks == key):
                stream = encode_key_val(stream_id, line) # <filename:linenumber, line>                    
                key_val = encode_key_val(linenumber, stream) # <id, stream> for acks
                queue.put((linenumber, stream), block=True)
                send_data(job_metadata["SOCKETS"], key, key_val)
            linenumber += 1

def setup_connection(vm_id, job_id):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(RECEIVE_TIMEOUT)
    sock.connect((HOSTS[vm_id - 1], RAINSTORM_PORT))
    sock.sendall(RUN.encode('utf-8')) # sends run request
    send_int(sock, job_id) # send job _id
    return sock

def update_connection(leader):
    new_data = leader.recv(1024 * 1024)
    new_data.decode('utf-8')
    leader.sendall("D".encode())
    config = decode_key_val(new_data)

    # MESSAGE FORM: update_message = {"VM": failed, "STAGE": stage, "NEW": new_vm}

    failed = config["VM"]
    stage = config["STAGE"] # (job_id, "stage1")
    new_vm = config["NEW"] # [vm_id_1, vm_id2, ...]


    if (current_jobs.contains(stage - 1)): # Look for sender and update their sockets
        job = current_jobs.get(stage - 1, copy=False)
        if ("VM" in job and failed in job["VM"]):
            idx = job["VM"].index(failed)
            new_sock = setup_connection(new_vm, stage)
            job["VM"][idx] = new_vm
            job["SOCKETS"][idx].replace(new_sock)

    # open_
def get_ip_id(sock):
    return id_from_ip(socket.gethostbyaddr(sock.getpeername()[0])[0])


def handle_client(client: socket.socket, ip_address):
    mode = client.recv(1).decode('utf-8')
    if (mode == READ):
        partition_file(client)
    elif (mode == EXECUTE):
        prepare_execution(client)
    elif (mode == RUN):
        stage_id = receive_int(client)
        thread_sock = ThreadSock(client)
        sock_id = get_ip_id(client)
        current_jobs.get(stage_id, copy=False)
        open_sockets.add(f"{stage_id - 1}{sock_id}R", thread_sock) # IP address to client socket
        run_job(stage_id, sock_id, thread_sock)
    elif (mode == UPDATE):
        # Updates connection on failures
        update_connection(client)
        return



def start_server(my_id: int):
    """
    Creates a server that listens on a specified port and handles client connections.
    It constantly waits for new connections and creates a new thread to handle each client connection.

    Parameters:
        machine_id (str): The ID of the machine.
    """
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    
    # Lets server reuse address so that it can relaunch quickly
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    # server.settimeout(RECEIVE_TIMEOUT)
    server.bind((HOSTS[my_id - 1], RAINSTORM_PORT))

    server.listen(MAX_CLIENTS)
    global machine_id
    global processed_streams

    machine_id = my_id
    processed_streams = Dict(bool)

    global current_jobs
    current_jobs = Dict(dict)
    sleep(7)

    global open_sockets
    open_sockets = Dict(socket.socket)
    
    global member_list
    member_list = set(get_machines())
    while True:
        client_socket, ip_address = server.accept()

        client_handler = threading.Thread(target=handle_client, args=(client_socket, ip_address,))
        
        # sets daemon to true so that there is no need of joining threads once thread finishes
        client_handler.daemon = True
        client_handler.start()

if __name__ == "__main__":
    start_server(int(sys.argv[1]))
