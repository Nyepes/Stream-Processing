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
from src.Streaming.constants import READ, EXECUTE, RUN, UPDATE
from src.FileSystem.shared import get_machines, generate_sha1, append, get_server_file_path, merge, id_from_ip, create, get_receiver_id_from_file, get_client_file_metadata, get_client_file_path, request_file
from src.shared.DataStructures.Dict import Dict
from src.shared.ThreadSock import ThreadSock
import fcntl

# Probability of syncing logs between nodes
SYNC_PROBABILITY = 1/1000

# Global state variables
member_list = None      # Set of all machines in the cluster
current_jobs = None     # Dict mapping job IDs to job configurations
machine_id = None       # ID of this machine
processed_streams = None # Dict tracking processed streams by (sender, line_num)
open_sockets = None     # Dict mapping (stage_id, vm_id, "R"/"S") to socket connections
state = None           # State for stateful operations

def send_int(sock, int_val: int):
    """Send an integer over a socket by converting to bytes"""
    sock.sendall(int_val.to_bytes(4, byteorder="little"))

def receive_int(sock):
    """Receive an integer from a socket and convert from bytes"""
    return int.from_bytes(sock.recv(4), byteorder="little")

def to_bytes(val, length = 4):
    """Convert integer to bytes with specified length"""
    return val.to_bytes(length, byteorder="little")

def from_bytes(val):
    """Convert bytes to integer"""
    return int.from_bytes(val, byteorder="little")

def get_hydfs_log_name(job):
    """Get log filename for a job on this machine"""
    return f'{job["JOB_ID"]}-{machine_id}.log'

def encode_key_val(key:str, val:str, in_bytes = True):
    """Encode key-value pair as JSON string"""
    json_str = json.dumps({
        "key": key, 
        "value": val
    })
    return json_str

def decode_key_val(line):
    """Decode JSON string into key-value pair"""
    return json.loads(line)

def get_process_output(process, poller, timeout_sec = 5):
    """Get output from a process, optionally using a poller with timeout"""
    if (poller is None):
        process.stdout.flush()
        line = process.stdout.readline()
        return line
    
    events = poller.poll(timeout_sec * 1000)
    if not events:
        return ""
        
    line = process.stdout.readline()
    if not line:
        return ""
        
    return line

def randomized_sync_log(local_log, hydfs_log, processed: Queue, last_merge):
    """
    Sync local log to HYDFS periodically or when queue gets too large.
    Also handles sending acknowledgements for processed messages.
    
    Args:
        local_log: Local log file
        hydfs_log: HYDFS log file
        processed: Queue of processed messages
        last_merge: Timestamp of last merge
        
    Returns:
        New last_merge timestamp
    """
    cur_time = time()
    if (processed.qsize() >= 30 or last_merge + 0.1 < cur_time):
        local_log.flush()
        log_name = local_log.name
        append(machine_id, log_name, hydfs_log)
        merge(hydfs_log)
        sleep(1)
        qsize = processed.qsize()
        for i in range(qsize):
            val = processed.get()
            if not isinstance(val, tuple):
                continue
            sender_sock = open_sockets.get(val[0], copy=False)
            try:
                send_int(sender_sock, int(val[1]))
            except (ConnectionResetError, BrokenPipeError):
                processed.put(val[0], val[1])
        local_log.truncate(0)
        local_log.seek(0, 0)
        last_merge = time()

    return last_merge
      
def listen_acks(queue, socks, sock_idx, queue_lock):
    """
    Listen for acknowledgements from downstream nodes.
    Handles retrying failed sends.
    
    Args:
        queue: Queue of messages waiting for acks
        socks: List of sockets to downstream nodes
        sock_idx: Index of socket to listen on
        queue_lock: Lock for queue access
    """
    timeout = 0.1
    sock = socks[sock_idx]
    while(1):
        if (queue.empty()): 
            # print("EMPTY!", file=sys.stderr)
            timeout = 2 * timeout
            sleep(timeout)
            continue
        try:
            # print("LISTENING!", file=sys.stderr)
            ack_num = sock.recv(4)
        except (ConnectionResetError, BrokenPipeError):
            # print("ERROR!", file=sys.stderr)
            sleep(2)
            sock = socks[sock_idx]
            continue
        except (ConnectionRefusedError, socket.timeout):
            queue_len = queue.qsize()
            # print("RESENDING", file=sys.stderr)
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
                    sleep(1)
                    continue
            continue
        if (ack_num == b""):
            # print("EXITING ACKS", file=sys.stderr)
            # print("SOCKET", sock.get_socket(), file=sys.stderr)
            sock = socks[sock_idx]
            sleep(timeout)
            timeout *= 2
            continue

        ack = from_bytes(ack_num)
        # print("RECEIVED", ack, file=sys.stderr)

        linenumber, value = queue.queue[0]
        # print("FROnT", linenumber, file=sys.stderr)
        if (linenumber == ack):
            # print("ACKING", ack, file = sys.stderr)
            queue.get()
        
def resend_queue(queue, sock):
    """Resend all messages in queue over socket"""
    while(not queue.empty()):
        line_number, key_val = queue.get()
        json_encoded = encode_key_val(line_number, key_val).encode('utf-8')
        # print("RESENDING", line_number, file=sys.stderr)
        send_int(sock, len(json_encoded))
        # print("RESENDING", json_encoded, file=sys.stderr)
        sock.sendall(json_encoded)

def make_non_blocking(fd):
    """Set a file descriptor to non-blocking mode"""
    flags = fcntl.fcntl(fd, fcntl.F_GETFL)
    fcntl.fcntl(fd, fcntl.F_SETFL, flags | os.O_NONBLOCK)

def pipe_vms(job):
    """
    Handle piping data between VMs for a job.
    Sets up connections to downstream nodes and handles sending data.
    
    Args:
        job: Job configuration dict
    """
    process = job["PROCESS"]
    vms = job["VM"] 
    poller = None
    stage_id = job["JOB_ID"]

    log_name = get_hydfs_log_name(job)
    local_processed_log = open(log_name, "wb")
    create(log_name, log_name)
    
    socks = []
    queues = []
    threads = []
    queue_locks = []
    
    # Set up connections to downstream nodes
    for i, vm in enumerate(vms):
        sock = setup_connection(vm, stage_id + 1)
        thread_safe = ThreadSock(sock)
        job["SOCKETS"][i] = thread_safe

        q = Queue(maxsize = 10240)
        queues.append(q)
        queue_lock = threading.Lock()
        queue_locks.append(queue_lock)

        thread = threading.Thread(target=listen_acks, args=(q, job["SOCKETS"], i, queue_lock))
        threads.append(thread)
        thread.start()
    
    # Process and pipe data
    processed_data = Queue()
    line_number = job["START"]
    last_merge = time()
   
    while 1:
        process_state = process.poll() 
        if (process_state is not None):
            break
        new_line = get_process_output(process, poller)

        if (new_line == b""):
            sleep(1)
            last_merge = randomized_sync_log(local_processed_log, log_name, processed_data, last_merge)
            continue
        local_processed_log.write(new_line)
        new_line = new_line.decode('utf-8')
        
        dict_data = decode_key_val(new_line)
        vm_id, stream_id = dict_data["key"].split(':')
        key_vals = dict_data["value"]

        processed_data.put((f"{stage_id - 1}{vm_id}R", stream_id))
        if (key_vals is None):
            continue
        for key_val in key_vals:
            output_idx = generate_sha1(str(key_val[0])) % len(vms)
            json_key_val = encode_key_val(key_val[0], key_val[1])
            queues[output_idx].put((line_number, json_key_val))
            json_string = encode_key_val(line_number, json_key_val)
            print(output_idx, job["SOCKETS"])
            send_data(job["SOCKETS"], output_idx, json_string)
            line_number += job["NUM_TASKS"]
        last_merge = randomized_sync_log(local_processed_log, log_name, processed_data, last_merge)
    
    for thread in threads:
        thread.join()

    local_processed_log.close()

def pipe_file(job):
    """
    Handle piping data to a file for a job.
    Similar to pipe_vms but writes to a single output file.
    
    Args:
        job: Job configuration dict
    """
    process = job["PROCESS"]
    stage_id = job["JOB_ID"]
    leader = job["LEADER"]
    processed_data = Queue(maxsize = 10240)
    log_name = get_hydfs_log_name(job)
    local_processed_log = open(log_name, "wb")
    create(log_name, log_name)
    last_merge = time()
    line_number = job["START"]

    while 1:
        process_state = process.poll() 
        if (process_state is not None):
            break
        new_line = get_process_output(process, None)

        if (new_line == b""):
            sleep(1)
            last_merge = randomized_sync_log(local_processed_log, log_name, processed_data, last_merge)
            continue
    
        local_processed_log.write(new_line)
        new_line = new_line.decode('utf-8')
        print("new line", file=sys.stderr)
        
        dict_data = decode_key_val(new_line)
        vm_id, stream_id = dict_data["key"].split(':')
        key_vals = dict_data["value"]

        processed_data.put((f"{stage_id - 1}{vm_id}R", stream_id))
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
    """Route job output to either VMs or file based on job config"""
    job = current_jobs.get(job_id, copy=False)
    if ("VM" in job):
        job["SOCKETS"] = [0] * len(job["VM"])
        pipe_vms(job)
    elif ("OUTPUT" in job):
        pipe_file(job)

def recover_log(job, stateful):
    """
    Recover state from log after node failure.
    
    Args:
        job: Job configuration
        stateful: Whether job maintains state
    """
    global state
    job_id = job["JOB_ID"]
    failed_node_id = job["PREV"]

    file_name = f"{job_id}-{failed_node_id}.log"
    server_id = get_receiver_id_from_file(machine_id, file_name)
    file_version = get_client_file_metadata(file_name)["version"]
    local_cache_path = get_client_file_path(file_name)
    request_file(server_id, file_name, local_cache_path)
    self_own = get_hydfs_log_name(job)
    
    append(machine_id, local_cache_path, file_name)

    state = dict()

    with open(local_cache_path, "r") as file:
        for line in file:
            if line[0] == 'N':
                line = line[1:]
            if (line == ""):
                continue
            try:
                key_vals = decode_key_val(line)
            except:
                continue
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
    """
    Prepare job for execution by setting up process and recovering state if needed.
    
    Args:
        leader_socket: Socket connection to leader node
    """
    job_metadata = json.loads(leader_socket.recv(1024 * 1024))
    # print("CHILD", job_metadata, file=sys.stderr)
    operation_exe = job_metadata["PATH"]
    job_id = int(job_metadata["JOB_ID"])

    error_f = open("error.txt", "w")
    process = subprocess.Popen(
        operation_exe + [str(machine_id)],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=error_f
    )
    sleep(0.5)
    job_metadata["PROCESS"] = process

    stateful = process.stdout.readline()
    make_non_blocking(process.stdout.fileno())

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
    """Write input line to process stdin"""
    sleep(0.01)
    process.stdin.write(line)
    process.stdin.flush()

def run_job(job_id, client_id, client: socket.socket):
    """
    Run a job by processing input from client socket.
    
    Args:
        job_id: ID of job to run
        client_id: ID of client sending data
        client: Socket connection to client
    """
    job = current_jobs.get(job_id) 
    # print("RUNNING NEW JOB", file=sys.stderr)
    process = job["PROCESS"]
    
    while (1):
        try:
            data_length = client.recv(4)
        except (ConnectionResetError):
            # Failure
            # print("EXITING" )
            return

        if (data_length == b''):
            # print("EXITING" )
            break
        data_length = from_bytes(data_length)
        data = client.recv(data_length).decode('utf-8')
        # print("RECEIVED", data, file=sys.stderr)
        received_stream = decode_key_val(data)
        line_number = received_stream["key"]
        
        stream = received_stream["value"]

        if (processed_streams.get((job_id, line_number)) == True):
            send_int(client, line_number)
            continue
        
        processed_streams.add((job_id, line_number), True)
        new_key = f"{client_id}:{line_number}"
        p_input = encode_key_val(new_key, stream) + '\n'
        pipe_input(process, p_input.encode())

def send_data(sockets, idx, data):
    """
    Send data over socket with retries.
    
    Args:
        sockets: List of sockets
        idx: Index of socket to use
        data: Data to send
    """
    while (1):
        socket = sockets[idx]
        try:
            send_int(socket, len(data))
            socket.sendall(data.encode())
            return
        except (ConnectionResetError, BrokenPipeError):
            sleep(5)

def partition_file(leader_socket: socket.socket):
    """
    Partition input file and distribute to worker nodes.
    
    Args:
        leader_socket: Socket connection to leader node
    """
    print("Partition START")
    leader_socket.sendall('D'.encode())
    job_metadata = json.loads(leader_socket.recv(1024 * 1024))
    filename = job_metadata["FILE"]
    num_tasks = int(job_metadata["NUM_TASKS"])
    key = int(job_metadata["KEY"])
    vm_id = int(job_metadata["VM"][key])
    stage_id = int(job_metadata["JOB_ID"])
        
    current_jobs.add(stage_id, job_metadata)
    queue = Queue(maxsize=10240)
    queue_lock = threading.Lock()
    next_stage = setup_connection(vm_id, stage_id + 1)
    client_id = get_ip_id(next_stage)
    job_metadata["SOCKETS"] = [None] * num_tasks
    job_metadata["SOCKETS"][key] = ThreadSock(next_stage)

    thread = threading.Thread(target=listen_acks, args=(queue, job_metadata["SOCKETS"], key, queue_lock))
    thread.daemon = True
    thread.start()
    
    file_path = get_server_file_path(filename)
    sleep_tmr = 0.1
    with open(file_path, "r") as file:
        linenumber = 1
        while (1):
            line = file.readline()
            if (not line):
                sleep(sleep_tmr * 2)
                sleep_tmr *= 2
                print("Done")
                continue
            stream_id = f"{filename}:{linenumber}"
            hash_parition = generate_sha1(stream_id)
            
            if (hash_parition % num_tasks == key):
                stream = encode_key_val(stream_id, line)                   
                key_val = encode_key_val(linenumber, stream)
                queue.put((linenumber, stream), block=True)
                send_data(job_metadata["SOCKETS"], key, key_val)
            linenumber += 1

def setup_connection(vm_id, job_id):
    """
    Set up socket connection to worker node.
    
    Args:
        vm_id: ID of worker node
        job_id: ID of job
        
    Returns:
        Socket connection to worker
    """
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(RECEIVE_TIMEOUT + 5)
        sock.connect((HOSTS[vm_id - 1], RAINSTORM_PORT))
        sock.sendall(RUN.encode('utf-8'))
        send_int(sock, job_id)
        return sock
    except:
        return None

def update_connection(leader):
    """
    Update connections after node failure.
    
    Args:
        leader: Socket connection to leader node
    """
    new_data = leader.recv(1024 * 1024)
    new_data.decode('utf-8')
    leader.sendall("D".encode())
    config = decode_key_val(new_data)

    failed = config["VM"]
    stage = config["STAGE"]
    new_vm = config["NEW"]

    if (current_jobs.contains(stage - 1)):
        job = current_jobs.get(stage - 1, copy=False)
        if ("VM" in job and failed in job["VM"]):
            idx = job["VM"].index(failed)
            new_sock = setup_connection(new_vm, stage)
            job["VM"][idx] = new_vm
            job["SOCKETS"][idx].replace(new_sock)
            # print("JOB", job)

def get_ip_id(sock):
    """Get ID from socket IP address"""
    return id_from_ip(socket.gethostbyaddr(sock.getpeername()[0])[0])

def handle_client(client: socket.socket, ip_address):
    """
    Handle incoming client connection based on mode.
    
    Args:
        client: Socket connection to client
        ip_address: IP address of client
    """
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
        open_sockets.add(f"{stage_id - 1}{sock_id}R", thread_sock)
        run_job(stage_id, sock_id, thread_sock)
    elif (mode == UPDATE):
        update_connection(client)
        return

def start_server(my_id: int):
    """
    Start server to handle client connections.
    
    Args:
        my_id: ID of this machine
    """
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
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
        
        client_handler.daemon = True
        client_handler.start()

if __name__ == "__main__":
    start_server(int(sys.argv[1]))
