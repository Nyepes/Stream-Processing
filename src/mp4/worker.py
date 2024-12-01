import socket
from time import sleep
import threading
import json
import subprocess
import sys
import random
from queue import Queue
from collections import defaultdict
from src.shared.DataStructures.mem_table import MemTable
from src.shared.constants import RECEIVE_TIMEOUT, HOSTS, RAINSTORM_PORT, MAX_CLIENTS
from src.mp4.constants import READ, EXECUTE, RUN
from src.mp3.shared import get_machines, generate_sha1, append, get_server_file_path, merge
from src.shared.DataStructures.Dict import Dict

SYNC_PROBABILITY = 1/500

## Plan

# Start Server
# If Read then read files and find next available node to send

member_list = None # All jobs
current_jobs = None # Job Id to configuration of the job (executable, next stage vms, keys, etc...)
machine_id = None # Id of the machine
processed_streams = None # Dict (<sender, line_num>)


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
    if (in_bytes):
        key = key.encode('utf-8')
        val = val.encode('utf-8')
    return f"{to_bytes(len(key))}{key}{to_bytes(len(val))}{val}"

def decode_key_val_list(key_val_list):
    # TODO: fix
    idx = 0
    result = []
    while (idx < len(key_val_list)):
        key_val = decode_key_val(key_val_list[idx:])
        result.append(key_val)
        idx += 8 + len(key_val[0]) + len(key_val[1])
    return result

def decode_key_val(line):
    # TODO: fix
    key_length = from_bytes(line[:4])
    key = line[4: key_length]
    val_length = from_bytes(line[4 + key_length: 8 + key_length])
    val = line[8 + key_length: val_length + 8 + key_length]
    return (key, val) # bstr, bstr

def randomized_sync_log(local_log, hydfs_log, sender_sock, processed: list):
    # TODO: Send ack of ids that were already processed not sure how to do quite yet
    if (random.random() < SYNC_PROBABILITY):
        append(machine_id, local_log, hydfs_log)
        merge(hydfs_log)
        for processed_input in processed:
            sender_sock.sendall(to_bytes(processed_input))
        processed.clear()

def resend_queue(queue, sock):
    new_queue = Queue()

    while(not queue.empty()):
        key_val = q.get()
        sock.sendall(encode_key_val(key_val))
        new_queue.put(key_val)


def listen_acks(queue, sock):
    # Queue (key, val)
    sock.settimeout(RECEIVE_TIMEOUT)
    while(1):
        try:
            ack_num = sock.recv(4)
        except (ConnectionRefusedError, socket.timeout):
            resend_queue(queue, sock)
            continue


        if (ack_num == b""):
            break

        ack = from_bytes(ack_num)

        line_number, value = queue[0]
        if (line_number == ack):
            queue.get()
        

def pipe_vms(job):
    process = job["PROCESS"]
    vms = job["VM"]
    job_id = job["JOB_ID"]
    local_processed_log = open(get_hydfs_log_name(job), "wb")
    socks = []
    queues = []
    threads = []
    # Start connections
    for vm in vms:
        vm_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        vm_sock.connect((HOSTS[vm - 1], RAINSTORM_PORT))
        vm_sock.sendall(RUN.encode('utf-8'))
        send_int(vm_sock, job_id)
        socks.append(vm_sock)
        q = Queue(maxsize = 1024 // len(vms))
        queues.append(q)

        thread = threading.Thread(target=listen_acks, args=(q, vm_sock))
        threads.append(thread)
        thread.start()
    
    # Pipe Data
    processed_data = defaultdict(list)
    line_number = 0
    while process.poll() is None:
        print("vm new_line")
        new_line = get_process_output(process, local_processed_log)
        if (new_line == b""):
            break
        input_id, output_list = decode_key_val(new_line) # input: (vm_id, input_id) Output List b"[(key,val), (key,val)...]"
        key_vals = decode_key_val_list(output_list) # b"[(key, val), ...]"
        # print("input: ", input_id.decode('utf-8'))
        vm_id, line_num = input_id.decode('utf-8').split(':')
        processed_data[vm_id].append(line_num)
        print("input", key_vals)
        for key_val in key_vals:
            output_id = vms[generate_sha1(key.decode('utf-8')) % len(vms)]
            randomized_sync_log(local_processed_log, get_hydfs_log_name(job), HOSTS[vm_id], processed_data[vm_id])
            queues[output_id].put((line_number, key_val))
            print("HELLO: ", key_val)
            socks[output_id].sendall(encode_key_val(line_number, encode_key_val(key_val)))
            line_number += 1
    
    # Close socks
    for thread in threads:
        thread.join()
    for sock in socks:
        sock.close()
    local_processed_log.close()

def pipe_file(job):
    process = job["PROCESS"]
    output_file = job["OUTPUT"]
    processed_data = defaultdict(list)
    with open(output_file, "wb") as output:
        while process.poll() is None:
            print("file new_line")
            new_line = get_process_output(process, output)
            input_id, output_list = decode_key_val(new_line) # input: (vm_id, input_id) Output List b"[(key,val), (key,val)...]"
            key_vals = decode_key_val_list(output_list) # [(key, val), ...]
            vm_id, line_num = input_id.split(':')
            
            processed_data[vm_id].append(line_num)

            # TODO: change hydfs file to just one
            randomized_sync_log(output, get_hydfs_log_name(job), HOSTS[vm_id], processed_data[vm_id])

    append(machine_id, output_file, output_file)

def handle_output(job_id):
    job = current_jobs.get(job_id)
    print(f"Handle Output: {job}")
    if ("VM" in job):
        pipe_vms(job)
    elif ("OUTPUT" in job):
        pipe_file(job)     

def get_process_output(process, local_file):
    line = process.stdout.readline() # <input: [(key,val), (key,val),...]>
    local_file.write(line) # Store in persistent storage
    return line

def pipe_input(process, line):
    process.stdin.write(line)
    process.stdin.flush()

def run_job(client: socket.socket):
    job_id = receive_int(client)
    job = current_jobs.get(job_id)
    process = job["PROCESS"]
    client_id = client.getpeername()
    while (1):
        line_number_length = client.recv(4)
        if (line_number_length == b''):
            break
        line_number_length = from_bytes(line_number_length)
        line_number = client.recv(line_number_length).decode('utf-8')
        
        stream_length = from_bytes(client.recv(4))
        stream = client.recv(stream_length) # 
        if (processed_streams.get(job_id, client_id, line_number)):
            continue
        
        processed_streams.add((job_id, client_id, line_number), True) # set == hashmap(key, bool)
        new_key = f"{client_id}:{line_number}"
        p_input = encode_key_val(new_key, stream)
        pipe_input(process, p_input)

    process.stdin.close()    

def partition_file(leader_socket: socket.socket):
    # We should ignore unmerged data so only bring next stage vm id

    job_metadata = json.loads(leader_socket.recv(1024 * 1024))
    filename = job_metadata["FILE"]
    num_tasks = int(job_metadata["NUM_TASKS"])
    key = int(job_metadata["KEY"])
    vm_id = int(job_metadata["VM"])
    job_id = int(job_metadata["JOB_ID"])

    # TODO: Create thread to listen for acks and update queue

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as next_stage:
        next_stage.settimeout(RECEIVE_TIMEOUT)
        next_stage.connect((HOSTS[vm_id - 1], RAINSTORM_PORT))
        next_stage.sendall(RUN.encode('utf-8'))
        send_int(next_stage, job_id)
        queue = Queue(maxsize=1024)
        # Suppose key can't have commas
        with open(get_server_file_path(filename), "r") as file:
            linenumber = 0
            while (1):
                line = file.readline()
                if (line is None):
                    break
                key = f"{filename}:{linenumber}"
                # Maybe check if key has comma
                hash_parition = generate_sha1(key)
                if (hash_parition % num_tasks == key):
                    stream = encode_key_val(key, line, in_bytes=True)
                    # stream = f"{key}, {line}\n" #TODO: Maybe better serialization so that files with : or line with , keep working
                    key_val = encode_key_val(linenumber, stream, in_bytes=True)
                    queue.put((line_number, stream), block=True)
                    next_stage.sendall(key_val) # Since TCP ordered
                linenumber += 1
    
def prepare_execution(leader_socket):
    job_metadata = json.loads(leader_socket.recv(1024 * 1024))
    operation_exe = job_metadata["PATH"]
    job_id = int(job_metadata["JOB_ID"]) # Job id
    print(operation_exe)

    process = subprocess.Popen(
        operation_exe.split(" "),
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE
    )
    del job_metadata["PATH"]
    job_metadata["PROCESS"] = process
    current_jobs.add(job_id, job_metadata)

    # Handle Output
    writer = threading.Thread(target=handle_output, args=(job_id,))
    writer.daemon = True
    writer.start()

def handle_client(client: socket.socket, ip_address):
    mode = client.recv(1).decode('utf-8')
    if (mode == READ):
        partition_file(client)
    elif (mode == EXECUTE):
        prepare_execution(client)
    elif (mode == RUN):
        run_job(client)
    
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
    server.settimeout(RECEIVE_TIMEOUT)
    server.bind((HOSTS[my_id - 1], RAINSTORM_PORT))

    server.listen(MAX_CLIENTS)
    global machine_id
    global processed_streams

    machine_id = my_id
    processed_streams = Dict(bool)

    global current_jobs
    current_jobs = Dict(dict)
    sleep(7)
    
    global member_list
    member_list = set(get_machines())
    print("Worker Accepting Connections...")
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
