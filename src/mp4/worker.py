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
    json_str = json.dumps({
        "key": key, 
        "value": val
    })
    return json_str

def decode_key_val(line):
    return json.loads(line)

def get_process_output(process, local_file):
    line = process.stdout.readline() # <input: [(key,val), (key,val),...]>
    local_file.write(line) # Store in persistent storage
    return line


def randomized_sync_log(local_log, hydfs_log, sender_sock, processed: list):
    # TODO: Send ack of ids that were already processed not sure how to do quite yet
    if (random.random() <= SYNC_PROBABILITY or len(processed) >= 500):
        append(machine_id, local_log, hydfs_log)
        merge(hydfs_log)
        # for processed_input in processed:
        #     sender_sock.sendall(to_bytes(processed_input))
        processed.clear()

def pipe_vms(job):
    process = job["PROCESS"] # subprocess popen
    vms = job["VM"] # next stage vms
    job_id = job["JOB_ID"] # job_id
    local_processed_log = open(get_hydfs_log_name(job), "wb") # Log file
    
    socks = []
    queues = []
    threads = []

    # Set UP
    for vm in vms:
        # Create connection
        vm_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        vm_sock.connect((HOSTS[vm - 1], RAINSTORM_PORT))
        
        # Send RUN request
        vm_sock.sendall(RUN.encode('utf-8'))
        # Send job_id
        send_int(vm_sock, job_id)

        socks.append(vm_sock)

        q = Queue(maxsize = 1024) # Every next stage vm has size of 1024 before blocks
        queues.append(q)
        # TODO: Listen Acks
        # thread = threading.Thread(target=listen_acks, args=(q, vm_sock))
        # threads.append(thread)
        # thread.start()
    
    # Pipe Data
    processed_data = defaultdict(list)
    line_number = 0

    while process.poll() is None:
        new_line = get_process_output(process, local_processed_log)
        if (new_line == b""):
            break
        new_line = new_line.decode('utf-8') # Stdout
        dict_data = decode_key_val(new_line) # Get dict
        
        print(f"STDOUT Stream: {new_line}")
        vm_id, stream_id = dict_data["key"] .split(':')
        key_vals = dict_data["value"]
        vm_id = int(vm_id)

        processed_data[vm_id].append(stream_id) # Already processed on sync return

        for key_val in key_vals:
            output_idx = generate_sha1(str(key_val[0])) % len(vms)
            # output_id = vms[output_idx]
            randomized_sync_log(local_processed_log.name, get_hydfs_log_name(job), HOSTS[vm_id], processed_data[vm_id])
            queues[output_idx].put((line_number, key_val))
            json_string = encode_key_val(line_number, key_val).encode()
            socks[output_idx].sendall(encode_key_val(line_number, key_val).encode())
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
            print(new_line.decode('utf-8'))
            length = from_bytes(new_line[:4])
            print(length)
            print(new_line[4:4 + length])

            input_id, output_list = decode_key_val(new_line) # input: (vm_id, input_id) Output List b"[(key,val), (key,val)...]"
            key_vals = decode_key_val_list(output_list) # [(key, val), ...]
            vm_id, line_num = input_id.split(':')
            
            processed_data[vm_id].append(line_num)

            # TODO: change hydfs file to just one
            randomized_sync_log(output, get_hydfs_log_name(job), HOSTS[vm_id], processed_data[vm_id])
    append(machine_id, output_file, output_file)
def handle_output(job_id):
    job = current_jobs.get(job_id)
    if ("VM" in job):
        pipe_vms(job)
        pass
    elif ("OUTPUT" in job):
        pass
        # pipe_file(job)    



def prepare_execution(leader_socket):
    job_metadata = json.loads(leader_socket.recv(1024 * 1024))
    operation_exe = job_metadata["PATH"]
    job_id = int(job_metadata["JOB_ID"]) # Job id

    process = subprocess.Popen(
        operation_exe.split(" ") + [str(machine_id)],
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


def pipe_input(process, line):
    process.stdin.write(line)
    process.stdin.flush()

def run_job(client: socket.socket):
    job_id = receive_int(client) # MODE + JOB_ID
    job = current_jobs.get(job_id) 


    process = job["PROCESS"] # subprocess Popen()
    client_id = client.getpeername()[0] # If we need it

    while (1):
        data_length = client.recv(4)
        if (data_length == b''):
            break
        data_length = from_bytes(data_length)
        data = client.recv(data_length).decode('utf-8')
        received_stream = decode_key_val(data)
        print(f"RECEIVED STREAM: {received_stream}")
        line_number = received_stream["key"]
        stream = decode_key_val(received_stream["value"])

        if (processed_streams.get((job_id, client_id, line_number))):
            # If already processed
            continue
        
        processed_streams.add((job_id, client_id, line_number), True) # set == hashmap(key, bool)
        new_key = f"{client_id}:{line_number}"
        p_input = encode_key_val(new_key, stream)
        pipe_input(process, p_input)

    process.stdin.close()    

def partition_file(leader_socket: socket.socket):
    # We should ignore unmerged data so only bring next stage vm id

    job_metadata = json.loads(leader_socket.recv(1024 * 1024))
    filename = job_metadata["FILE"] # Read from
    num_tasks = int(job_metadata["NUM_TASKS"]) # How many nodes
    key = int(job_metadata["KEY"]) # If Hash % num_tasks send to vm_id
    vm_id = int(job_metadata["VM"]) # vm to send data to
    job_id = int(job_metadata["JOB_ID"]) # job_id

    # TODO: Create thread to listen for acks and update queue

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as next_stage:
        next_stage.settimeout(RECEIVE_TIMEOUT)
        next_stage.connect((HOSTS[vm_id - 1], RAINSTORM_PORT))
        
        next_stage.sendall(RUN.encode('utf-8')) # sends run request

        send_int(next_stage, job_id) # send job _id

        queue = Queue(maxsize=1024)

        # Suppose key can't have commas
        file_path = get_server_file_path(filename)

        with open(file_path, "r") as file:
            linenumber = 0
            while (1):
                line = file.readline()
                if (not line):
                    break
                stream_id = f"{filename}:{linenumber}"
                hash_parition = generate_sha1(stream_id)
                
                if (hash_parition % num_tasks == key):
                    stream = encode_key_val(stream_id, line) # <filename:linenumber, line>                    
                    key_val = encode_key_val(linenumber, stream) # <id, stream> for acks
                    queue.put((linenumber, stream), block=True)
                    send_int(next_stage, len(key_val))
                    next_stage.sendall(key_val.encode())
                linenumber += 1

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
