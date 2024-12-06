import socket
from time import sleep, time
import threading
import json
import subprocess
import sys
import random
import copy
import select

from queue import Queue
from collections import defaultdict
from src.shared.DataStructures.mem_table import MemTable
from src.shared.constants import RECEIVE_TIMEOUT, HOSTS, RAINSTORM_PORT, MAX_CLIENTS
from src.mp4.constants import READ, EXECUTE, RUN
from src.mp3.shared import get_machines, generate_sha1, append, get_server_file_path, merge, id_from_ip, create
from src.shared.DataStructures.Dict import Dict
from src.shared.ThreadSock import ThreadSock


SYNC_PROBABILITY = 1/1000

## Plan

# Start Server
# If Read then read files and find next available node to send

member_list = None # All jobs
current_jobs = None # Job Id to configuration of the job (executable, next stage vms, keys, etc...)
machine_id = None # Id of the machine
processed_streams = None # Dict (<sender, line_num>)

open_sockets = None


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
        line = process.stdout.readline() # <input: [(key,val), (key,val),...]
        return line
    events = poller.poll(timeout_sec * 1000)
    if (events):
        line = process.stdout.readline() # <input: [(key,val), (key,val),...]
        return line
    return ""



def randomized_sync_log(local_log, hydfs_log, processed: Queue, last_merge):
    # TODO: Send ack of ids that were already processed not sure how to do quite yet
    cur_time = time()
    if (last_merge + 4 < cur_time):
        local_log.flush()
        log_name = local_log.name
        
        append(machine_id, log_name, hydfs_log)
        merge(hydfs_log)
        while (not processed.empty()):
            val = processed.get()
            sender_sock = open_sockets.get(val[0], copy=False)
            send_int(sender_sock, int(val[1]))
            # try:
            # sender_sock.sendall(to_bytes(processed_input))
            # except:
            #     pass
                # TODO: Check failures and change sockets
        local_log.truncate(0)
        local_log.seek(0, 0)

    return cur_time

        
def listen_acks(queue, sock, queue_lock):
    # Queue (key, val)
    sock.settimeout(5)
    while(1):
        try:
            ack_num = sock.recv(4)
        except (ConnectionRefusedError, socket.timeout):
            
            queue_len = queue.qsize()

            if (queue_len > 0):
                with queue_lock:
                    queue_copy = Queue(maxsize=1024)
                    for i in range(queue_len):
                        val = queue.get()
                        queue_copy.put(val)
                        queue.put(val)
            
                resend_queue(queue_copy, sock)
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
        json_encoded = encode_key_val(line_number, key_val).encode()
        send_int(sock, len(json_encoded))
        sock.sendall(json_encoded)

def pipe_vms(job):
    process = job["PROCESS"] # subprocess popen
    vms = job["VM"] # next stage vms
    poller = job["POLLER"]
    job_id = job["JOB_ID"] # job_id

    log_name = get_hydfs_log_name(job)
    local_processed_log = open(log_name, "wb") # Log file
    create(log_name, log_name)
    
    socks = []
    queues = []
    threads = []
    queue_locks = []
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
        queue_lock = threading.Lock()
        queue_locks.append(queue_lock)

        thread = threading.Thread(target=listen_acks, args=(q, vm_sock, queue_lock))
        threads.append(thread)
        thread.start()
    
    # Pipe Data
    processed_data = Queue()
    line_number = 1
    last_merge = time()
   
    while 1:
        process_state = process.poll() 
        if (process_state is not None):
            break
        new_line = get_process_output(process, poller)

        if (new_line == ""): # Timeout
            last_merge = randomized_sync_log(local_processed_log, log_name, processed_data, last_merge)
            continue
        local_processed_log.write(new_line)
        new_line = new_line.decode('utf-8') # Stdout
        dict_data = decode_key_val(new_line) # Get dict
        
        vm_id, stream_id = dict_data["key"].split(':')

        key_vals = dict_data["value"]
        # vm_id = int(vm_id)
        processed_data.put((vm_id, stream_id)) # Already processed on sync return
        if (key_vals is None):
            continue
        for key_val in key_vals:
            output_idx = generate_sha1(str(key_val[0])) % len(vms)
            json_key_val = encode_key_val(key_val[0], key_val[1])
            queues[output_idx].put((line_number, json_key_val))
            json_string = encode_key_val(line_number, json_key_val).encode()
            # send_int(socks[output_idx], len(json_string))
            # socks[output_idx].sendall(json_string)
            line_number += 1
        last_merge = randomized_sync_log(local_processed_log, log_name, processed_data, last_merge)
    
    # Close socks
    for thread in threads:
        thread.join()
    
    for sock in socks:
        sock.close()

    local_processed_log.close()


def pipe_file(job):
    process = job["PROCESS"]
    poller = job["POLLER"]
    output_file = job["OUTPUT"]
    processed_data = Queue() #Unacked
    last_merge = time()
    # local_processed_log = open(get_hydfs_log_name(job), "wb") # Log file
    with open(output_file, "wb") as output:
        while 1:
            process_state = process.poll() 
            if (process_state is not None):
                break
            
            new_line = get_process_output(process, poller)

            if (new_line == ""):
                last_merge = randomized_sync_log(output, output_file, processed_data, last_merge)
                continue
            
            new_line = new_line.decode('utf-8')
            dict_data = decode_key_val(new_line) # input: (vm_id, input_id) Output List b"[(key,val), (key,val)...]"
            
            vm_id, stream_id = dict_data["key"].split(':')
            key_vals = dict_data["value"]
            # vm_id = int(vm_id)
            
            processed_data.put((vm_id,stream_id))
            for key_val in key_vals:    
                output.write(f"{key_val[0]}:{key_val[1]}\n".encode())
            last_merge = randomized_sync_log(output, output_file, processed_data, last_merge)
            
    # randomized_sync_log(output, output_file, 0, processed_data)

def handle_output(job_id):
    job = current_jobs.get(job_id)
    if ("VM" in job):
        pipe_vms(job)
    elif ("OUTPUT" in job):
        pipe_file(job)    




def prepare_execution(leader_socket):
    job_metadata = json.loads(leader_socket.recv(1024 * 1024))
    operation_exe = job_metadata["PATH"]
    job_id = int(job_metadata["JOB_ID"]) # Job id

    process = subprocess.Popen(
        operation_exe.split(" ") + [str(machine_id)],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE
    )
    poller = select.poll()
    poller.register(process.stdout, select.POLLIN)
    job_metadata["POLLER"] = poller

    del job_metadata["PATH"]
    job_metadata["PROCESS"] = process
    current_jobs.add(job_id, job_metadata)

    # Handle Output
    writer = threading.Thread(target=handle_output, args=(job_id,))
    writer.daemon = True
    writer.start()
    leader_socket.sendall('D'.encode())


def pipe_input(process, line):
    process.stdin.write(line)
    process.stdin.flush()

def run_job(client: socket.socket):
    job_id = receive_int(client) # MODE + JOB_ID
    job = current_jobs.get(job_id) 


    process = job["PROCESS"] # subprocess Popen()
    client_id = client.get_socket().getpeername()

    while (1):
        data_length = client.recv(4)
        if (data_length == b''):
            break
        data_length = from_bytes(data_length)
        try:
            rec = client.recv(data_length)
            data = rec.decode('utf-8')
        except:
            print("DECODING")
            print(data_length, len(rec), rec)
        try:
            received_stream = decode_key_val(data)
        except:
            print(data_length, len(rec), rec)
            print("NOT JSON")
            print(data)
        line_number = received_stream["key"]
        
        stream = received_stream["value"]

        if (processed_streams.get((job_id, client_id, line_number))):
            send_int(client, line_number)
            continue
        
        processed_streams.add((job_id, client_id, line_number), True) # set == hashmap(key, bool)
        new_key = f"{client_id}:{line_number}"
        p_input = encode_key_val(new_key, stream) + '\n'
        pipe_input(process, p_input.encode())

    # process.stdin.close()    

def partition_file(leader_socket: socket.socket):
    # We should ignore unmerged data so only bring next stage vm id
    leader_socket.sendall('D'.encode())
    job_metadata = json.loads(leader_socket.recv(1024 * 1024))
    filename = job_metadata["FILE"] # Read from
    num_tasks = int(job_metadata["NUM_TASKS"]) # How many nodes
    key = int(job_metadata["KEY"]) # If Hash % num_tasks send to vm_id
    vm_id = int(job_metadata["VM"]) # vm to send data to
    job_id = int(job_metadata["JOB_ID"]) # job_id
        
    queue = Queue(maxsize=1024)
    queue_lock = threading.Lock()
    next_stage = socket.socket(socket.AF_INET, socket.SOCK_STREAM)


    next_stage.settimeout(RECEIVE_TIMEOUT)
    next_stage.connect((HOSTS[vm_id - 1], RAINSTORM_PORT))
    next_stage.sendall(RUN.encode('utf-8')) # sends run request

    thread = threading.Thread(target=listen_acks, args=(queue, next_stage, queue_lock))
    thread.daemon = True
    thread.start()


    send_int(next_stage, job_id) # send job _id


    # Suppose key can't have commas
    file_path = get_server_file_path(filename)

    with open(file_path, "r") as file:
        linenumber = 1
        while (1):
            line = file.readline()
            if (not line):
                continue
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
        thread_sock = ThreadSock(client)
        open_sockets.add(str(ip_address), thread_sock) # IP address to client socket
        partition_file(thread_sock)
    elif (mode == EXECUTE):
        prepare_execution(client)
    elif (mode == RUN):
        thread_sock = ThreadSock(client)
        open_sockets.add(str(ip_address), thread_sock) # IP address to client socket
        run_job(thread_sock)



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

    global open_sockets
    open_sockets = Dict(socket.socket)
    
    global member_list
    member_list = set(get_machines())
    while True:
        try:
            client_socket, ip_address = server.accept()
        except (ConnectionRefusedError, socket.timeout):
            # On timeout check if failures and update open_sockets
            continue
        # Creates a new thread for each client
        client_handler = threading.Thread(target=handle_client, args=(client_socket, ip_address,))
        
        # sets daemon to true so that there is no need of joining threads once thread finishes
        client_handler.daemon = True
        client_handler.start()

if __name__ == "__main__":
    start_server(int(sys.argv[1]))
