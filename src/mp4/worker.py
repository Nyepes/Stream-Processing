import socket
from time import sleep
import threading
import json
import subprocess
import sys

from src.shared.DataStructures.mem_table import MemTable
from src.shared.constants import RECEIVE_TIMEOUT, HOSTS, RAINSTORM_PORT, MAX_CLIENTS
from src.mp4.constants import READ, EXECUTE, RUN
from src.mp3.shared import get_machines, generate_sha1, append, get_server_file_path
from src.shared.DataStructures.Dict import Dict


## Plan

# Start Server
# If Read then read files and find next available node to send

member_list = None
current_jobs = None # Job Id to configuration of the job (executable, next stage vms, keys, etc...)

def send_int(sock, int_val: int):
    sock.sendall(int_val.to_bytes(4, byteorder="little"))
def receive_int(sock):
    return int.from_bytes(sock.recv(4), byteorder="little")

def pipe_vms(job):
    process = job["PROCESS"]
    vms = job["VM"]
    job_id = job["JOB_ID"]
    socks = []
    for vm in vms:
        vm_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        vm_sock.connect((HOSTS[vm - 1], RAINSTORM_PORT))
        vm_sock.sendall(RUN.encode('utf-8'))
        send_int(vm_sock, job_id)
        socks.append(vm_sock)
    while process.poll() is None:
        # print("Loop Vms")
        new_line = get_process_output(process, bytes=1024)
        decoded_line = new_line.decode("utf-8")
        key_val = decoded_line.split(',')
        output_id = vms[generate_sha1(key_val[0]) % len(vms)]
        socks[output_id].sendall(new_line)
    for sock in socks:
        sock.close()

def pipe_file(job):
    process = job["PROCESS"]
    output_file = job_metadata["OUTPUT"]
    with open(output_file, "wb") as output:
        while process.poll() is None:
            print("Loop File")
            new_line = get_process_output(process, bytes=1024)
            output.write(new_line)
            sleep(0.1)
    append(machine_id, output_file, output_file)


def handle_output(job_id):
    job = current_jobs.get(job_id)
    print(f"Handle Output: {job}")
    if ("VM" in job):
        print("PIPE")
        pipe_vms(job)
    elif ("OUTPUT" in job):
        print("FILE")
        pipe_file(job)

        
def get_process_output(process, bytes=1024):
    return process.stdout.readline()
def pipe_input(process, input: list):
    process.stdin.write()
    process.stdin.flush()

def run_job(client: socket.socket):
    print("a")
    job_id = receive_int(client)
    print("b")
    job = current_jobs.get(job_id)
    print("c")
    process = job["PROCESS"]
    print("HERE")
    writer = threading.Thread(target=handle_output, args=(job_id,))
    writer.daemon = True
    writer.start()

    while (1):
        data = client.recv(1024 * 1024)
        if (data == b''):
            break
        data = data.decode('utf-8')
        pipe_input(process, data)
    process.stdin.close()
        
def partition_file(leader_socket: socket.socket):
    # We should ignore unmerged data so only bring next stage vm id

    job_metadata = json.loads(leader_socket.recv(1024 * 1024))
    filename = job_metadata["FILE"]
    num_tasks = int(job_metadata["NUM_TASKS"])
    key = int(job_metadata["KEY"])
    vm_id = int(job_metadata["VM"])
    job_id = int(job_metadata["JOB_ID"])
    
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as next_stage:
        next_stage.settimeout(RECEIVE_TIMEOUT)
        next_stage.connect((HOSTS[vm_id - 1], RAINSTORM_PORT))
        print("Sending data")
        next_stage.sendall(RUN.encode('utf-8'))
        send_int(next_stage, job_id)

        print("Sending Done")

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
                    stream = f"{key}, {line}\n" #TODO: Maybe better serialization so that files with : or line with , keep working
                    next_stage.sendall(stream.encode('utf-8'))
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

def handle_client(client: socket.socket, ip_address):
    mode = client.recv(1).decode('utf-8')
    print(f"MODE: {mode}")
    print(f"MODE == RUN: {mode == RUN} | {RUN}")
    if (mode == READ):
        partition_file(client)
    elif (mode == EXECUTE):
        prepare_execution(client)
    elif (mode == RUN):
        print("HERE")
        run_job(client)

def start_server(machine_id: int):
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

    server.bind((HOSTS[machine_id - 1], RAINSTORM_PORT))
    server.listen(MAX_CLIENTS)
    
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
