import socket
import sys
import threading
import random

from src.shared.shared import send_data, receive_data, udp_receive_data, udp_send_data
from src.mp2.marshalling import create_member_list, create_join_message, decode_message, MessageType
from src.mp2.constants import FAILURE_DETECTOR_PORT, INTRODUCER_ID
from src.shared.constants import HOSTS, MAX_CLIENTS, RECEIVE_TIMEOUT
from src.shared.logging import log

machine_id = -1

member_list = []
suspicion_list = []

suspicion_list_lock = threading.Lock()
member_list_lock = threading.Lock()

def get_random_member():
    return random.choice(member_list)

def add_member_list(id):
    member_list_lock.acquire()
    member_list.append(id)
    member_list_lock.release()

def remove_member_list(id):
    member_list_lock.acquire()
    del member_list[id]
    member_list_lock.release()

def join_member(client_socket):
    membership_requested = receive_data(client_socket)
    membership_data = decode_message(membership_requested)
    data = create_member_list(member_list)
    send_data(client_socket, data)
    add_member_list(membership_data["id"])

def introducer_server():
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    
    # Lets server reuse address so that it can relaunch quickly
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((HOSTS[machine_id - 1], FAILURE_DETECTOR_PORT))
    server.listen(MAX_CLIENTS)
    while True:
        
        client_socket, _ = server.accept()
        
        # Creates a new thread for each client
        client_handler = threading.Thread(target=join_member, args=(client_socket, ))
        
        # sets daemon to true so that there is no need of joining threads once thread finishes
        client_handler.daemon = True
        client_handler.start()

def join():
    try: 
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
            server.settimeout(RECEIVE_TIMEOUT)
            server.connect((HOSTS[INTRODUCER_ID - 1], FAILURE_DETECTOR_PORT))
            send_data(server, create_join_message(machine_id))
            result = receive_data(server)
            decoded = decode_message(result)
            member_list = decoded["members"]
            member_list.append(INTRODUCER_ID)
        return result
    
    except (ConnectionRefusedError, socket.timeout):
        return -1

def failure_detector():
    failure_detector = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    failure_detector.bind((HOSTS[machine_id - 1], FAILURE_DETECTOR_PORT))

    log(f"UDP server listening on {HOSTS[machine_id - 1]}:{FAILURE_DETECTOR_PORT}")

    while True:
        # Receive data from a client
        data = udp_receive_data(failure_detector)
        log(f"Received message: {data}")
        # udp_server_socket.sendto(response_message.encode('utf-8'), client_address)

if __name__ == "__main__":
    machine_id = int(sys.argv[1])

    introducer = threading.Thread(target=introducer_server)
    introducer.daemon = True
    introducer.start()
    join()
    log("Joined", machine_id)
    failure_detector()
    

