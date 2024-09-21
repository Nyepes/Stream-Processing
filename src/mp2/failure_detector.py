import socket
import sys
import threading
import random

from src.shared.shared import send_data, receive_data, udp_receive_data, udp_send_data
from src.mp2.marshalling import create_member_list, create_join_message, decode_message, MessageType
from src.mp2.constants import FAILURE_DETECTOR_PORT, INTRODUCER_ID, INTRODUCER_PORT
from src.shared.constants import HOSTS, MAX_CLIENTS, RECEIVE_TIMEOUT
from src.shared.logging import log
from src.mp2.time_based_dict import TTLDict

machine_id = -1

member_list = []
# suspicion_list = []

events = TTLDict()

suspicion_list_lock = threading.Lock()
member_list_lock = threading.Lock()

TTL = 5

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
    member = Member(machine_id = membership_data["id"], time_stamp = membership_data["timestamp"])
    add_member_list(member)
    events.set(machine_id, "machine_id", TTL)

def introducer_server():
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    
    # Lets server reuse address so that it can relaunch quickly
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((HOSTS[machine_id - 1], INTRODUCER_PORT))
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
            server.connect((HOSTS[INTRODUCER_ID - 1], INTRODUCER_PORT))
            send_data(server, create_join_message(machine_id))
            result = receive_data(server)
            decoded = decode_message(result)
            member_list = decoded["members"]
            member_list.append(INTRODUCER_ID)
        return result
    
    except (ConnectionRefusedError, socket.timeout):
        return -1

def handle_client_ack(data):
    ### TODO: Implement what happens when data is received and how the ack is handled
    ### This means updating the current members and the list of events
    return

def handle_timeout():
    log("Timed Out")

def ping():

    while (1):
        member_id = get_random_member()
        socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        udp_send_data(failure_detector, create_ack_message(events.get_all()), member_id)
        socket.settimeout(0.5)
        try:
            data, address = udp_receive_data(socket)
            handle_client_ack(data)
    
        except (ConnectionRefusedError, socket.timeout):
            handle_timeout()
        return -1


        sleep(1)

def ack():
    return 

def failure_detector():
    """
    Listenes for pings from other machines
    """
    failure_detector = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    failure_detector.bind((HOSTS[machine_id - 1], FAILURE_DETECTOR_PORT))

    log(f"UDP server listening on {HOSTS[machine_id - 1]}:{FAILURE_DETECTOR_PORT}")

    while True:
        # Receive data from a client
        data, address = udp_receive_data(failure_detector)
        log(f"Received message: {data}")
        handle_client_ack(data)

        packet = create_ack_message(events.get_all())

        udp_send_data(failure_detector, packet, address)

if __name__ == "__main__":
    machine_id = int(sys.argv[1])
    
    if (machine_id == INTRODUCER_ID):
        introducer = threading.Thread(target=introducer_server)
        introducer.daemon = True
        introducer.start()
    else:
        join()
    log("Joined", machine_id)

    failure_listener = threading.Thread(target = failure_detector)
    failure_listener.daemon = True

    pinger = threading.Thread(target = ping)
    pinger.daemon = True

    

