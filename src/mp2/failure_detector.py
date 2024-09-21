import socket
import sys
import threading
import random
from time import time, sleep

from src.shared.shared import send_data, receive_data, udp_receive_data, udp_send_data
from src.mp2.marshalling import create_member_list, create_join_message, decode_message, MessageType, create_ack_message
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
    global member_list
    member_list_lock.acquire()
    for member in member_list:
        if (member["id"] == id): 
            member_list_lock.release()
            return
    member_list.append({"id": id, "timestamp": time()})

    member_list_lock.release()

def remove_member_list(id):
    global member_list
    member_list_lock.acquire()
    for i, member in enumerate(member_list):
        if (member["id"] == id):
            del member_list[i]
            member_list_lock.release()
            return
    member_list_lock.release()

def join_member(client_socket):
    membership_requested = receive_data(client_socket)
    membership_data = decode_message(membership_requested)

    data = create_member_list(member_list)
    send_data(client_socket, data)

    member = membership_data["id"]
    add_member_list(member)
    events.set(member, "joined", TTL)

    log(f"{member} joined")

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
            for member in decoded["members"]:
                if member["id"] != machine_id:
                    add_member_list(member["id"])

            add_member_list(INTRODUCER_ID)
        return result
    
    except (ConnectionRefusedError, socket.timeout):
        return -1

def handle_failed(id):
    remove_member_list(id)
    events.set(id, "fail", 5)

def handle_client_ack(data):
    ### TODO: Implement what happens when data is received and how the ack is handled
    ### This means updating the current members and the list of events
    for id, val in data.items():
        if (val == "failed"):
            handle_failed(id)
    log(data)
    return

def handle_timeout(id):
    if (False):
        log("SUS")

        # TODO: Suspicion
    else:
        handle_failed(id)
        log(f"{id} FAILED")
        # TODO: Fail
    

def ping():
    while (1):
        print(events.get_all())
        print(member_list)
        if (len(member_list) == 0):
            sleep(1)
            print("a")
            continue
        member_id = get_random_member()["id"]
        print(member_id)

        machine_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        packet = create_ack_message(machine_id, events.get_all())
        udp_send_data(machine_socket, packet, (HOSTS[member_id - 1], FAILURE_DETECTOR_PORT))
        machine_socket.settimeout(0.5)
        try:
            data, address = udp_receive_data(machine_socket)
            handle_client_ack(data)
    
        except (ConnectionRefusedError, socket.timeout):
            print("timeout")
            handle_timeout(member_id)
        print("hasdlhfa")
        sleep(1)

def ack(data):
    log(data)
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
        ack(data)

        packet = create_ack_message(machine_id, events.get_all())

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
    failure_listener.start()

    print("Pinger")
    pinger = threading.Thread(target = ping)
    pinger.daemon = True
    pinger.start()

    

