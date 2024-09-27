import socket
import sys
import threading
import random
import os
import signal
import json
from time import time, sleep

from src.shared.shared import send_data, receive_data, udp_receive_data, udp_send_data
from src.mp2.marshalling import current_member_list_packet, request_join_packet, decode_message, ack_packet
from src.mp2.constants import MEMBER_ID, TIMESTAMP, JOINED, FAILED, CURRENT_MEMBERS, TTL, PING_TIME, DATA, SUSPICION_ENABLED, PRINT_SUSPICION, LEAVING
from src.shared.constants import HOSTS, MAX_CLIENTS, RECEIVE_TIMEOUT, FAILURE_DETECTOR_PORT, INTRODUCER_ID, INTRODUCER_PORT
from src.shared.logging import log
from src.mp2.time_based_dict import TTLDict

machine_id = -1

member_list = []
suspicion_list = []

events = TTLDict()
clean_up = TTLDict()

suspicion_list_lock = threading.Lock()
member_list_lock = threading.Lock()
config_lock = threading.Lock()

member_condition_variable = threading.Condition()

configuration = {
    "suspicion_enabled": True, 
    "print_suspicion": False, 
    "leaving": False
}

def kill(time):
    """
    kills process after time seconds. 
    Useful for leaving gracefully
    """
    sleep(time)
    os.kill(os.getpid(), signal.SIGTERM)


def update_member_list_file():
    with open("src/member_list.txt", "w") as file:
        for member in member_list:
            file.write(f"{member[MEMBER_ID]}\n")

def poll_configuration():
    """
    Updates configuration with whatever is wirtten on the configuration file
    """
    with open("src/mp2/metadata.json", "r") as metadata:
        new_configuration = json.load(metadata)
    if (get_config(SUSPICION_ENABLED) != new_configuration[SUSPICION_ENABLED]):
        add_event(SUSPICION_ENABLED, new_configuration[SUSPICION_ENABLED])
    if new_configuration[LEAVING]:
        add_event(machine_id, LEAVING)
        # Set up timer to kill after all nodes received information
        # Kills after 10 seconds
        thread = threading.Thread(target=kill, args=(10,))
        thread.start()
    with config_lock:
        configuration = new_configuration

def get_config(key):
    """
    returns a value from the configuration file
    """
    with config_lock:
        val = configuration[key]
    return val

def set_config(key, val):
    """
    Sets a new value for the configuration and stores the file
    Useful to change before exiting and leaving
    Also useful to set the state of suspicion
    """
    with config_lock:
        configuration[key] = val
        with open("src/mp2/metadata.json", "w") as file:
            json_data = json.dump(configuration, file)


def add_event(id, event):
    """
    Adds event to the event list if it has not been recieved before
    We track what has been received through another TTLDict called clean_up set
    """
    if (clean_up.get(id) == event):
        clean_up.set(id, event, TTL)
    else:
        events.set(id, event, TTL)
        clean_up.set(id, event, TTL)

def get_random_member():
    """
    Returns a Member Class of a randomly connected Member
    """
    return random.choice(member_list)

def add_member_list(id):
    """
    Tried adding a member with given id to the member list
    If it already exists then nothing happens and False is returned
    If added was succesful True will be returned
    """

    global member_list
    with member_condition_variable:
        for member in member_list:
            if (member[MEMBER_ID] == id or machine_id == id): 
                #Member already in list or selfs
                return False

        # Adds member and records current timestamp
        member_list.append({MEMBER_ID: id, TIMESTAMP: time()})
        update_member_list_file()
        member_condition_variable.notify()
    return True

def remove_member_list(id):
    """
    Removes member with id from member_list.
    If it does not exist then nothing happens and false is returned
    If deleted is succesful, True is returned
    """
    global member_list
    with member_list_lock:
        for i, member in enumerate(member_list):
            if (member[MEMBER_ID] == id):
                del member_list[i]
                update_member_list_file()
                return True
    return False

def handle_failed(id):
    """
    Handles receiving a failed message
    Tries to delete, and if succesful spread the message
    """
    success = remove_member_list(id)
    if (success):
        add_event(id, FAILED)

def handle_joined(id):
    """
    Handles receiving a new member message
    Tries to add to member list, and if succesful spread the message
    """
    success = add_member_list(id)
    if (success):
        add_event(id, JOINED)

def change_sus_status(status):
    if (get_config[SUSPICION_ENABLED] != status):
        add_event(SUSPICION_ENABLED, status)
        set_config(SUSPICION_ENABLED, status)

def update_system_events(data):
    """
    Handles and updates members after another member acknowledges or pings machine
    """

    events = data[DATA]
    for id, state in events.items():
        if (id == SUSPICION_ENABLED):
            change_sus_status(status)
        if (state == FAILED or state == LEAVING):
            handle_failed(int(id))
        if (state == JOINED):
            handle_joined(int(id))
        
    log(f"Received ACK: {events}")

def handle_timeout(id):
    if (get_config[SUSPICION_ENABLED]):
        if (get_config[PRINT_SUSPICION]):
            print(f"Suspecting {id}")
        log("SUS")

        # TODO: Suspicion
    else:
        handle_failed(id)
        log(f"{id} {FAILED}")
    

def ping():
    """
    Selects random meber and pings to see if still alive
    At the same time sends data about recent events
    """


    while (1):
        # TODO: Condition variable
        with member_condition_variable:
            while (len(member_list) == 0):
                member_condition_variable.wait()
                poll_configuration()

        
        # Get Updates on configuration
        poll_configuration()

        member_id = get_random_member()[MEMBER_ID]

        machine_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

        ping = ack_packet(machine_id, events.get_all())
        udp_send_data(machine_socket, ping, (HOSTS[member_id - 1], FAILURE_DETECTOR_PORT))

        # Timeout if no response
        machine_socket.settimeout(PING_TIME)
        try:
            ack, address = udp_receive_data(machine_socket)
            update_system_events(ack)
            print(member_list)
    
        except (ConnectionRefusedError, socket.timeout):
            handle_timeout(member_id)
        # Ping Every PING_TIME seconds
        sleep(PING_TIME)

def failure_detector():
    """
    Listenes for pings from other machines
    """
    failure_detector = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    failure_detector.bind((HOSTS[machine_id - 1], FAILURE_DETECTOR_PORT))

    log(f"UDP server listening on {HOSTS[machine_id - 1]}:{FAILURE_DETECTOR_PORT}")

    # Waits and listens for pings from other machines
    # Responds with the current events
    while True:
        ping, address = udp_receive_data(failure_detector)

        log(f"Received message: {ping}")
        update_system_events(ping)

        ack = ack_packet(machine_id, events.get_all())
        udp_send_data(failure_detector, ack, address)


# ------------------------------------------
# --------------- INTRODUCER ---------------
# ------------------------------------------

def introduce_member(client_socket):
    """
    Introducer calls this method to add new member to System
    It adds new member to list and creates an event that the user joined
    """
    # Receives who is trying to join
    membership_requested = receive_data(client_socket)
    membership_data = decode_message(membership_requested)

    # Creates and sends a message containing the member list
    data = current_member_list_packet(member_list)
    send_data(client_socket, data)

    # Adds member to member_list and creates an event
    member = membership_data[MEMBER_ID]
    sleep(PING_TIME)
    add_member_list(member)
    add_event(member, JOINED)

    log(f"{member} {JOINED}")

def start_introducer_server():
    """
    Creates an introducer server with TCP so that
    clients can join by connecting.
    """

    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    
    # Reuse address
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    server.bind((HOSTS[machine_id - 1], INTRODUCER_PORT))
    server.listen(MAX_CLIENTS)
    while True:
        
        client_socket, address = server.accept()
        client_handler = threading.Thread(target=introduce_member, args=(client_socket, ))
        # Set daemon so that thread does not need to join
        client_handler.daemon = True
        client_handler.start()

def join():
    """
    Called by each new member to connect with introducer
    Fills out the membership list with whatever the introducer has.
    """
    try: 
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
            server.settimeout(RECEIVE_TIMEOUT)
            server.connect((HOSTS[INTRODUCER_ID - 1], INTRODUCER_PORT))

            # Sends request to join to introducer
            send_data(server, request_join_packet(machine_id))
    
            result = receive_data(server)
            decoded = decode_message(result)

            # Adds each member to its member list
            for member in decoded[CURRENT_MEMBERS]:
                add_member_list(member[MEMBER_ID])

            # Adds introducer as well
            add_member_list(INTRODUCER_ID)
        return result
    
    except (ConnectionRefusedError, socket.timeout):
        # If Introducer is down or too slow then timeout
        log("Introducer failed: Timeout", machine_id)
        return -1

if __name__ == "__main__":
    machine_id = int(sys.argv[1])
    if (machine_id == INTRODUCER_ID):
        introducer = threading.Thread(target=start_introducer_server)
        introducer.daemon = True
        introducer.start()
    else:
        join()
    set_config(LEAVING, False)
    poll_configuration()
    log(JOINED, machine_id)

    failure_listener = threading.Thread(target = failure_detector)
    failure_listener.daemon = True
    failure_listener.start()

    pinger = threading.Thread(target = ping)
    pinger.daemon = True
    pinger.start()

    

