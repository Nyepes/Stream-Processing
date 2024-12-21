from time import time, sleep
import threading
import socket
import random
import signal
import json
import sys
import os

from src.FailureDetector.constants import MEMBER_ID, TIMESTAMP, JOINED, FAILED, CURRENT_MEMBERS, TTL, PING_TIME, DATA, SUSPICION_ENABLED, PRINT_SUSPICION, LEAVING, SUSPICION, NOT_SUS, INCARNATION
from src.shared.constants import HOSTS, MAX_CLIENTS, RECEIVE_TIMEOUT, FAILURE_DETECTOR_PORT, INTRODUCER_ID, INTRODUCER_PORT
from src.FailureDetector.marshalling import current_member_list_packet, request_join_packet, decode_message, ack_packet
from src.shared.shared import send_data, receive_data, udp_receive_data, udp_send_data
from src.FailureDetector.time_based_dict import TTLDict
from src.shared.logging import log

# Machine Specs

machine_id = -1 
incarnation = 0

member_list = []
suspicion_list = {}

configuration = {
    SUSPICION_ENABLED: False, 
    PRINT_SUSPICION: True, 
    LEAVING: False
}

events = TTLDict() # Data dissemination for TTL (Time-To-Leave) seconds
clean_up = TTLDict() # Helps us to avoid data dissemination infinitely

# Some specs aren't thread safe

suspicion_list_lock = threading.Lock()
member_list_lock = threading.Lock()
config_lock = threading.Lock()

# Condition variable to avoid busy waiting

member_condition_variable = threading.Condition()

def kill(time: int) -> None:
    
    """

    Kills machine after time seconds.

    Allows machine to intentionally leave (i.e. no failure/suspicion)

    """

    sleep(time)
    os.kill(os.getpid(), signal.SIGTERM)

def update_member_list_file() -> None:

    """
    
    Updates machine's member list .txt file.

    We update .txt file to run command ./run.sh list_mem to list machine's members

    """


    with open("src/member_list.txt", "w") as file:
        
        #  Although we are reading from member_list all function calls of 
        # update_member_list_file are called within a lock (i.e. it is thread safe)
        
        for member in member_list: 
            file.write(f"{member[MEMBER_ID]}\n")

def poll_configuration() -> None:
    
    global configuration 
    
    """
    
    Updates machine's configuration based on the metadata.json file.

    We can modify metadata.json through calling commands such as ./run.sh toggle_sus 
    
    """

    with open("src/FailureDetector/metadata.json", "r") as metadata:
        new_configuration = json.load(metadata)
    
    # Disseminate suspicion_enabled configuration to all machines 
    # in the network if a change is present

    if (get_config(SUSPICION_ENABLED) != new_configuration[SUSPICION_ENABLED]):
        
        add_event(SUSPICION_ENABLED, new_configuration[SUSPICION_ENABLED])
       
        with suspicion_list_lock:
            suspicion_list = {}

    # Disseminate that machine is intentionally LEAVING network
    
    if new_configuration[LEAVING]:
        
        add_event(machine_id, LEAVING)
        
        # Set up timer to kill after all nodes received information
        # Kills after 10 seconds

        thread = threading.Thread(target=kill, args=(10,))
        thread.start()

    # Update configuration file

    with config_lock:
        configuration = new_configuration
    
def get_config(key: str) -> bool:
    
    """
    
    API call to get the value for the key passed from machine's configuration.

    key must be in configuration; Otherwise fail.

    """

    assert(key in configuration)
    
    with config_lock:
        val = configuration[key]
    
    return val

def set_config(key: str, val: bool) -> None:

    global configuration
    
    """

    API call to update the passed key in the machine's configuration with the passed value.

    Updates metadata.json file with new configuration.

    """

    with config_lock:
        
        configuration[key] = val
        
        with open("src/FailureDetector/metadata.json", "w") as file:
            json_data = json.dump(configuration, file)

def add_event(id: int, event: str) -> None:
    
    """

    Adds event to the machine's event list if it has not been received before (useful for dissemination).
    
    We shouldn't disseminate event if we've already seen the event (useful to avoid infinite dissemination).
    We keep track of this through another event list of previously seen events in the machine. 

    Parameters imply that the event `event` was recieved from the machine `id` 

    """

    if (clean_up.get(id) == event): #  If already seen event, reset time. Makes sure that we don't disseminate message infinitely
        
        clean_up.set(id, event, 5 * TTL) #  Make the message last longer to ensure that we don't disseminate message again 
                                         #  (account for worst case - message gets back just after event is forgotten)
    
    else: # New event
        
        events.set(id, event, TTL)
        clean_up.set(id, event, 5 * TTL)

def get_random_member():
   
    """

    Returns random member from the machine's membership list.

    """

    with member_list_lock:
        
        if (len(member_list) == 0): #  Avoid failure 
            return None
        
        return random.choice(member_list)

def add_member_list(id: int, incarnation:int=0) -> bool:
    
    """
    
    Attempts to add a member with the given id to the member list. 
    
    Returns False if the member already exists or matches the machine's id, otherwise adds the member and returns True.
    
    """

    global member_list

    if (machine_id == id): return False
    
    with member_list_lock:
        
        with member_condition_variable:
            
            for member in member_list:
                
                if (member[MEMBER_ID] == id): 
                    
                    #Member already in list or selfs
                    return False

            # Adds member and records current timestamp
            member_list.append({MEMBER_ID: id, TIMESTAMP: time(), INCARNATION: incarnation})
            member_condition_variable.notify()
        
        update_member_list_file()
    
    return True

def remove_member_list(id: int) -> bool:
    
    """
    
    Removes the member with the given id from the member list. 
    
    Returns False if the member doesn't exist, otherwise removes it and returns True.
    
    """

    global member_list

    with member_list_lock:
        
        for i, member in enumerate(member_list):
            
            if (member[MEMBER_ID] == id):
                
                del member_list[i]
                update_member_list_file()
                return True

    return False

def handle_failed(id: int) -> None:
    
    """
    
    Handles a failed message by attempting to remove the member with the given id. 
    
    If the removal is successful, it spreads the failure message by adding a failure event.
    
    """


    success = remove_member_list(id)
    if (success):
        add_event(id, FAILED)

def handle_joined(id: int) -> None:
    
    """
    
    Handles a new member message by attempting to add the member with the given id to the member list. 
    
    If the addition is successful, it spreads the join event.
    
    """


    success = add_member_list(id)
    if (success):
        add_event(id, JOINED)

def change_sus_status(status: bool) -> None:

    """
    
    Changes the suspicion status if it differs from the current configuration. 

    Disseminates suspicion status as all nodes must have the same protocol (PingAck or PingAck + S)

    """
    
    if (get_config(SUSPICION_ENABLED) != status):
        
        add_event(SUSPICION_ENABLED, status)
        set_config(SUSPICION_ENABLED, status)

def remove_sus(id: int) -> None:

    """
    
    Removes the given id from the suspicion list if it exists. This occurs when the machine believes it 
    has failed as no message from the machine in suspicion has been recieved.
    
    The operation is performed within a lock to ensure thread safety.
    
    """

    
    with suspicion_list_lock:
        
        if id in suspicion_list:
        
            del suspicion_list[id]

def update_incarnation_number(id: int, incarnation_number: int) -> None:

    """
    
    Updates the incarnation number of the member with the given id if the new incarnation 
    number is higher than the current one, and removes the member from the suspicion list.
    
    """

    
    global member_list
    
    with member_list_lock:
        
        for i, member in enumerate(member_list):
        
            if member[MEMBER_ID] == id:
        
                if (member[INCARNATION] < incarnation_number):
        
                    member_list[i][INCARNATION] = incarnation_number
                    add_event(id, f"OK{incarnation_number}")
                    remove_sus(id)

def get_incarnation(id: int) -> int:

    """
    
    Returns the incarnation number of the member with the given id. 
    
    """


    global member_list
    
    with member_list_lock:
    
        for member in member_list:
    
            if (id == member[MEMBER_ID]):
    
                return member[INCARNATION]

def update_system_events(data: dict) -> None:
    
    """
    
    Handles and updates system events based on the ack response from another member. 
    It processes various event types (e.g., suspicion, failure, join) and updates member states accordingly.
    
    """

    events = data[DATA] 
    for id, state in events.items():
        
        if (id == SUSPICION_ENABLED):
            
            change_sus_status(state)
            continue
        
        if (state[:2] == "OK"):
            
            if (int(id) == machine_id): 
                continue
            
            update_incarnation_number(int(id), int(state[2:]))
        
        if (state == FAILED or state == LEAVING):
            handle_failed(int(id))
        
        if (state == JOINED):
            handle_joined(int(id))
        
        if (state[:len(SUSPICION)] == SUSPICION):
            handle_suspect(int(id), int(state[len(SUSPICION):]))
        
    log(f"Received ACK: {events}")

def handle_suspect(id: int, incarnation_number: int) -> None:

    """
    
    If the incarnation number of the machine id is higher than that currently stored in the machine, then a new event has happend.
    The newest suspicious is take into account and the message disseminated across the network.
    
    """

    if (get_config(SUSPICION_ENABLED) is False): 
        return
    
    global incarnation

    if (id == machine_id):
        
        if (incarnation_number == incarnation):
            incarnation += 1
        
        add_event(machine_id, f"OK{incarnation}")
        
        return

    current_incarnation_number = get_incarnation(id)
    
    #  If OK comes before SUSPICION
    if (current_incarnation_number is None): 
        return
    
    suspecting = False
    with suspicion_list_lock:
        
        if (id not in suspicion_list):
        
            if (incarnation_number >= current_incarnation_number):
        
                suspecting = True
                add_event(id, f"{SUSPICION}{incarnation_number}")
                suspicion_list[id] = (time(), incarnation_number)
        
                update_incarnation_number(id, incarnation_number)
    
    if (suspecting and get_config(PRINT_SUSPICION)):
        
        print(f"Suspecting {id} with incarnation: {incarnation_number}")

def handle_timeout(id: int) -> None:

    """
    
    Handles a timeout event for a member with the given id. 
    If suspicion is enabled, the member is marked as a suspect. 
    Otherwise, the member is marked as failed, and the appropriate event is logged.
    
    """


    if (get_config(SUSPICION_ENABLED)):

        handle_suspect(id, get_incarnation(id))
        log(f"{id} {SUSPICION}")

    else:

        handle_failed(id)
        log(f"{id} {FAILED}")
    
def reap_suspect_list():

    """
    Removes expired entries from the suspicion list based on their time-to-live (TTL). 
    If an entry has expired, it is removed and the member is marked as failed.
    """


    cur_time = time()
    with suspicion_list_lock:
        
        copy = suspicion_list.copy()
        
        for suspect, values in copy.items():
            
            if (values[0]  + TTL < cur_time):
                
                del suspicion_list[suspect]
                handle_failed(suspect)

def ping():
    
    """
    
    Periodically selects a random member and pings them to check if they are still alive, 
    while also sending recent event data. If the ping times out, the member is marked as suspect or failed. 
    The function continues to run in a loop, reaping expired entries from the suspicion list and handling configuration updates.
    
    """

    while (1):
        
        with member_condition_variable:
            
            while (len(member_list) == 0): # Handle supuracious wakeup
                
                member_condition_variable.wait()
                poll_configuration()

        
        # Get Updates on configuration
        poll_configuration()
        if (get_config(SUSPICION_ENABLED)):
            reap_suspect_list()
        
        random_member = get_random_member()

        if (random_member is None):
            continue

        member_id = random_member[MEMBER_ID]

        machine_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

        ping = ack_packet(machine_id, events.get_all())
        udp_send_data(machine_socket, ping, (HOSTS[member_id - 1], FAILURE_DETECTOR_PORT))

        # Timeout if no response
        machine_socket.settimeout(PING_TIME)
        try:
            ack, address = udp_receive_data(machine_socket)
            update_system_events(ack)
    
        except (ConnectionRefusedError, socket.timeout):
            handle_timeout(member_id)
        
        # Ping Every PING_TIME seconds
        sleep(PING_TIME)

def failure_detector():
    
    """
    Listens for pings from other machines using a UDP socket. 
    When a ping is received, it updates system events and responds with the current event data. 
    The function runs in a continuous loop to ensure constant listening and responding.
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
    with member_list_lock:
        member_list_copy = member_list.copy()
    data = current_member_list_packet(member_list_copy, get_config(SUSPICION_ENABLED))
    send_data(client_socket, data)

    # Adds member to member_list and creates an event
    member = membership_data[MEMBER_ID]
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
            configuration[SUSPICION_ENABLED] = decoded[SUSPICION_ENABLED]
            # Adds introducer as well

            add_member_list(INTRODUCER_ID)
        return result
    
    except (ConnectionRefusedError, socket.timeout):
        # If Introducer is down or too slow then timeout
        log("Introducer failed: Timeout", machine_id)
        return -1

if __name__ == "__main__":

    """
    Main function for initializing the failure detection system. 
    The machine id is passed as a command-line argument, and if the machine is an introducer, the introducer server is started. 
    he configuration is set, and both the failure detector listener and the pinger threads are launched. 
    If the machine is not an introducer, it attempts to join the network after a short delay.
    """

    
    machine_id = int(sys.argv[1])
    
    if (machine_id == INTRODUCER_ID):
        introducer = threading.Thread(target=start_introducer_server)
        introducer.daemon = True
        introducer.start()
    
    set_config(LEAVING, False)
    poll_configuration()
    log(JOINED, machine_id)

    failure_listener = threading.Thread(target = failure_detector)
    failure_listener.daemon = True
    failure_listener.start()
    
    pinger = threading.Thread(target = ping)
    pinger.daemon = True
    pinger.start()
    
    # Give time for listener to start
    
    sleep(4)
    if (machine_id != INTRODUCER_ID):
        join()
