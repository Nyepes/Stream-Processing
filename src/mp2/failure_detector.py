from socket import socket
from src.shared.shared import send_data
from src.mp2.marshalling import create_member_list

member_list = []
def add_member_list(id):
    return
def remove_member_list(id):
    return



def join_member(client_socket):
    membership_requested = receive_data(client_socket)
    data = create_join_message(member_list)
    send_data(client_socket, data)
    add_member_list(id)

def introducer_server(machine_id, PORT):
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    
    # Lets server reuse address so that it can relaunch quickly
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((HOSTS[machine_id - 1], PORT))
    server.listen(MAX_CLIENTS)
    while True:
        
        client_socket, _ = server.accept()
        
        # Creates a new thread for each client
        client_handler = threading.Thread(target=join_member, args=(client_socket, ))
        
        # sets daemon to true so that there is no need of joining threads once thread finishes
        client_handler.daemon = True
        client_handler.start()