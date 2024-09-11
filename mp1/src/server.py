from shared.constants import HOSTS, PORT, MAX_CLIENTS
import socket
import threading
import subprocess
from shared.shared import send_data, receive_data
import sys

machine_id = 1

def run_query(query: str):
    """
    Runs the arguments for the query passed in.
    Returns the string containing the result.
    """
    command = f"grep {query} machine.{machine_id}.log | sed 's/^/machine.{machine_id}.log: /'"
    # Runs the command given and captures output so that the result can be retrieved.
    result = subprocess.run(command, capture_output=True, text = True, shell=True)
    output_value = result.stdout.strip()
    return output_value

def handle_client(client_socket: socket.socket):
    """
    Given a socket connection to the client, this method will run the query given by the user
    to the machine's log file and return the result back to the client.
    """
    command = receive_data(client_socket)
    result = run_query(command)
    send_data(client_socket, result)
    client_socket.close()

def start_server():
    """
    Create a server listening on PORT on its address
    Constantly waiting on new connection, to execute handle client.
    """
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    # Lets server reuse address so that it can relaunch quickly
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind(("127.0.0.1", PORT))
    server.listen(MAX_CLIENTS)
    
    while True:
        client_socket, _ = server.accept()
        # Creates a new thread for each client
        client_handler = threading.Thread(target=handle_client, args=(client_socket,))
        # sets daemon to true so that there is no need of joining threads once thread finishes
        client_handler.daemon = True
        client_handler.start()

if __name__ == "__main__":
    machine_id = int(sys.argv[1])
    start_server()