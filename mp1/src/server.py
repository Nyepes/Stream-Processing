from shared.constants import HOSTS, PORT, MAX_CLIENTS, MACHINE_SEPARATOR
from shared.shared import send_data, receive_data
import subprocess
import threading
import socket
import sys

TEST = False  # If true run grep on test.log; Otherwise, machine.i.log

def run_query(machine_id: str, arguments: str):
    
    """

    Executes a grep command with `arguments` on the machine's log file (or test file) and returns the result as a string.

    This function constructs a `grep` command to search the log file of a specific machine for the given arguments.
    The result is prefixed with the machine's log file name followed by a separator for easier identification.
    The output is captured and returned as a string.

    Parameters:
        machine_id (str): The ID of the machine whose log file is being queried.
        arguments (str): The arguments string to search for in the log file.

    Returns:
        str: The output of the `grep` command, with each line prefixed by the machine log file name.

    """

    # grep result prefixed with "machine.i.log: "
    command = f"grep {arguments} {f'machine.{machine_id}.log' if not TEST else 'test.log'} | sed 's/^/machine.{machine_id}.log{MACHINE_SEPARATOR}/'"
    
    # Runs the command given and captures output so that the result can be retrieved.
    result = subprocess.run(command, capture_output=True, text = True, shell=True)
    output_value = result.stdout.strip()
    
    return output_value

def handle_client(client_socket: socket.socket, machine_id: str):
    
    """
    
    Handles a client connection by receiving a arguments, executing it on the machine's log file,
    and sending the result back to the client.

    This function performs the following steps:
    1. Receives the query command from the client through the socket connection.
    2. Executes the query on the machine's log file using the `run_query` function.
    3. Sends the query result back to the client.
    4. Closes the client socket after the data has been sent.

    Parameters:
        client_socket (socket.socket): The socket connection to the client.
        machine_id (str): The ID of the machine, used to identify which log file to query.

    """
    
    command = receive_data(client_socket)
    result = run_query(machine_id, command)
    send_data(client_socket, result)
    
    client_socket.close()

def start_server(machine_id: str):
    
    """
    
    Creates a server that listens on a specified port and handles client connections.
    It constantly waits for new connections and creates a new thread to handle each client connection.

    Parameters:
        machine_id (str): The ID of the machine.
    
    """
    
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    
    # Lets server reuse address so that it can relaunch quickly
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((HOSTS[machine_id - 1], PORT))
    server.listen(MAX_CLIENTS)
    
    while True:
        
        client_socket, _ = server.accept()
        
        # Creates a new thread for each client
        client_handler = threading.Thread(target=handle_client, args=(client_socket, machine_id,))
        
        # sets daemon to true so that there is no need of joining threads once thread finishes
        client_handler.daemon = True
        client_handler.start()

if __name__ == "__main__":
    
    machine_id = int(sys.argv[1])
    
    # -t flag to use test.log rather than machine.i.log
    if (len(sys.argv) == 3 and sys.argv[2] == '-t'):
        TEST = True
    
    start_server(machine_id)
