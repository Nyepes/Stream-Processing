from shared.constants import HOSTS, PORT
import socket
import sys
from shared.shared import send_data, receive_data
import threading

def query_host(host: str, arguments: str):
    """
    Opens up a connection with server to send the command passed in to arguments
    and returns the result.
    """
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
        result = ""
        server.connect((host, PORT))
        send_data(server, arguments)
        result = receive_data(server)
    return result

def print_server_data(host, arguments):
    """
    Query the result from the server and print it
    """
    data = query_host(host, arguments)
    print(data)

def query_all_hosts(arguments: str):
    """
    For each host open up a new thread that will query the srever and print the data
    Threads are created to increase parallization.
    """
    threads = [0] * len(HOSTS)
    for i, host in enumerate(HOSTS):
        threads[i] = threading.Thread(target=print_server_data, args=(host, arguments))
        threads[i].start()
        
    for thread in threads:
        thread.join()



if __name__ == "__main__":
    """
    Application starting point.
    Parses arguments and runs command
    """
    arguments = ''
    # Skip the command name argument
    for string in sys.argv[1:]:
        # If argument has a space, put in quotations to not change meaning 
        # (e.g. grep Hello World != grep "Hello World")
        if (" " in string or ';' in string or '|' in string or '&' in string or ">" in string or "<" in string):
            arguments += f'"{string}"'
        else:
            arguments += string
        arguments += ' '
    query_all_hosts(arguments)