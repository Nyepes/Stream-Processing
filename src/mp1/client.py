from src.shared.constants import HOSTS, LOGGER_PORT, MACHINE_SEPARATOR, RECEIVE_TIMEOUT
from src.shared.shared import send_data, receive_data
import threading
import socket
import sys

LINE_COUNT = False  # If True prints line count of each process and whole system; Otherwise, prints matching lines

total_line_count = 0
duck = threading.Lock()  # Modify total_line_count without race condition risk

def increase_total_line_count(amount: int):

    """
    
    This function acquires a lock to ensure thread-safe updating of the global variable `total_line_count`. 

    Parameters:
        amount (int): The amount to increment `total_line_count` by.

    Returns:
        None

    """
    
    global total_line_count
    
    duck.acquire()
    total_line_count += amount
    duck.release()

def query_host(host: str, arguments: str):
   
    """

    Sends arguments to host such that it runs a grep command on those arguments within its local filesystem.
    

    This function performs the following steps:
    1. Establishes a TCP connection to the specified host.
    2. Sends the provided `arguments` to the host to run grep locally.
    3. Receives output from host's local grep command. 

    Parameters:
        host (str): The hostname or IP address of the server to connect to.
        arguments (str): The data to send to the server.

    Returns:
        str: The response received from the server.
        int: Returns `-1` if the connection is refused or a timeout occurs.
    
    """
    
    try:
        
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
            server.settimeout(RECEIVE_TIMEOUT)
            server.connect((host, LOGGER_PORT))

            send_data(server, arguments)
            result = receive_data(server)

        return result
    
    except (ConnectionRefusedError, socket.timeout):
        
        return -1

def print_server_data(host, arguments):
    
    """
    
    Queries a host with given arguments and prints the server's response or an error message.

    This function performs the following steps:
    1. Calls `query_host` to send the specified `arguments` to the `host` and retrieve data.
    2. Checks if the connection was successful. If not, it prints a failure message for the host.
    3. If the connection is successful, it prints the data received from the server.

    Parameters:
        host (str): The host to perform grep command. 
        arguments (str): The arguments to send to the server.

    Returns:
        None
    
    """

    data = query_host(host, arguments)
    
    # Connection failed/refused
    if (data == -1):
        print(f"{host}: FAILED")
        return

    # Raw matching strings of machine i or machine.i.log: match count
    print(data) 
    
    # Calculate total line count for distributed grep
    if (LINE_COUNT):
        
        line_counts = data.split(MACHINE_SEPARATOR)
        
        # Unexpected error
        if (len(line_counts) != 2):
            return -1
        
        increase_total_line_count(int(line_counts[1]))

def query_all_hosts(arguments: str):

    global total_line_count
    
    """
    
    For each host open up a new thread that will query the srever and print the data
    Threads are created to increase parallization.
    """

    """
    
    For each host open up a new thread that will query the server and print the data of running grep.
    Threads are created to increase parallelization.

    This function performs the following steps:
    1. Iterates over a list of hosts.
    2. For each host, it creates a new thread.
    3. Starts all threads to run in parallel.
    4. Waits for all threads to complete their execution.
    5. If `LINE_COUNT` prints the total line count match of the distributed system.

    Parameters:
        arguments (str): The argument to run grep locally in each host.

    Returns:
        None

    """
    
    threads = [0] * len(HOSTS)
    for i, host in enumerate(HOSTS):
        threads[i] = threading.Thread(target=print_server_data, args=(host, arguments))
        threads[i].start()
    
    for thread in threads:
        thread.join()

    if (LINE_COUNT):
        print(f"TOTAL: {total_line_count}")

if __name__ == "__main__":
    
    """
    
    Application starting point.
    Parses arguments and runs distributed grep by sending 
    passed argument to each machine in the system.
    
    """
    
    arguments = ''
    
    # Skip the command name argument
    for string in sys.argv[1:]:
        
        # If argument has a space, put in quotations to not change meaning 
        # (e.g. grep Hello World != grep "Hello World")

        # -c flag allows us to print the accumulated line count of the distibuted system
        if (string == '-c'):
            
            LINE_COUNT = True
        
        # Handle special characters

        if (" " in string or ";" in string or "|" in string or "&" in string or ">" in string or "<" in string or "\\" or "$" in string):
            
            arguments += f'"{string}"'
        
        elif ("\"" in string):
            
            # Handle " inside the requested string
            args = string.split("\"")
            new_arg = ""
            
            for arg in args:
                
                new_arg += arg + "\\\""
            
            arguments += f'"{new_arg[:-2]}"'
        
        else:
            
            arguments += string
       
        arguments += ' '

    query_all_hosts(arguments)
