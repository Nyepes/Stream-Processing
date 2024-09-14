from shared.constants import HOSTS, PORT, MACHINE_SEPARATOR
import socket
import sys
from shared.shared import send_data, receive_data
import threading
import queue
from time import sleep, time

LINE_COUNT = False
total_line_count = 0
duck = threading.Lock()
RECEIVE_TIMEOUT = 5

def increase_total_line_count(amount: int):
    global total_line_count
    duck.acquire()
    total_line_count += amount
    duck.release()

def query_host(host: str, arguments: str):
    """
    Opens up a connection with server to send the command passed in to arguments
    and returns the result.
    """
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
            server.settimeout(RECEIVE_TIMEOUT)
            result = ""
            server.connect((host, PORT))

            start_time = time()
            send_data(server, arguments)
            result = receive_data(server)
            end_time = time()

            print(f"{host}: {end_time - start_time} seconds")

        return result
    except (ConnectionRefusedError, socket.timeout):
        return -1

def print_server_data(host, arguments):
    """
    Query the result from the server and print it
    """
    data = query_host(host, arguments)
    if (data == -1):
        print(f"{host}: FAILED")
        return
    print(data)
    if (LINE_COUNT):
        line_counts = data.split(MACHINE_SEPARATOR)
        if (len(line_counts) != 2):
            return -1
        increase_total_line_count(int(line_counts[1]))


def query_all_hosts(arguments: str):
    """
    For each host open up a new thread that will query the srever and print the data
    Threads are created to increase parallization.
    """
    result_queue = queue.Queue()
    threads = [0] * len(HOSTS)
    for i, host in enumerate(HOSTS):
        threads[i] = threading.Thread(target=print_server_data, args=(host, arguments))
        threads[i].start()
    for thread in threads:
        thread.join()
    if (LINE_COUNT):
        global total_line_count
        print(f"TOTAL: {total_line_count}")


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
        if (string == '-c'):
            LINE_COUNT = True
        if (" " in string or ';' in string or '|' in string or '&' in string or ">" in string or "<" in string):
            arguments += f'"{string}"'
        elif ("\"" in string):
            args = string.split("\"")
            new_arg = ""
            for arg in args:
                new_arg += arg + "\\\""
            arguments += f'"{new_arg[:-2]}"'
        else:
            arguments += string
        arguments += ' '
    query_all_hosts(arguments)