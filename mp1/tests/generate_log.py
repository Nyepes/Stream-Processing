from datetime import datetime, timedelta
import random

FILENAME = "../src/test.log"
ENTRIES = 1000

# Should sum up to 1
FREQUENT_SPLIT = 0.60
MEDIUM_SPLIT = 0.30
RARE_SPLIT = 0.10

REGEX_PERCENT = 0.35

def generate_log_file():

    """

    Generates a synthetic log file with simulated HTTP request entries.

    This function creates a log file containing simulated HTTP requests from IP addresses with varying frequencies.
    It is designed to emulate a real-world server log for testing purposes, such as load testing or log parsing.

    The function performs the following steps:
    1. Defines frequent, medium, and rare IP addresses.
    2. Calculates the number of log entries for each IP address based on predefined proportions.
    3. Generates timestamps for each log entry, spread over the last 20 days.
    4. Generates log entries for each IP address using the `generate_entries` function.
    5. Shuffles all log entries to randomize their order.
    6. Writes the log entries to a file.

    """
    
    lines = []
    
    frequent_ip = "192.168.1.100"  # Frequent IP address
    medium_ip = "192.168.1.150"  # Medium frequency IP address
    rare_ip = "10.0.0.50"  # Rare frequency IP address
    
    http_methods = ['GET', 'POST', 'PUT', 'DELETE']
    paths = ['/home', '/about', '/contact', '/login', '/logout']

    # Calculate the number of times each IP should appear
    num_frequent_ip = int(ENTRIES * FREQUENT_SPLIT)
    num_medium_ip = int(ENTRIES * MEDIUM_SPLIT)
    num_rare_ip = int(ENTRIES * RARE_SPLIT)

    # Generate timestamps
    base_time = datetime.now()
    time_deltas = [timedelta(days=(i%20)) for i in range(ENTRIES)]
    timestamps = [base_time - delta for delta in time_deltas]

    # Add entries for frequent IP
    lines.extend(generate_entries(
        frequent_ip, num_frequent_ip, http_methods, 
        timestamps, paths)
    )

    # Add entries for medium frequency IP
    lines.extend(generate_entries(
        medium_ip, num_medium_ip, http_methods, 
        timestamps, paths)
    )

    # Add entries for rare frequency IP
    lines.extend(generate_entries(
        rare_ip, num_rare_ip, http_methods, 
        timestamps, paths)
    )

    random.shuffle(lines)

    with open(FILENAME, 'w') as f:
        for line in lines:
            f.write(f"{line}\n")

def generate_entries(ip: str, num_ip: int, http_methods: list[str], timestamps: list[datetime], paths: list[str]):

    """
    
    Generates a list of log entries for a given IP address.

    The function performs the following steps:
    1. For a specified proportion of requests (`REGEX_PERCENT`), generates URLs that match a regex-like pattern `/product/{id}`.
    2. For the remaining requests, picks random URLs from a predefined list of paths.
    3. For each log entry, it selects a random HTTP method, timestamp, and generates the log line.

    Parameters:
        ip (str): The IP address for which to generate log entries.
        num_ip (int): The number of log entries to generate for this IP address.
        http_methods (list[str]): A list of HTTP methods to randomly assign to the log entries.
        timestamps (list[datetime]): A list of timestamps to randomly assign to the log entries.
        paths (list[str]): A list of predefined URL paths to randomly assign to the log entries.

    Returns:
        list[str]: A list of generated log entries as strings.
    
    """
   
    entries = []
    
    for _ in range(int(num_ip * REGEX_PERCENT)):
        url = f"/product/{str(random.randint(1, 101))}"  # URL matching the regex pattern
        line = generate_log_entry(ip, random.choice(http_methods), url, random.choice(timestamps))
        entries.append(line)
    
    for _ in range(int(num_ip * (1 - REGEX_PERCENT))):
        url = random.choice(paths)
        line = generate_log_entry(ip, random.choice(http_methods), url, random.choice(timestamps))
        entries.append(line)
    
    return entries

def generate_log_entry(ip, method, url, timestamp):

    """

    This function creates a log entry with the following structure:
    `<IP address> - - [<timestamp>] "<HTTP method> <URL> HTTP/1.1" <status code> <response size>`

    Parameters:
        ip (str): The IP address of the client making the request.
        method (str): The HTTP method (e.g., 'GET', 'POST').
        url (str): The requested URL path (e.g., '/home', '/product/42').
        timestamp (datetime): The timestamp of the request.

    Returns:
        str: A formatted log entry as a string in the Common Log Format.
        
    """
    
    status_code = random.choice([200, 301, 404, 500])
    response_size = random.randint(500, 5000)

    timestamp_str = timestamp.strftime('%d/%b/%Y:%H:%M:%S %z')
    
    return f"{ip} - - [{timestamp_str}] \"{method} {url} HTTP/1.1\" {status_code} {response_size}"

if __name__ == "__main__":
    generate_log_file()
