import random
from datetime import datetime, timedelta
import re

def generate_log_file(filename, total_entries=1000):
    lines = []
    frequent_ip = '192.168.1.100'    # Frequent IP address
    medium_ip = '192.168.1.150'      # Medium frequency IP address
    rare_ip = '10.0.0.50'      # Infrequent IP address
    regex_pattern = r'/product/\d+'  # Regex pattern to match
    other_ips = [f'172.16.0.{i}' for i in range(1, 255)]
    http_methods = ['GET', 'POST', 'PUT', 'DELETE']
    urls = ['/home', '/about', '/contact', '/login', '/logout']
    product_ids = [str(i) for i in range(1, 101)]  # For regex matching

    # Calculate the number of times each IP should appear
    num_frequent_ip = int(total_entries * 0.6)   # 60% of total entries
    num_medium_ip = int(total_entries * 0.3)     # 30% of total entries
    num_rare_ip = int(total_entries * 0.1) # 10% of total entries
    total_ips_entries = num_frequent_ip + num_medium_ip + num_rare_ip

    # Ensure total entries sum up correctly
    if total_ips_entries != total_entries:
        num_rare_ip = total_entries - (num_frequent_ip + num_medium_ip)

    # Calculate the number of regex matches (35% of total entries)
    num_regex_entries = int(total_entries * 0.35)  # 35% regex matches

    # Distribute regex matches among IP addresses proportionally
    num_frequent_regex = int(num_frequent_ip * 0.35)
    num_medium_regex = int(num_medium_ip * 0.35)
    num_infrequent_regex = num_regex_entries - (num_frequent_regex + num_medium_regex)

    # Ensure the counts are correct
    num_frequent_non_regex = num_frequent_ip - num_frequent_regex
    num_medium_non_regex = num_medium_ip - num_medium_regex
    num_infrequent_non_regex = num_rare_ip - num_infrequent_regex

    # Generate timestamps
    base_time = datetime.now()
    time_deltas = [timedelta(seconds=i) for i in range(total_entries)]
    timestamps = [base_time + delta for delta in time_deltas]

    # Add entries for frequent IP
    lines.extend(generate_entries(
        frequent_ip, num_frequent_regex, num_frequent_non_regex,
        http_methods, timestamps, product_ids, urls
    ))

    # Add entries for medium frequency IP
    lines.extend(generate_entries(
        medium_ip, num_medium_regex, num_medium_non_regex,
        http_methods, timestamps, product_ids, urls
    ))

    # Add entries for infrequent IP
    lines.extend(generate_entries(
        rare_ip, num_infrequent_regex, num_infrequent_non_regex,
        http_methods, timestamps, product_ids, urls
    ))

    # Shuffle the lines to randomize their order
    random.shuffle(lines)

    # Write the lines to the log file
    with open(filename, 'w') as f:
        for line in lines:
            f.write(f"{line}\n")

def generate_entries(ip, num_regex, num_non_regex, http_methods, timestamps, product_ids, urls):
    entries = []
    for _ in range(num_regex):
        url = f"/product/{random.choice(product_ids)}"  # URL matching the regex pattern
        line = generate_log_entry(ip, random.choice(http_methods), url, random.choice(timestamps))
        entries.append(line)
    for _ in range(num_non_regex):
        url = random.choice(urls)
        line = generate_log_entry(ip, random.choice(http_methods), url, random.choice(timestamps))
        entries.append(line)
    return entries

def generate_log_entry(ip, method, url, timestamp):
    log_format = '{ip} - - [{timestamp}] "{method} {url} HTTP/1.1" {status_code} {response_size}'
    status_code = random.choice([200, 301, 404, 500])
    response_size = random.randint(500, 5000)
    timestamp_str = timestamp.strftime('%d/%b/%Y:%H:%M:%S %z')
    return log_format.format(
        ip=ip,
        timestamp=timestamp_str,
        method=method,
        url=url,
        status_code=status_code,
        response_size=response_size
    )

def main():
    
    log_filename = '../src/test.log'
    total_entries = 1000

    generate_log_file(log_filename, total_entries)

if __name__ == "__main__":
    main()
