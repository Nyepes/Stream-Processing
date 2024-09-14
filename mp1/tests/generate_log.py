import random
from datetime import datetime, timedelta

def generate_log_file(filename, total_entries=1000):
    lines = []
    frequent_ip = '192.168.1.100'    # Frequent IP address
    infrequent_ip = '10.0.0.50'      # Infrequent IP address
    regex_pattern = '/product/\\d+'  # URLs matching this pattern
    other_ips = [f'172.16.0.{i}' for i in range(1, 255)]
    http_methods = ['GET', 'POST', 'PUT', 'DELETE']
    urls = ['/home', '/about', '/contact', '/login', '/logout']
    product_ids = [str(i) for i in range(1, 101)]  # For regex matching

    # Calculate the number of times each string should appear
    num_frequent_ip = int(total_entries * 0.6)   # At least 60%
    num_infrequent_ip = int(total_entries * 0.1) # At most 10%
    num_regex_entries = int(total_entries * 0.25) # Let's say regex matches 25% of the entries
    num_other_entries = total_entries - num_frequent_ip - num_infrequent_ip - num_regex_entries

    # Generate timestamps
    base_time = datetime.now()
    time_deltas = [timedelta(seconds=i) for i in range(total_entries)]
    timestamps = [base_time + delta for delta in time_deltas]

    # Add entries with the frequent IP address
    for _ in range(num_frequent_ip):
        line = generate_log_entry(frequent_ip, random.choice(http_methods), random.choice(urls), random.choice(timestamps))
        lines.append(line)

    # Add entries with the infrequent IP address
    for _ in range(num_infrequent_ip):
        line = generate_log_entry(infrequent_ip, random.choice(http_methods), random.choice(urls), random.choice(timestamps))
        lines.append(line)

    # Add entries that match the regex pattern
    for _ in range(num_regex_entries):
        url = f"/product/{random.choice(product_ids)}"  # URL matching the regex pattern
        ip = random.choice(other_ips)
        line = generate_log_entry(ip, random.choice(http_methods), url, random.choice(timestamps))
        lines.append(line)

    # Add other random entries
    for _ in range(num_other_entries):
        ip = random.choice(other_ips)
        url = random.choice(urls)
        line = generate_log_entry(ip, random.choice(http_methods), url, random.choice(timestamps))
        lines.append(line)

    # Shuffle the lines to randomize their order
    random.shuffle(lines)

    # Write the lines to the log file
    with open(filename, 'w') as f:
        for line in lines:
            f.write(f"{line}\n")

    return num_frequent_ip, num_infrequent_ip, num_regex_entries

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

def calculate_expected_matches(num_frequent_ip, num_infrequent_ip, num_regex_entries):
    expected_matches = {
        'frequent_ip': num_frequent_ip,
        'infrequent_ip': num_infrequent_ip,
        'regex_matches': num_regex_entries
    }
    return expected_matches

def write_answers(filename, expected_matches):
    with open(filename, 'w') as f:
        f.write(f"{expected_matches['frequent_ip']}\n")
        f.write(f"{expected_matches['infrequent_ip']}\n")
        f.write(f"{expected_matches['regex_matches']}\n")

def main():
    log_filename = 'web_access.log'
    answers_filename = 'answers.txt'
    total_entries = 1000

    num_frequent_ip, num_infrequent_ip, num_regex_entries = generate_log_file(log_filename, total_entries)
    expected_matches = calculate_expected_matches(num_frequent_ip, num_infrequent_ip, num_regex_entries)
    write_answers(answers_filename, expected_matches)

    print(f"Log file '{log_filename}' generated successfully.")
    print(f"Expected matches written to '{answers_filename}'.")

if __name__ == "__main__":
    main()
