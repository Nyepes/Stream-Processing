import hashlib 

from src.shared.constants import HOSTS, FILE_SYSTEM_PORT
from src.shared.shared import get_machines

def generate_sha1(input_string):
    """Generate a SHA-1 hash of the input string."""
    sha1_hash = hashlib.sha1()
    sha1_hash.update(input_string.encode('utf-8'))
    return sha1_hash.hexdigest()


def read_file(file_name, pipe = None):
    with open(file_name, 'r') as file:
        for line in file:
            if pipe is None:
                print(line.strip())
            else: 
                pipe.write(line)


BUFFER_SIZE = 1024
def request_file(machine_id, file_name, output_file):
    try:    
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
            server.settimeout(RECEIVE_TIMEOUT)
            server.connect((host, FILE_SYSTEM_PORT))
            send_data(server, "GET " + file_name)
            with open(output_file, "wb") as output:
                while (1):
                    data = server.recv(BYTE_STREAM)
                    if (data == b''): return
                    output.write()    
    except (ConnectionRefusedError, socket.timeout):
        return -1
    except (OSError):
        return -2

def send_file(receiver_socket):
    with open(file_name, 'rb') as file:
        while True:
            chunk = file.read(BUFFER_SIZE)        
            if not chunk:
                break
                    
            s.sendall(chunk)
            print(f"File {file_name} sent successfully.")
    except FileNotFoundError:
        print(f"File {file_name} not found.")
    except (ConnectionRefusedError, socket.timeout) as e:
        print(f"Error: {e}")
    except OSError as e:
        print(f"OS error: {e}")




