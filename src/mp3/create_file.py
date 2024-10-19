import socket

from src.shared.constants import HOSTS, FILE_SYSTEM_PORT
from src.shared.shared import get_machines

BUFFER_SIZE = 1024

def request_create_file(machine_to_request, file_name):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((HOSTS[machine_to_request - 1], FILE_SYSTEM_PORT))
        s.sendall(f"CREATE {file_name}")
        result = s.recv(BUFFER_SIZE) # TODO: Ensure no hang
        if (result == b'ALREADY EXISTS'):
            return -1
    except ConnectionRefusedError:
        print(f"Connection to {host}:{port} refused.")
    except socket.timeout:
        print("Connection timed out.")
    except OSError as e:
        print(f"Socket error: {e}")

    

if __name__ == "__main__":
    
    file_name = int(sys.argv[1])
    machines = get_machines()
    value = generate_sha1(file_name)
    machine_with_file = value % 10 + 1

    id = os.environ["ID"]

    if (id < machine_with_file + REPLICATION_FACTOR and id >= machine_with_file):
        res = create_local_file()
        if (res < 0):
            print("Failed Creating File")
        else:
            print("File Created")
    else:
        res = request_create_file(machine_with_file + id % REPLICATION_FACTOR, file_name)
        if (res < 0):
            print("Failed Creating File")
        else:
            print("File Created")


