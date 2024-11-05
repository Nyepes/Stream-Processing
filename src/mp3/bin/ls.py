import sys
import socket

from src.shared.shared import get_machines
from src.shared.constants import HOSTS
from src.mp3.shared import generate_sha1, get_receiver_id_from_file, id_from_ip
from src.mp3.constants import REPLICATION_FACTOR



file_name = sys.argv[1]
file_hash = generate_sha1(file_name)
machines = get_machines() + [id_from_ip(socket.gethostname())] # TODO: Fix others
machines.sort()


print(f"File {file_name} ({file_hash})")
for i in range(len(machines)):
    if (machines[i] >= (file_hash % 10) + 1):
        break
res = 0

for j in range(min(REPLICATION_FACTOR, len(machines))):
    machine = machines[(i+j)%len(machines)]
    print(f"{HOSTS[machine - 1]} \t id: {machine}")