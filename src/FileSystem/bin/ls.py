import sys
import socket

from src.shared.shared import get_machines
from src.shared.constants import HOSTS
from src.FileSystem.shared import generate_sha1, get_receiver_id_from_file, id_from_ip, get_replica_ids
from src.FileSystem.constants import REPLICATION_FACTOR



file_name = sys.argv[1]
file_hash = generate_sha1(file_name)
print(get_replica_ids(file_hash))