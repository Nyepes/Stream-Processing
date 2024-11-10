import sys
import socket

from src.shared.shared import get_machines
from src.shared.constants import HOSTS
from src.mp3.shared import generate_sha1, get_receiver_id_from_file, id_from_ip
from src.mp3.constants import REPLICATION_FACTOR



file_name = sys.argv[1]
file_hash = generate_sha1(file_name)
get_replica_ids(file_hash)