import sys

from src.mp3.shared import request_file, id_from_ip

vm_address = sys.argv[1]
hydfs_filename = sys.argv[2]
local_filename = sys.argv[3]
machine_id = id_from_ip(vm_address)
request_file(machine_id, hydfs_filename, local_filename)
print("Done")