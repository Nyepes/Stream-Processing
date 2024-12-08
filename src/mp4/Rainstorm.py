import sys
import socket
import json

from src.shared.constants import HOSTS, RECEIVE_TIMEOUT, INTRODUCER_ID, LEADER_PORT


op1_exe = sys.argv[1]
op2_exe = sys.argv[2]
input_hydfs = sys.argv[3]
output_hydfs = sys.argv[4]
num_tasks = sys.argv[5]
is_stateful = sys.argv[6] # 0 - False, 1 - True

job_data = {}
job_data["OP_1_PATH"] = op1_exe
job_data["OP_2_PATH"] = op2_exe
job_data["INPUT_FILE"] = input_hydfs
job_data["OUTPUT_FILE"] = output_hydfs
job_data["NUM_TASKS"] = num_tasks
job_data["STATEFUL"] = is_stateful == "1"

json_data = json.dumps(job_data)

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as leader:
    leader.settimeout(RECEIVE_TIMEOUT)
    leader.connect((HOSTS[INTRODUCER_ID - 1], LEADER_PORT))
    leader.sendall(b"S")
    leader.sendall(json_data.encode('utf-8'))




