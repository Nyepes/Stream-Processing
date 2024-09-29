from src.shared.constants import HOSTS

"""
Script to run when calling ./run.sh list_mem
"""

with open("src/member_list.txt", "r") as member_list_file:

    for line in member_list_file:

        machine_id = int(line.strip())
        print(f"ID: {machine_id}\tADDRESS: {HOSTS[machine_id - 1]}")