from src.shared.constants import HOSTS
from src.shared.shared import get_machines
"""
Script to run when calling ./run.sh list_mem
"""

machines = get_machines()
machines.sort()
for machine_id in machines:
    print(f"ID: {machine_id}\tADDRESS: {HOSTS[machine_id - 1]}")