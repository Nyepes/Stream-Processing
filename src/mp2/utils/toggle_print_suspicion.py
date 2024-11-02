import json

from src.mp2.constants import PRINT_SUSPICION
from src.shared.constants import HOSTS

"""
Script to run when calling ./run.sh toggle_print_sus
"""

with open("src/mp2/metadata.json", "r") as metadata:
    dict_data = json.load(metadata)
    current_enabled = dict_data[PRINT_SUSPICION]

dict_data[PRINT_SUSPICION] = not current_enabled

with open("src/mp2/metadata.json", "w") as metadata:
    json.dump(dict_data, metadata)

if (not current_enabled):
    print("Print Suspicion on")
else:
    print("Print Suspicion off")