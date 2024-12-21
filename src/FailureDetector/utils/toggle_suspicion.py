import json

from src.FailureDetector.constants import SUSPICION_ENABLED
from src.shared.constants import HOSTS

"""
Script to run when calling ./run.sh toggle_sus
"""

with open("src/FailureDetector/metadata.json", "r") as member_list_file:
    dict_data = json.load(member_list_file)
    current_enabled = dict_data[SUSPICION_ENABLED]

dict_data[SUSPICION_ENABLED] = not current_enabled

with open("src/FailureDetector/metadata.json", "w") as member_list_file:
    json.dump(dict_data, member_list_file)

if (not current_enabled):
    print("Suspicion on")
else:
    print("Suspicion off")