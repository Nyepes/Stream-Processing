import json
from src.shared.constants import HOSTS

with open("src/mp2/metadata.json", "r") as metadata:
    dict_data = json.load(metadata)
    current_enabled = dict_data["print_suspicion"]

dict_data["print_suspicion"] = not current_enabled

with open("src/mp2/metadata.json", "w") as metadata:
    json.dump(dict_data, metadata)

if (not current_enabled):
    print("Print Suspicion on")
else:
    print("Print Suspicion off")