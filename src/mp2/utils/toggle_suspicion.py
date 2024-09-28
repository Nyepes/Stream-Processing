import json
from src.shared.constants import HOSTS
from src.mp2.constants import SUSPICION_ENABLED

with open("src/mp2/metadata.json", "r") as member_list_file:
    dict_data = json.load(member_list_file)
    current_enabled = dict_data[SUSPICION_ENABLED]

dict_data[SUSPICION_ENABLED] = not current_enabled

with open("src/mp2/metadata.json", "w") as member_list_file:
    json.dump(dict_data, member_list_file)

if (not current_enabled):
    print("Suspicion on")
else:
    print("Suspicion off")