import json
from src.shared.constants import HOSTS

with open("src/mp2/metadata.json", "r") as metadata:
    data = metadata.read()
    dict_data = json.loads(data)
    print(dict_data["suspicion_enabled"])