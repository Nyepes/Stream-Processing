import json
from src.shared.constants import HOSTS

with open("src/mp2/metadata.json", "r") as metadata:
    dict_data = json.load(metadata)
    print(f"Suspicion Status: {dict_data['suspicion_enabled']}")