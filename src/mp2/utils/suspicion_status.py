import json
from src.shared.constants import HOSTS
from src.mp2.constants import SUSPICION_ENABLED

with open("src/mp2/metadata.json", "r") as metadata:
    dict_data = json.load(metadata)
    print(f"Suspicion Status: {dict_data[SUSPICION_ENABLED]}")