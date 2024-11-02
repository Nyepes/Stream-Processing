import json

from src.mp2.constants import SUSPICION_ENABLED
from src.shared.constants import HOSTS

"""
Script to run when calling ./run.sh sus_status
"""

with open("src/mp2/metadata.json", "r") as metadata:
    
    dict_data = json.load(metadata)
    print(f"Suspicion Status: {dict_data[SUSPICION_ENABLED]}")