from src.mp2.constants import LEAVING
import json

with open("src/mp2/metadata.json", "r") as metadata:
    dict_data = json.load(metadata)
dict_data[LEAVING] = True
with open("src/mp2/metadata.json", "w") as metadata:
    json.dump(dict_data, metadata)