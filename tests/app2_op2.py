from src.mp4.framework import rain_storm_framework
from src.mp4.framework import decode_key_val
import sys

machine_id = sys.argv[1]
counts = dict()

def count_by_category(key, value):

    if (key != "STATE"):

        row = value.split(",")
        category = row[8]
        counts[category] = counts.get(category, 0) + 1
        return [(category, counts[category])]

rain_storm_framework(machine_id, count_by_category)