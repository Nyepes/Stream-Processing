from src.mp4.framework import rain_storm_framework
from src.mp4.framework import decode_key_val
import sys

machine_id = sys.argv[1]
counts = defaultdict(int)



def count_by_category(key, value):

    if (key == "STATE"):
        counts = decode_key_val(value)
    else:
        row = value.split(",")
        category = row[3]
        counts[category] += 1
        return [(category, counts[category])]

rain_storm_framework(machine_id, count_vals)