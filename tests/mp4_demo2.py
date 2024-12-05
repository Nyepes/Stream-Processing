from src.mp4.framework import rain_storm_framework
import sys
from collections import defaultdict

machine_id = sys.argv[1]
counts = defaultdict(int)

def count_vals(key, value):
    counts[key] += 1
    return [(key, counts[key])]

rain_storm_framework(machine_id, count_vals)