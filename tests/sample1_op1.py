from src.Streaming.framework import rain_storm_framework
import sys
import re

pattern = sys.argv[1]
machine_id = sys.argv[2]

def filter_by_pattern(key, value): 
    
    if (re.search(pattern, value) is not None):
        return [(key, value)]

rain_storm_framework(machine_id, filter_by_pattern)