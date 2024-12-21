from src.Streaming.framework import rain_storm_framework
import sys
import re

machine_id = sys.argv[2]
pattern = sys.argv[1]

def filter_pattern(key, value): # <filename:linenumber - line>
    if (re.search(pattern, value) is not None):
        line_vals = value.split(",")
        OBJECTID = line_vals[2]
        SignType = line_vals[3]
        return [(line_vals[2], SignType)]

rain_storm_framework(machine_id, filter_pattern)