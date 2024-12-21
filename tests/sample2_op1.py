from src.Streaming.framework import rain_storm_framework
import sys
import re

sign = sys.argv[1]
machine_id = sys.argv[2]

def filter_by_sign_type(key, value):
    
    row = value.split(",")
    SignType = row[6]
    
    if sign == SignType:
        return [(SignType, value)]

rain_storm_framework(machine_id, filter_by_sign_type)