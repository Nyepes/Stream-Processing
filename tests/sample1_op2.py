from src.Streaming.framework import rain_storm_framework
import sys
import re

machine_id = sys.argv[1]

def select_columns(key, value):
    
    row = value.split(",")
    
    OBJECTID = row[2]
    SignType = row[3]
    
    return [(OBJECTID, SignType)]

rain_storm_framework(machine_id, select_columns)