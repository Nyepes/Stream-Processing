import sys
import json
from time import sleep

machine_id = int(sys.argv[1])

def user_defined_job(key, val):
    return [(key, val)]

file =  open("output.txt", "w")
for line in sys.stdin:  # Read from stdin line by line
    file.write(line)
    file.flush()
    input_dict = decode_key_val(line)
    new_key = f"{machine_id}:{line}" # new_key

    value_dict = decode_key_val(input_dict["value"])
    result = user_defined_job(value_dict["key"], value_dict["value"]) # User defined function

    json.dump({"key": new_key, "value": result}, sys.stdout)
    sys.stdout.flush()

sleep(1000000)
