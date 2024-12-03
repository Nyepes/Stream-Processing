import sys
import json
from time import sleep

machine_id = int(sys.argv[1])

def user_defined_job(key, val):
    return [(key, val)]

for i in range(100):  # Read from stdin line by line
    new_key = f"{machine_id}:{i}" 
    # TODO: read from stdin
    result = user_defined_job("a", "1")
    json.dump({"key": new_key, "value": result}, sys.stdout)
    print()
sleep(10000)
    # print(encode_key_val(new_key, encode_key_val(*result), in_bytes = False))