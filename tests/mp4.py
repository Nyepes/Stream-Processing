from src.mp4.worker import encode_key_val
import sys

machine_id = int(sys.argv[1])

def user_defined_job(key, val):
    return key, val

for i in range(100):  # Read from stdin line by line
    new_key = f"{machine_id}:{i}" 
    # TODO: read from stdin
    result = user_defined_job("a", "1")
    print(encode_key_val(new_key, encode_key_val(*result)))
    

