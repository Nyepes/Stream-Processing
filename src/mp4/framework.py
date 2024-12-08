import sys
import json
from time import sleep
from src.mp4.worker import decode_key_val

# def user_defined_job(key, val):
#     return [(key, val)]


def rain_storm_framework(machine_id, user_defined_job, init=None):
    try:
        for line in sys.stdin:  # Read from stdin line by line
            print("INPUT: ", line, file=sys.stderr)
            if (line == "DONE"):
                exit(1)
            input_dict = decode_key_val(line)
            if (input_dict["value"] == "null" or input_dict["value"] == None):
                continue
            if (init is not None and input_dict["key"] == "STATE" and input_dict["value"] != None):
                init(decode_key_val(input_dict["value"]))
                continue
            value_dict = decode_key_val(input_dict["value"])
            result = user_defined_job(value_dict["key"], value_dict["value"]) # User defined function
            str_out = json.dumps({"key": input_dict["key"], "value": result})
            print("OUTPUT: ", line, file=sys.stderr)
            print(str_out)
            sys.stdout.flush()
    except Exception as e:
        print(e, f"{line}")
        print("ERROR")


