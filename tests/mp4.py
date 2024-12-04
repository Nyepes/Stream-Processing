import sys
import json
from time import sleep
from src.mp4.worker import decode_key_val
import signal


def handle_signal(signum, frame):
    print(f"Received signal {signum}, but continuing...")
    sys.stdout.flush()

machine_id = int(sys.argv[1])
signal.signal(signal.SIGTERM, handle_signal)
signal.signal(signal.SIGHUP, handle_signal)
signal.pthread_sigmask(signal.SIG_BLOCK, range(1, signal.NSIG))

def user_defined_job(key, val):
    return [(key, val)]

file =  open("output.txt", "w")
try:
    for line in sys.stdin:  # Read from stdin line by line

        input_dict = decode_key_val(line)

        value_dict = decode_key_val(input_dict["value"])
        result = user_defined_job(value_dict["key"], value_dict["value"]) # User defined function

        str_out = json.dumps({"key": input_dict["key"], "value": result})

        file.write(str_out)
        file.flush()

        print(str_out)
        sys.stdout.flush()
except Exception as e:
    print("ERROR")
    print(e)


