import sys
import json
from time import sleep
from src.Streaming.worker import decode_key_val

# Example user defined job function signature
# def user_defined_job(key, val):
#     return [(key, val)]


def rain_storm_framework(machine_id, user_defined_job, init=None, stateful=False):
    """
    Main framework for processing key-value pairs in a distributed system.
    
    Args:
        machine_id: Identifier for the current machine
        user_defined_job: Function that processes key-value pairs
        init: Optional initialization function for stateful processing
        stateful: Boolean indicating if the processing maintains state
    
    The framework:
    1. Reads input key-value pairs from stdin
    2. Processes state initialization if needed
    3. Applies the user defined job function to each valid input
    4. Outputs results as JSON to stdout
    """
    # Print and flush processing mode
    if stateful: 
        print("STATEFUL")
        sys.stdout.flush()
    else:
        print("STATELESS")
        sys.stdout.flush()

    try:
        # Main processing loop
        for line in sys.stdin:  
            print("INPUT: ", line, file=sys.stderr)
            
            # Check for termination signal
            if (line == "DONE"):
                exit(1)
                
            # Decode input line into key-value dictionary
            input_dict = decode_key_val(line)

            # Skip empty/null values
            if (input_dict["value"] == "null" or input_dict["value"] == None or input_dict["value"] == "{}"):
                continue
                
            # Handle state initialization for stateful processing
            if (init is not None and input_dict["key"] == "STATE" and (input_dict["value"] != None or input_dict["value"] != "{}")):
                init(decode_key_val(input_dict["value"]))
                continue

            # Decode the nested value dictionary
            value_dict = decode_key_val(input_dict["value"])

            # Process key-value pair through user defined function
            result = user_defined_job(value_dict["key"], value_dict["value"])
            
            # Format and output results
            str_out = json.dumps({"key": input_dict["key"], "value": result})
            print(str_out)
            sys.stdout.flush()
            
    except Exception as e:
        # Error handling
        print(e, f"{line}")
        print("ERROR")

