from src.mp4.worker import encode_key_val
import sys

for i in range(100):  # Read from stdin line by line
    new_key = f"1:{i}" 
    print(encode_key_val(new_key, "a"))