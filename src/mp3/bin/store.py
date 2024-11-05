import sys
import os
import socket
from src.mp3.shared import generate_sha1, id_from_ip

print(f"VM ID {id_from_ip(socket.gethostname())}")
for filename in os.listdir("src/mp3/fs"):
    file_path = os.path.join("src/mp3/fs", filename)
    if os.path.isfile(file_path):  # Only process files, not directories
        print(f"{filename} \t {generate_sha1(filename)}")
