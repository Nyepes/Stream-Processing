import sys
import os
import socket
from src.mp3.shared import generate_sha1, id_from_ip, get_server_file_path

print(f"VM ID {id_from_ip(socket.gethostname())}")
for filename in os.listdir("src/mp3/fs"):
    file_path = get_server_file_path(filename)
    if os.path.isfile(file_path):  # Only process files, not directories
        print(f"{filename} \t {generate_sha1(filename)}")
