import sys
import os
import socket
from src.FileSystem.shared import generate_sha1, id_from_ip, get_server_file_path

# print(f"VM ID {id_from_ip(socket.gethostname())}")
root_dir = "src/FileSystem/fs"
metadata_dir = "metadata"
# List all files and directories and reconstruct the full path
for dirpath, dirnames, filenames in os.walk(root_dir):
    # print(dirnames)
    if metadata_dir in dirnames:
        dirnames.remove(metadata_dir)
    for filename in filenames:
        # Recreate the path with forward slashes
        full_path = os.path.join(dirpath, filename)
        # Convert backslashes (Windows) to forward slashes
        full_path = full_path.replace(os.path.sep, "/")
        # print(full_path)
