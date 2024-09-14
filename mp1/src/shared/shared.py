from socket import socket
from shared.constants import DATA_LENGTH_BYTES
from time import sleep

def send_data(socket: socket, data: str):
    """
    Writes the string data to the socket tcp connection specified in socket.
    We use the first DATA_LENGTH_BYTES bytes of the string to specify the total length of the packet.
    """
    size = len(data)
    num_characters = 0
    # Count character size to store as encoded size string
    while size > 0:
        size //= 10
        num_characters += 1

    # Forms string of length 10 where that stores in string form the length of the string.
    # This is useful to determine how much data to receive.
    encoded_size = "0" * (DATA_LENGTH_BYTES - num_characters) + str(len(data))
    socket.sendall((encoded_size + data).encode("utf-8"))

def receive_data(socket: socket):
    """
    Receives a message from socket and returns it.
    The first DATA_LENGTH_BYTES bytes of the string are used to determine how many bytes are in the message.
    The data is sent in 1024 byte packets.
    """
    # Send DATA_LENGTH_BYTES bytes (arbitrary str representation of int)
    # We will prefix with 0s to force int length of data to send to be 10 bytes = 10 characters
    data = socket.recv(DATA_LENGTH_BYTES)
    size = int(data.decode("utf-8"))
    BYTE_STREAM = 1024
    message = ""

    for _ in range(0, size, BYTE_STREAM):
        received = socket.recv(BYTE_STREAM)
        if (received == ''):
            raise ConnectionRefusedError
        message += received.decode("utf-8")
    return message
