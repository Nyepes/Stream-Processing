from src.shared.constants import DATA_LENGTH_BYTES
from socket import socket

"""

Helper functions that are used within both client and server 
models to recieve and send data over the socket object.

"""

def send_data(socket: socket, data: str):

    """
    
    Sends a string message over a socket connection with a fixed-length header indicating the message size.

    This function performs the following steps:
    1. Calculates the length of the data to be sent.
    2. Encodes the size as a zero-padded string to match `DATA_LENGTH_BYTES` bytes.
    3. Sends the size header followed by the actual data over the socket.

    Parameters:
        socket (socket.socket): The connected socket object to send data through.
        data (str): The string to be sent over the socket.

    Returns:
        None

    """

    size = len(data)
    num_characters = 0
    
    while size > 0:
        
        size //= 10
        num_characters += 1

    encoded_size = "0" * (DATA_LENGTH_BYTES - num_characters) + str(len(data))
    socket.sendall((encoded_size + data).encode("utf-8"))

def receive_data(socket: socket):

    """
    
    Receives a complete message from a socket connection.

    This function performs the following steps:
    1. Reads a fixed number of bytes from the socket to determine the total size of the incoming message.
    2. Receives the message in chunks of 1024 bytes until the entire message is received.
    3. Decodes the received bytes using UTF-8 encoding and returns the resulting string.

    Parameters:
        socket (socket.socket): The connected socket object from which to receive data.

    Returns:
        str: The complete message received from the socket, decoded using UTF-8.

    Raises:
        ConnectionRefusedError: If the socket connection is closed or no data is received during the reception.
    
    """
    
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


def udp_send_data(socket: socket, data, address):
    """
    This is intended to be sent in a single packet since more 
    could possible mean some do not get there. As aresult there is no need 
    of implementing logic to partition message
    """
    if (len(data) >= 2048):
        print("attempted to send too much data")
        return -1
    message = data.encode("utf-8")
    socket.sendto(message, address)
    return

def udp_receive_data(socket: socket):
    data, address = socket.recvfrom(DATA_LENGTH_BYTES)
    return data, address