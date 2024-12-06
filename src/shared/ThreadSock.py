from socket import socket
import threading

class ThreadSock():
    def __init__(self, sock: socket):
        self.socket = sock
        self.lock = threading.Lock()
    
    def get_socket(self):
        return self.socket

    def sendall(self, data: str):
        with self.lock:
            self.socket.sendall(data)
    def recv(self, num_bytes):
        with self.lock:
            data = b""
            while len(data) < num_bytes:
                packet = self.socket.recv(num_bytes - len(data))
                if not packet:
                    return data
                data += packet
            return data