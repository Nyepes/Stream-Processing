from socket import socket
import threading

class ThreadSock():
    def __init__(self, sock: socket):
        self.socket = sock
        self.send_lock = threading.Lock()
        self.recv_lock = threading.Lock()
    
    def get_socket(self):
        return self.socket

    def sendall(self, data: str):
        with self.send_lock:
            if (self.socket == None): return b""
            self.socket.sendall(data)
    def recv(self, num_bytes):
        with self.recv_lock:
            if self.socket == None: return b""
            data = b""
            while len(data) < num_bytes:
                packet = self.socket.recv(num_bytes - len(data))
                if not packet:
                    # print("DATA RETRUNED" + data.decode())
                    return data
                data += packet
            # print("DATA RETRUNED" + data.decode())
            return data
    def replace(self, new_sock):
        with self.send_lock:
            with self.recv_lock:
                if (self.socket is None): return
                self.socket.close()
                self.socket = new_sock
    