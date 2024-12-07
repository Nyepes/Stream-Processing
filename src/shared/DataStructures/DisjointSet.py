import threading

class DisjointSet:
    def __init__(size=10):
        self.tree = [ 0 ] * size
        self.vals = [ None ] * size
        self.locks = [threading.Lock()] * size # Each tree has its own lock.
