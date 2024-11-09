import threading
from collections import defaultdict

class Dict:
    def __init__(self, t = int):
        self.dict = defaultdict(t)
        self.lock = threading.Lock()
    def add(self, key, val):
        with self.lock:
            self.dict[key] = val
    def increment_list(self, key, val):
        with self.lock:
            self.dict[key] += [val]
    def get(self, key):
        with self.lock:
            return self.dict[key].copy()
    def increment(self, key):
        # Undefined behavior if does not support += 1
        with self.lock:
            self.dict[key] += 1
    def delete(self, key):
        with self.lock:
            del self.dict[key]
    def items(self):
        with self.lock:
            return list(self.dict.items())

    