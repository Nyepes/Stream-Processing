import threading
import time

"""
A thread-safe dictionary class (`TTLDict`) where entries automatically 
expire after a specified time-to-live (TTL). This is useful for tracking members 
who join, become suspicious, or fail, and ensures that expired data is removed 
in the background, making it ready to send to other processes.
"""

class TTLDict:
    
    def __init__(self):
        self.store = {}
        self.lock = threading.Lock()
        self.condition = threading.Condition(self.lock)

        worker_thread = threading.Thread(target=self.remove_expired_entries)
        worker_thread.start()

    def set(self, key, value, ttl):

        expiration_time = time.time() + ttl

        self.lock.acquire()
        if (key in self.store and self.store[key][0] == value):
            self.lock.release()
            return

        self.store[key] = (value, expiration_time)
        self.condition.notify()

        self.lock.release()

    def get(self, key):
        val = None
        self.lock.acquire()
        if (key in self.store):
            val = self.store.get(key)[0]
        self.lock.release()
        return val

    def get_all(self):
        data = {}

        self.lock.acquire()
        data = self.store
        self.lock.release()
        data_no_ttl = {}
        for key, val in data.items():
            # Remove TTL field
            data_no_ttl[key] = data[key][0]
        return data_no_ttl

    def remove_expired_entries(self):
        with self.condition:
            while True:
                # Avoid spurious wakeup
                while len(self.store) == 0:
                    self.condition.wait()

                nearest_expiration = min(expiration for _, expiration in self.store.values())
                current_time = time.time()

                if len(self.store) == 0 or nearest_expiration > current_time:
                    self.condition.wait(timeout=nearest_expiration - current_time)
                else:
                    break

                # Remove all expired keys
                copy = self.store.copy()
                current_time = time.time()
                for key in copy:
                    expiration = copy[key][1]
                    if (expiration <= current_time):
                        del self.store[key]