import threading
from collections import defaultdict

class Dict:
    """
    A thread-safe dictionary wrapper that uses a defaultdict for storage.

    Attributes:
        dict (defaultdict): The underlying dictionary that stores key-value pairs.
        lock (threading.Lock): A lock to ensure thread-safe operations on the dictionary.
    """

    def __init__(self, t=int):
        """
        Initializes the Dict with a specified default type.

        Parameters:
            t (type): The default type for the dictionary values (default is int).
        """
        self.dict = defaultdict(t)
        self.lock = threading.Lock()

    def add(self, key, val):
        """
        Adds a key-value pair to the dictionary.

        Parameters:
            key: The key to add.
            val: The value associated with the key.
        """
        with self.lock:
            self.dict[key] = val

    def increment_list(self, key, val):
        """
        Increments the value associated with the key by adding to a list.

        If the value is not a list, it converts it into a list before adding.

        Parameters:
            key: The key whose value will be incremented.
            val: The value to add to the list.
        """
        with self.lock:
            if type(val) != list:
                val = [val]
            self.dict[key] += val

    def get(self, key):
        """
        Retrieves a copy of the value associated with the key.

        Parameters:
            key: The key whose value is to be retrieved.

        Returns:
            A copy of the value associated with the key.
        """
        with self.lock:
            return self.dict[key].copy()

    def increment(self, key):
        """
        Increments the value associated with the key by 1.

        This method assumes that the value supports the += operation.
        
        Parameters:
            key: The key whose value will be incremented.
        """
        # Undefined behavior if does not support += 1
        with self.lock:
            self.dict[key] += 1

    def delete(self, key):
        """
        Deletes the key-value pair associated with the key.

        Parameters:
            key: The key to delete from the dictionary.
        """
        with self.lock:
            if key in self.dict:
                del self.dict[key]

    def items(self):
        """
        Retrieves a list of key-value pairs in the dictionary.

        Returns:
            A list of tuples representing the key-value pairs.
        """
        with self.lock:
            return list(self.dict.items())