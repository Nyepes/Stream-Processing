from collections import defaultdict
from typing import List, Dict

class MemTableEntry:

    # Timestamp help us merge in appropriate order (buffer)

    def __init__(self, content: str, machine_id: int, vector_clock: List[int]):
        self.content = content
        self.machine_id = machine_id
        self.vector_clock = vector_clock
    
class MemTable:

    def __init__(self, num_processes: int, process_id: int):
        self.table = defaultdict(list)
        self.num_processes = num_processes
        self.process_id = process_id
        self.vector_clock = [0] * (num_processes + 1)
        self.buffered_messages = defaultdict(list)

    def _insert(self, filename: str, content: str, machine_id: int, vector_clock: List[int]):
        self.table[filename].append(MemTableEntry(content, machine_id, vector_clock))

    def receive_multicast(self, filename: str, content: str, sender_id: int, vector_clock: List[int]):
        if self._can_deliver(sender_id, vector_clock):
            self._deliver_message(filename, content, sender_id, vector_clock)
        else:
            self.buffered_messages[filename].append((content, sender_id, vector_clock))

    def _can_deliver(self, sender_id: int, vector_clock: List[int]) -> bool:
        
        if vector_clock[sender_id] != self.vector_clock[sender_id] + 1:
            return False
        
        for k in range(self.num_processes):
            if k != sender_id and vector_clock[k] > self.vector_clock[k]:
                return False
        
        return True

    def _deliver_message(self, filename: str, content: str, sender_id: int, vector_clock: List[int]):
        self._insert(filename, content, sender_id, vector_clock)
        self.vector_clock[sender_id] = vector_clock[sender_id]
        self._check_buffered_messages(filename)

    def _check_buffered_messages(self, filename: str):
        delivered = True
        while delivered:
            delivered = False
            for i, (content, sender_id, vector_clock) in enumerate(self.buffered_messages[filename]):
                if self._can_deliver(sender_id, vector_clock):
                    del self.buffered_messages[filename][i]
                    self._deliver_message(filename, content, sender_id, vector_clock)
                    delivered = True
                    break

    def merge(self, filename: str):
        
        content = ""
        for entry in self.table[filename]:
            content += entry.content + "\n"

        self.table[filename] = []

        return content.strip()

    def get_all(self, filename: str):
        return self.table[filename]
