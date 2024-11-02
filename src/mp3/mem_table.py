from collections import defaultdict

class MemTable:
    MAX = 10
    def __init__(self):
        self.data = defaultdict(list)
    def add(self, filename, content):
        self.data[filename].append(content)
        print(self.data[filename])
        return 1
    def get(self, file_name):
        return self.data[filename]
    
    def clear(self, file_name):
        self.data[file_name] = []
    
    def merge(self, file_name):
        with open(f"fs/file_name", "a") as file:
            for chunk in self.data:
                file.write(chunk)
    
    ## TODO: On 10 return