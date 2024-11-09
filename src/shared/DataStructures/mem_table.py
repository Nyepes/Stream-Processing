from collections import defaultdict

class MemTable:
    MAX = 10
    def __init__(self):
        self.data = defaultdict(list)
        self.file_versions = defaultdict(int)

    def add(self, filename, content):
        self.data[filename].append(content)
        return 1

    def get_file_version(self, file_name):
        if (file_name in self.file_versions):
            return self.file_versions[file_name]
        return None
    
    def set_file_version(self, file_name, version):
        self.file_versions[file_name] = version

    def get(self, file_name):
        return self.data[file_name]
    
    def clear(self, file_name):
        self.data[file_name] = []
    
    ## TODO: On 10 return