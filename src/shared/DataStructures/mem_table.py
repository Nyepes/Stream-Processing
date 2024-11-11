from collections import defaultdict

class MemTable:
    """
    A memory table that stores file contents and their versions.

    Attributes:
        MAX (int): The maximum number of entries allowed (currently unused).
        data (defaultdict): A dictionary that maps filenames to a list of content-status tuples.
        file_versions (defaultdict): A dictionary that maps filenames to their respective version numbers.
    """
    MAX = 10

    def __init__(self):
        """Initializes the MemTable with empty data and file_versions."""
        self.data = defaultdict(list)
        self.file_versions = defaultdict(int)

    def add(self, filename, content, status):
        """
        Adds a new content-status tuple to the specified file's entry.

        Parameters:
            filename (str): The name of the file to which content is being added.
            content: The content to be added.
            status (str): The status of the content (e.g., 'N' for new, 'F' for failed).

        Returns:
            int: Returns 1 to indicate successful addition.
        """
        self.data[filename].append((content, status))
        return 1

    def get_file_version(self, file_name):
        """
        Retrieves the version number of the specified file.

        Parameters:
            file_name (str): The name of the file whose version is to be retrieved.

        Returns:
            int or None: The version number if it exists, otherwise None.
        """
        if (file_name in self.file_versions):
            return self.file_versions[file_name]
        return None
    
    def set_file_version(self, file_name, version):
        """
        Sets the version number for the specified file.

        Parameters:
            file_name (str): The name of the file for which the version is being set.
            version (int): The version number to be set.
        """
        self.file_versions[file_name] = version

    def get(self, file_name):
        """
        Retrieves the content-status tuples for the specified file.

        Parameters:
            file_name (str): The name of the file whose content is to be retrieved.

        Returns:
            list: A list of content-status tuples associated with the file.
        """
        return self.data[file_name]
    
    def clear(self, file_name):
        """
        Clears all content for the specified file.

        Parameters:
            file_name (str): The name of the file to be cleared.
        """
        self.data[file_name] = []

    def delete(self, file_name):
        """
        Deletes the entry for the specified file.

        Parameters:
            file_name (str): The name of the file to be deleted.
        """
        del self.data[file_name]
        
    def items(self):
        """
        Retrieves all items in the memory table.

        Returns:
            dict_items: A view of the dictionary's items (filename and associated content).
        """
        return self.data.items()