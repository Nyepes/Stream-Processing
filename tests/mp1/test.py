import os

"""

Test suite to determine if our distributed grep 
implementation works under distinct cases.

For all the test cases we may assume that all 
machines are turned on and have server.py running.

"""

def compare(expected, test):

    """
    
    Compares the contents of a result file with an expected list of strings.

    Parameters:
        expected (list[str]): A list of expected strings that should match the lines in the file.
        test (str): A message to be used when an assertion fails.

    Raises:
        AssertionError: If a line in the file is not found in the `expected` list, an assertion error is raised with the provided `test` message.

    """
    
    with open("result.txt", "r") as file:
        lines = file.read().split("\n")[:-1]
        for line in lines:
            assert line in expected, test

def test_simple_grep():
    
    """
    
    Tests the grep functionality by simulating log queries for frequent, medium, and rare IPs.

    This function performs the following:
    1. Defines the expected output for log queries from frequent, medium, and rare IPs.
    2. Executes the `client.py` script with different IP addresses.
    3. Compares actual vs expected output.

    The test cases are (based on generate_log.py):
    - Frequent IP: `192.168.1.100`
    - Medium frequency IP: `192.168.1.150`
    - Rare IP: `10.0.0.50`

    Raises:
        AssertionError: If any of the test results do not match the expected output.
    
    """

    expected_frequent = {
        "machine.1.log: 600", 
        "machine.2.log: 600", 
        "machine.3.log: 600", 
        "machine.4.log: 600", 
        "machine.5.log: 600", 
        "machine.6.log: 600", 
        "machine.7.log: 600", 
        "machine.8.log: 600",
        "machine.9.log: 600", 
        "machine.10.log: 600"
    }

    expected_medium = {
        "machine.1.log: 300", 
        "machine.2.log: 300", 
        "machine.3.log: 300", 
        "machine.4.log: 300", 
        "machine.5.log: 300", 
        "machine.6.log: 300", 
        "machine.7.log: 300", 
        "machine.8.log: 300",
        "machine.9.log: 300", 
        "machine.10.log: 300"
    }

    expected_rare = {
        "machine.1.log: 100", 
        "machine.2.log: 100", 
        "machine.3.log: 100", 
        "machine.4.log: 100", 
        "machine.5.log: 100", 
        "machine.6.log: 100", 
        "machine.7.log: 100", 
        "machine.8.log: 100",
        "machine.9.log: 100", 
        "machine.10.log: 100"
    }

    #Frequent
    os.system('python ../src/client.py -c "192.168.1.100" > result.txt')
    compare(expected_frequent, "Frequent")

    #Medium
    os.system('python ../src/client.py -c "192.168.1.150" > result.txt')
    compare(expected_medium, "Medium")

    # Rare
    os.system('python ../src/client.py -c "10.0.0.50" > result.txt')
    compare(expected_rare, "Rare")
    
    os.remove("result.txt ")

def test_complex_grep():
    
    """
    
    Same as test_simple_grep, but test grep functionality with complex options and special characters.
    More specifically tests that the regex expression `/product/\d+` works properly.

    """
    
    expected_regex = {
        "machine.1.log: 350", 
        "machine.2.log: 350", 
        "machine.3.log: 350", 
        "machine.4.log: 350", 
        "machine.5.log: 350", 
        "machine.6.log: 350", 
        "machine.7.log: 350", 
        "machine.8.log: 350",
        "machine.9.log: 350", 
        "machine.10.log: 350"
    }

    os.system('python client.py -c -P "/product/\d+" > result.txt')
    compare(expected_regex)
    
    os.remove("result.txt ")

test_simple_grep()