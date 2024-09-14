import os
from constants import COMMON_STRING, NORMAL_STRING, RARE_STRING
"""
Tests to make sure that the grep works between machines 
and that all caharacters and grep commands work.
For all the test cases we may assume that all machines are turned on
and have server running
"""

def comapre(expected):
    
    with open("result.txt", "r") as file:
        lines = file.read().split("\n")
        for line for lines:
            assert(line in expected)
                

def test_simple_grep():
    """
    Test that a simple command with no options and no special characters work.
    Assumes that a server that has a log file, such that the frequencies of common, somewhat and rare
    patterns are those in answers.txt
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
    os.sys('python client.py -c "192.168.1.100" >> result.txt')
    comapre(expected_frequent)

    #Medium
    os.sys('python client.py -c "192.168.1.150" >> result.txt')
    comapre(expected_medium)

    # Rare
    os.sys('python client.py -c "10.0.0.50" >> result.txt')
    comapre(expected_rare)
    os.remove("result.txt ")
    



def test_complex_grep():
    """
    Test that a simple command with multiple options and special characters work
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
    os.sys('python client.py -c -E "/product/\d+" >> result.txt')
    comapre(expected_regex)
    os.remove("result.txt ")