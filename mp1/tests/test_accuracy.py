import os
from constants import COMMON_STRING, NORMAL_STRING, RARE_STRING
"""
Tests to make sure that the grep works between machines 
and that all caharacters and grep commands work.
For all the test cases we may assume that all machines are turned on
and have server running
"""

def test_simple_grep():
    """
    Test that a simple command with no options and no special characters work.
    Assumes that a server that has a log file, such that the frequencies of common, somewhat and rare
    patterns are those in answers.txt
    """
    frequent = 0
    somewhat_frequent = 0
    rare = 0
    os.system(f"""
              (python3 src/client.py {COMMON_STRING} >> ./tests/actual1.txt) && 
              (python3 src/client.py {NORMAL_STRING} >> ./tests/actual1.txt) && 
              (python3 src/client.py "{RARE_STRING}" >> ./tests/actual1.txt)""")
    with open("./tests/actual1.txt") as file:
        for line in file:
            stripped = line.strip()
            if COMMON_STRING in stripped:
                frequent += 1
            elif NORMAL_STRING in stripped:
                somewhat_frequent += 1
            elif RARE_STRING in stripped:
                rare += 1
        file.close()
    with open("./tests/answers.txt") as file:
        frequent_real = (int)(file.readline())
        somewhat_frequent_real = (int)(file.readline())
        rare_real = (int)(file.readline())
    
    os.remove("./tests/actual1.txt")
    assert(frequent == frequent_real)
    assert(somewhat_frequent == somewhat_frequent_real)
    assert(rare == rare_real)



    assert(True)

def test_complex_grep():
    """
    Test that a simple command with multiple options and special characters work
    """
    assert(True)