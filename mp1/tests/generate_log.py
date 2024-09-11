import random
import string
import sys
from constants import COMMON_STRING, NORMAL_STRING, RARE_STRING





def generate_random_string(length):
    """Generate a random string of a given length."""
    letters = string.ascii_letters + string.digits # All characters + all digits
    return ''.join(random.choice(letters) for _ in range(length))

def write_random_strings_to_file(filename, num_strings):
    num_common = 0
    num_normal = 0
    num_rare = 0
    """Write a specified number of random strings to a file."""
    with open(filename, 'w') as file:
        for _ in range(num_strings):
            random_float = random.random()
            if (random_float < 0.6):
                random_string = COMMON_STRING
                num_common += 1
            elif(random_float < 0.9):
                random_string = NORMAL_STRING
                num_normal += 1
            else:
                random_string = RARE_STRING
                num_rare += 1
            file.write(random_string + '\n')
    return num_common, num_normal, num_rare

if __name__ == "__main__":
    if (len(sys.argv) != 4):
        print("Usage: python3 generate_log.py <filename> <number of strings>")
    log_filename = sys.argv[1] # File to write to
    answers_filename = sys.argv[2]
    num_strings = int(sys.argv[3])  # Number of random strings to generate

    num_common, num_normal, num_rare = write_random_strings_to_file(log_filename, num_strings)
    with open(answers_filename, 'w') as file:
        file.write(f"{num_common}\n")
        file.write(f"{num_normal}\n")
        file.write(f"{num_rare}\n")