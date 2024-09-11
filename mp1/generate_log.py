import random
import string
import sys

def generate_random_string(length):
    """Generate a random string of a given length."""
    letters = string.ascii_letters + string.digits # All characters + all digits
    return ''.join(random.choice(letters) for _ in range(length))

def write_random_strings_to_file(filename, num_strings):
    """Write a specified number of random strings to a file."""
    with open(filename, 'w') as file:
        for _ in range(num_strings):
            random_string = generate_random_string(random.randint(1, 50))
            file.write(random_string + '\n')

if __name__ == "__main__":
    if (len(sys.argv) != 3):
        print("Usage: python3 generate_log.py <filename> <number of strings>")
    filename = sys.argv[1] # File to write to
    num_strings = [2]  # Number of random strings to generate

    write_random_strings_to_file(filename, num_strings)