import random
import os
from datetime import datetime
import time
import numpy as np

# create files
NUM_FILES = 1000

random.seed(42)
for i in range(2 * NUM_FILES):
    file_id = random.randint(1, NUM_FILES)
    os.system(f"./run.sh get {file_id}.txt {file_id}.txt")
    current_time = time.time()
    os.utime(f"src/FileSystem/local_cache/{file_id}.txt", (current_time, current_time))

NUM_READS = int(2.5 * NUM_FILES)

zipf_param = 1.5
zipf_ranks = np.random.zipf(zipf_param, NUM_READS)
zipf_ranks = zipf_ranks[zipf_ranks <= NUM_FILES]

time_init = datetime.now()

for rank in zipf_ranks:
    file_to_read = rank
    if (random.random() < 0.1):
        os.system(f"./run.sh append 4kb.txt {file_to_read}.txt")
    else:
        os.system(f"./run.sh get {file_to_read}.txt {file_to_read}.txt")

time_end = datetime.now()
print(time_end - time_init)