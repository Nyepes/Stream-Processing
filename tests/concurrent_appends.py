import os
from datetime import datetime

init_time = datetime.now()
for i in range(200):
    os.system("./run.sh multiappend lyrics.md 1 2 4kb.txt 4kb.txt")
endtime = datetime.now()
print(endtime - init_time)
