# import rege.x as re
from collections import defaultdict
# file = open("tests/TrafficSigns_5000" ,"r")

with open("tests/TrafficSigns_5000", 'r') as file:
    categories = defaultdict(int)
    for line in file:
        row = line.split(",")
        SignType = row[3]
    
        if "Stop" != SignType: continue
        categories[row[8]] += 1

print(categories)

        

            