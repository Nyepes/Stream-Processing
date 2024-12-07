# ./run.sh create tests/TrafficSigns_1000 TrafficSigns_1000
# sleep 1
# ./run.sh create tests/TrafficSigns_5000 TrafficSigns_5000
# sleep 1

./run.sh create tests/TrafficSigns_10000 TrafficSigns_10000

./run.sh Rainstorm "python tests/mp4_demo1.py Unpunched" "python tests/mp4_demo2.py" TrafficSigns_10000 output 1
./run.sh merge output