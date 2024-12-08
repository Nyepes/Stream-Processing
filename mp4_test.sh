./run.sh create tests/TrafficSigns_1000 TrafficSigns_1000
sleep 1
# ./run.sh create tests/TrafficSigns_5000 TrafficSigns_5000
#sleep 1

# ./run.sh create tests/TrafficSigns_10000 TrafficSigns_10000
# sleep 2

./run.sh Rainstorm 'python tests/app2_op1.py Stop' "python tests/app2_op2.py" TrafficSigns_1000 output 1 1
# ./run.sh merge output
