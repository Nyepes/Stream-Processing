./run.sh create tests/TrafficSigns_1000 TrafficSigns_1000
sleep 1

./run.sh Rainstorm 'python tests/app2_op1.py Stop' "python tests/app2_op2.py" TrafficSigns_1000 output 1 1
