./run.sh start 1 &
sleep 5
./run.sh create tests/TrafficSigns_5000 TS5k1
sleep 10
./run.sh Rainstorm 'python tests/app1_op1.py "Unpunched T"' 'python tests/app1_op2.py' TS5k1 output 1