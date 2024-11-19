if [ "$1" == "1" ]; then
    ./run.sh create test_files/business_1.txt 1.txt
    ./run.sh create test_files/business_2.txt 2.txt
    ./run.sh create test_files/business_3.txt 3.txt
    ./run.sh create test_files/business_4.txt 4.txt
    ./run.sh create test_files/business_5.txt 5.txt
elif [ "$1" == "2" ]; then
    echo "FILES:"
    ./run.sh ls 1.txt
    echo ""
    echo "RING:"
    ./run.sh list_mem_ids
    echo ""
    echo "STORE:"
    ./run.sh store
elif [ "$1" == "3" ]; then
    echo "FILES:"
    ./run.sh ls 1.txt
elif [ "$1" == "4" ]; then
    echo "APPENDS:"
    ./run.sh append test_files/business_11.txt 5.txt
    ./run.sh append test_files/business_12.txt 5.txt
    ./run.sh get 5.txt 5.txt
elif [ "$1" == "5" ]; then
    echo "APPENDS:"
    # ./run.sh multiappend 5.txt 1 2 3 4 test_files/business_13.txt test_files/business_14.txt test_files/business_15.txt test_files/business_16.txt
    # sleep 5
    # ./run.sh merge 5.txt
    ./run.sh getfromreplica 2 5.txt 5_1.txt
    ./run.sh getfromreplica 3 5.txt 5_2.txt

else
    echo "Invalid test case number"
fi