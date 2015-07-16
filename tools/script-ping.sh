for ((i=1, WorkerNum=15; i<=WorkerNum; i++))
do
    ping worker$i
done
