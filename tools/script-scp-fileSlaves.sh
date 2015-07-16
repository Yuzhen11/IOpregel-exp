for ((i=1, WorkerNum=15; i<=WorkerNum; i++))
do
    scp -r $1 worker$i:$1 &
    echo copy to worker $i
done
