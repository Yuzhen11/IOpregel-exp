scp $1 master:$1 &
echo copy to master
for ((i=1, WorkerNum=15; i<=WorkerNum; i++))
do
    scp $1 worker$i:$1 &
    echo copy to worker $i
done
