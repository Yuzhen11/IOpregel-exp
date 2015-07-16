ssh -t master $1 &
for ((i=1, WorkerNum=15; i<=WorkerNum; i++ ))
do
    #$(printf 'ssh -t worker%d %s &' $i "$1") 
    ssh -t worker$i $1 &
done
