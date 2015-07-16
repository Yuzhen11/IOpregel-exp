
haloop=/data/yuzhen/haloop/bin/hadoop
# Pagerank
$haloop fs -rmr /yuzhen/output

date | tee time
sleep 19200
date | tee time



$haloop jar pr.jar PageRank /yuzhen/twitter/ /yuzhen/output 2 2>&1 | tee pagerank-twitter

$haloop fs -rmr /yuzhen/output

# Hashmin

$haloop jar Hashmin.jar Hashmin /yuzhen/btc/ /yuzhen/output 29 2>&1 | tee hashmin-btc

$haloop fs -rmr /yuzhen/output

$haloop jar Hashmin.jar Hashmin /yuzhen/friendster/ /yuzhen/output 21 2>&1 | tee hashmin-friendster

$haloop fs -rmr /yuzhen/output

# sssp

$haloop jar SSSP.jar SSSP /yuzhen/btc /yuzhen/output 0 15 2>&1 | tee sssp-btc
$haloop fs -rmr /yuzhen/output

$haloop jar SSSP.jar SSSP /yuzhen/friendster /yuzhen/output 0 22  2>&1 | tee sssp-friendster
$haloop fs -rmr /yuzhen/output

$haloop jar SSSP.jar SSSP /yuzhen/twitter /yuzhen/output 0 15 2>&1 | tee sssp-twitter 
$haloop fs -rmr /yuzhen/output



