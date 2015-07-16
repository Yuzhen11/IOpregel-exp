
haloop=/data/yuzhen/haloop/bin/hadoop

$haloop jar pr.jar PageRank /yuzhen/twitter /yuzhen/output 1 2>&1 | tee log-pagerank-twitter-2
#$haloop jar pr.jar PageRank /yuzhen/webuk /yuzhen/output 8 2>&1 | tee log-pagerank-webuk
