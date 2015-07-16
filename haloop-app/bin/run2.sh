
haloop=/data/yuzhen/haloop/bin/hadoop

$haloop jar SSSP.jar SSSP /yuzhen/webuk /yuzhen/output 2 664 2>&1 | tee sssp-webuk
