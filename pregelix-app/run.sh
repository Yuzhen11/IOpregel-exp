

#bin/pregelix neighbor-counting-0.2.12-jar-with-dependencies.jar edu.uci.ics.pregelix.nc.PageRankVertex -inputpaths /yuzhen/toy -outputpath /yuzhen/output -ip 192.168.50.99 -port 3099 -num-iteration 10 2>&1 | tee pagerank-webuk



#bin/pregelix neighbor-counting-0.2.12-jar-with-dependencies.jar edu.uci.ics.pregelix.nc.PageRankVertex -inputpaths /yuzhen/twitter -outputpath /yuzhen/output -ip 192.168.50.99 -port 3099 -num-iteration 10 2>&1 | tee pagerank-twitter

bin/pregelix neighbor-counting-0.2.12-jar-with-dependencies.jar edu.uci.ics.pregelix.nc.ConnectedComponentsVertex -inputpaths /yuzhen/friendster -outputpath /yuzhen/output -ip 192.168.50.99 -port 3099  2>&1 | tee cc-friendster
date
bin/pregelix neighbor-counting-0.2.12-jar-with-dependencies.jar edu.uci.ics.pregelix.nc.ConnectedComponentsVertex -inputpaths /yuzhen/btc -outputpath /yuzhen/output -ip 192.168.50.99 -port 3099  2>&1 | tee cc-btc
date
