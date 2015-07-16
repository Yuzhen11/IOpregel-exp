haloop com.sun.tools.javac.Main PageRank.java

jar cf pr.jar *.class

haloop jar pr.jar PageRank /yuzhen/toytest/ /yuzhen/output 2
## iteration number = pregel iteration number - 2

haloop jar pr.jar PageRank /yuzhen/twitter/ /yuzhen/output 5


haloop fs -rmr /yuzhen/output


haloop com.sun.tools.javac.Main PageRank.java RankLoopInputOutput.java RankReduceCacheFilter.java RankReduceCacheSwitch.java PageRankStepHook.java
