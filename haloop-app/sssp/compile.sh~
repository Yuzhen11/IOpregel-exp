haloop com.sun.tools.javac.Main SSSP.java

jar cf SSSP.jar *.class

haloop jar SSSP.jar SSSP /yuzhen/toytest/ /yuzhen/output 2 3
#input_path output_path src iteration
## iteration number = pregel iteration number - 1

haloop fs -rmr /yuzhen/output


haloop com.sun.tools.javac.Main SSSP.java SSSPLoopInputOutput.java SSSPReduceCacheFilter.java SSSPReduceCacheSwitch.java SSSPStepHook.java










weighted:

jar cf SSSP_weighted.jar *.class

haloop jar SSSP_weighted.jar SSSP_weighted /yuzhen/toy_weighted/ /yuzhen/output 2

haloop fs -rmr /yuzhen/output


haloop com.sun.tools.javac.Main SSSP_weighted.java SSSPLoopInputOutput.java SSSPReduceCacheFilter.java SSSPReduceCacheSwitch.java SSSPStepHook.java
