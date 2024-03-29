import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.iterative.LoopInputOutput;

public class RankLoopInputOutputReuse implements LoopInputOutput {

        @Override
        public List<String> getLoopInputs(JobConf conf, int iteration, int step) {
                List<String> paths = new ArrayList<String>();
                int currentPass = 2 * iteration + step;
                if (currentPass == 0) {
                        //paths.add(conf.getOutputPath() + "/count");
                        paths.add(conf.getInputPath());
                        paths.add(conf.getOutputPath() + "/i" + (currentPass - 1));
                }
                else {
                    if (step == 0) 
                        paths.add(conf.getOutputPath() + "/i1");
                    else
                        paths.add(conf.getOutputPath() + "/i0");
                }

                //if (step == 0) paths.add(conf.getInputPath());
                
                return paths;
        }

        @Override
        public String getLoopOutputs(JobConf conf, int iteration, int step) {
                int currentPass = 2 * iteration + step;
                return (conf.getOutputPath() + "/i" + step);
        }

}

