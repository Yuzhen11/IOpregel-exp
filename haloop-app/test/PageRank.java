import java.io.*; 
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;



public class PageRank
{
	
	public static class JoinMapper extends MapReduceBase implements
			Mapper<Object, Text, IntWritable, Text> {
	
		public void map(Object key, Text value, OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException {
			
			StringTokenizer tk = new StringTokenizer(value.toString());
			IntWritable SourceId = new IntWritable(Integer.parseInt(tk.nextToken()));
			output.collect(SourceId, new Text(tk.nextToken("\n").trim()));
		}
	}
	
	public static class JoinReducer extends MapReduceBase implements
            Reducer<IntWritable, Text, IntWritable, Text> { 			
            
        public void reduce(IntWritable key, Iterator<Text> values, OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException {

        	while (values.hasNext()){
        		String str = values.next().toString();        		
    			output.collect(key, new Text(str));
        	}
        }
    }
    
 
    
    public static void main(String[] args) throws Exception {
    	
        String inputPath = args[0];
        String outputPath = args[1];
    	
    	//loop
    	JobConf conf1 = new JobConf();
    	conf1.setOutputKeyClass(IntWritable.class);
    	conf1.setOutputValueClass(Text.class);
    	conf1.setMapOutputKeyClass(IntWritable.class);
        conf1.setMapOutputValueClass(Text.class);
        conf1.setMapperClass(JoinMapper.class);
 		conf1.setReducerClass(JoinReducer.class);
    	conf1.setInputFormat(TextInputFormat.class);
 		conf1.setOutputFormat(TextOutputFormat.class);
 		//conf1.setPartitionerClass(FirstPartitioner.class);
 		

 		JobConf conf = new JobConf(PageRank.class);
 		conf.setJobName("PageRank");
 		conf.setLoopInputOutput(RankLoopInputOutput.class);
        conf.setLoopReduceCacheSwitch(RankReduceCacheSwitch.class);
        conf.setLoopReduceCacheFilter(RankReduceCacheFilter.class);
        conf.setLoopStepHook(PageRankStepHook.class);
        conf.setInputPath(inputPath);
        conf.setOutputPath(outputPath);
        
        
        int specIteration = 2;
 		conf.setStepConf(0, conf1);
        
        conf.setIterative(true);
        conf.setNumIterations(specIteration);
        
        JobClient.runJob(conf);
        
    }
    
    
    
    
}




