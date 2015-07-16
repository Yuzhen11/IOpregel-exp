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



public class SSSP_weighted
{
	public static final String DIST = "DIST";
	public static final double MAX_DOUBLE = 1000000000;
	
	public static class FirstMapper extends MapReduceBase implements
			Mapper<Object, Text, IntWritable, DoubleWritable> {
		
		private int source = -1;
		public void configure(JobConf job) {
			source = Integer.parseInt(job.get("sourceID"));
		}
		public void map(Object key, Text value, OutputCollector<IntWritable, DoubleWritable> output, Reporter reporter) throws IOException {
			
			//vid neighbors_num n1 n2 ...
			String[] tokens = value.toString().split("\\s+");
			IntWritable SourceId = new IntWritable(Integer.parseInt(tokens[0]));
			int sid = SourceId.get();
			if (sid == source )
				output.collect(SourceId, new DoubleWritable(0));
			else
				output.collect(SourceId, new DoubleWritable(MAX_DOUBLE));
		}
	}
	
	public static class FirstReducer extends MapReduceBase implements
            Reducer<IntWritable, DoubleWritable, IntWritable, Text> { 			
            
        public void reduce(IntWritable key, Iterator<DoubleWritable> values, OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException {
        	double dist = -1;
        	if (values.hasNext()) {
        		dist = values.next().get();
        	}
        	if (dist == 0)
        		output.collect(key, new Text(Double.toString(dist)+" 1"+DIST) );
        	else
        		output.collect(key, new Text(Double.toString(dist)+" 0"+DIST) );
        }   
    }
	
	
	public static class JoinMapper extends MapReduceBase implements
			Mapper<Object, Text, IntWritable, Text> {
	
		public void map(Object key, Text value, OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException {
			
			//input:
			//vid neighbors_num n1 w1 n2 w2 ...
			//vid dist 1/0 "DIST"
			String str = value.toString();
			if (str.endsWith(DIST)) {
				//dist table
				String[] tokens = str.substring(0, str.length()-4).split("\\s+");
				int change = Integer.parseInt(tokens[2]);
				if (change == 1)
				{
					IntWritable SourceId = new IntWritable(Integer.parseInt(tokens[0]));
					StringBuilder sb = new StringBuilder();
					sb.append(tokens[1]); sb.append(" "); sb.append(tokens[2]);sb.append(DIST);
					output.collect(SourceId, new Text(sb.toString()));
				}
			}
			else {
				//edge table
				String[] tokens = value.toString().split("\\s+");
				IntWritable SourceId = new IntWritable(Integer.parseInt(tokens[0]));
				StringBuilder sb = new StringBuilder();
				for (int i = 1; i < tokens.length; i++){
					if (sb.length() != 0) sb.append(" ");
					sb.append(tokens[i]);
				}
				output.collect(SourceId, new Text(sb.toString()));
			}
		}
	}
	
	public static class JoinReducer extends MapReduceBase implements
            Reducer<IntWritable, Text, IntWritable, DoubleWritable> { 			
            
        public void reduce(IntWritable key, Iterator<Text> values, OutputCollector<IntWritable, DoubleWritable> output, Reporter reporter) throws IOException {
        	
        	double dist = MAX_DOUBLE;
        	ArrayList<String> neighbors = new ArrayList<String>();
        	while (values.hasNext()){
        		String str = values.next().toString();
        		if (str.endsWith(DIST)){
        			//dist table
        			String[] tmp = str.substring(0, str.length()-4).split("\\s+");
        			dist = Double.parseDouble(tmp[0]);
        		}
        		else{
        			//adj table
        			String[] tmp = str.split("\\s+");
        			for (String i : tmp)
	        			neighbors.add(i);
        		}
        	}
        	
        	if (neighbors.size() == 0) return;
        	int num = Integer.parseInt(neighbors.get(0));
        	if (num == 0) return;
        	
        	if (dist != MAX_DOUBLE)
        	{
        		//send messages   ---------modified for weighted graph
        		for (int i = 1; i < neighbors.size(); i += 2)
        		{
        			IntWritable TargetId = new IntWritable(Integer.parseInt(neighbors.get(i)));
        		
        			output.collect(TargetId, new DoubleWritable(dist+Double.parseDouble(neighbors.get(i+1)) ));
        		}
        	}
        }
    }
    
    public static class AggMapper extends MapReduceBase implements
			Mapper<Object, Text, IntWritable, Text> {
		
		public void map(Object key, Text value, OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException {	
			//id newDist
			//id dist 1/0 "DIST"
			String[] tokens = value.toString().split("\\s+");
			IntWritable SourceId = new IntWritable(Integer.parseInt(tokens[0]));
			StringBuilder sb = new StringBuilder();
			for (int i = 1; i < tokens.length; i++){
					if (sb.length() != 0) sb.append(" ");
					sb.append(tokens[i]);
				}
			output.collect(SourceId, new Text(sb.toString()));
		}
	}
	
	public static class AggReducer extends MapReduceBase implements
            Reducer<IntWritable, Text, IntWritable, Text> { 			
            
        public void reduce(IntWritable key, Iterator<Text> values, OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException {
        	double dist = -1;
        	double newDist = MAX_DOUBLE;
        	int change = 0;
        	while (values.hasNext()){
        		String str = values.next().toString();
        		if (str.endsWith(DIST)){
        			//dist table
        			String[] tmp = str.substring(0, str.length()-4).split("\\s+");
        			dist = Double.parseDouble(tmp[0]);
        		}
        		else{
        			//messages table
        			String[] tmp = str.split("\\s+");
        			for (String i : tmp)
        			{
        				double tmpDist = Double.parseDouble(i);
        				if (tmpDist < newDist) newDist = tmpDist; 
        			}
        		}
        	}
        	if (newDist < dist) {
        		dist = newDist;
        		change = 1;
        	}
        	output.collect(key, new Text(Double.toString(dist) + " " + Integer.toString(change)+DIST));
        }   
    }
    
    public static class AggCombiner extends MapReduceBase implements
            Reducer<IntWritable, Text, IntWritable, Text> { 			
            
        public void reduce(IntWritable key, Iterator<Text> values, OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException {
        	double dist = -1;
        	double newDist = MAX_DOUBLE;
        	while (values.hasNext()){
        		String str = values.next().toString();
        		if (str.endsWith(DIST)){
        			//dist table
        			String[] tmp = str.substring(0, str.length()-4).split("\\s+");
        			dist = Double.parseDouble(tmp[0]);
        			
        			output.collect(key, new Text(str));
        		}
        		else{
        			//messages table
        			String[] tmp = str.split("\\s+");
        			for (String i : tmp)
        			{
        				double tmpDist = Double.parseDouble(i);
        				if (tmpDist < newDist) newDist = tmpDist; 
        			}
        		}
        	}
        	if (newDist < MAX_DOUBLE) {
        		output.collect(key, new Text(Double.toString(newDist)));
        	}
        }   
    }
    
    public static void main(String[] args) throws Exception {
    	
        String inputPath = args[0];
        String outputPath = args[1];
        String query = args[2];
        
        //step 1
        JobConf conf0 = new JobConf(SSSP_weighted.class);
        conf0.setJobName("SSSP Step1");
        //set argument
        conf0.set("sourceID", query);
        
        conf0.setOutputKeyClass(IntWritable.class);
        conf0.setOutputValueClass(Text.class);
        conf0.setMapOutputKeyClass(IntWritable.class);
        conf0.setMapOutputValueClass(DoubleWritable.class);
        
        conf0.setMapperClass(FirstMapper.class);
 		conf0.setReducerClass(FirstReducer.class);
 		conf0.setInputFormat(TextInputFormat.class);
 		conf0.setOutputFormat(TextOutputFormat.class);
 		FileInputFormat.addInputPath(conf0, new Path(args[0]));
	    FileOutputFormat.setOutputPath(conf0, new Path(args[1]+"/i-1"));
	    JobClient.runJob(conf0);
    
    	
    	//loop
    	
    	JobConf conf1 = new JobConf();
    	conf1.setOutputKeyClass(IntWritable.class);
    	conf1.setOutputValueClass(DoubleWritable.class);
    	conf1.setMapOutputKeyClass(IntWritable.class);
        conf1.setMapOutputValueClass(Text.class);
        conf1.setMapperClass(JoinMapper.class);
 		conf1.setReducerClass(JoinReducer.class);
    	conf1.setInputFormat(TextInputFormat.class);
 		conf1.setOutputFormat(TextOutputFormat.class);
 		
 		JobConf conf2 = new JobConf();
    	conf2.setOutputKeyClass(IntWritable.class);
    	conf2.setOutputValueClass(Text.class);
    	conf2.setMapOutputKeyClass(IntWritable.class);
        conf2.setMapOutputValueClass(Text.class);
        conf2.setMapperClass(AggMapper.class);
 		conf2.setReducerClass(AggReducer.class);
 		//combiner
 		conf2.setCombinerClass(AggCombiner.class);
    	conf2.setInputFormat(TextInputFormat.class);
 		conf2.setOutputFormat(TextOutputFormat.class);
 		
 		JobConf conf = new JobConf(SSSP_weighted.class);
 		conf.setJobName("SSSP_weighted");
 		conf.setLoopInputOutput(SSSPLoopInputOutput.class);
        conf.setLoopReduceCacheSwitch(SSSPReduceCacheSwitch.class);
        conf.setLoopReduceCacheFilter(SSSPReduceCacheFilter.class);
        conf.setLoopStepHook(SSSPStepHook.class);
        conf.setInputPath(inputPath);
        conf.setOutputPath(outputPath);
        
        
        int specIteration = 2;
 		conf.setStepConf(0, conf1);
        conf.setStepConf(1, conf2);
        conf.setIterative(true);
        conf.setNumIterations(specIteration);
        
        JobClient.runJob(conf);
        
        
        
    }
    
    
    
    
}




