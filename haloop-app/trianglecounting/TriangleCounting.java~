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



public class TriangleCounting
{
	public static final String EDGE = "EDGE";
	
	public static class TCMapper extends MapReduceBase implements
			Mapper<Object, Text, IntWritable, Text> {
		
		public void map(Object key, Text value, OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException {
			
			//vid neighbors_num n1 n2 ...
			String[] tokens = value.toString().split("\\s+");
			IntWritable SourceId = new IntWritable(Integer.parseInt(tokens[0]));
			
			//output <adj>
			StringBuilder sb = new StringBuilder();
			for (int i = 1; i < tokens.length; i++){
				if (sb.length() != 0) sb.append(" ");
				sb.append(tokens[i]);
			}
			sb.append(EDGE);
			output.collect(SourceId, new Text(sb.toString()));
			
			//output <ni, nj>
			for (int i = 2; i < tokens.length; i++)
			{
				for (int j = i+1; j < tokens.length; j++)
				{
					output.collect(new IntWritable(Integer.parseInt(tokens[i])), new Text(tokens[j]));
				}
			}
		}
	}
	
	public static class TCReducer extends MapReduceBase implements
            Reducer<IntWritable, Text, IntWritable, IntWritable> { 			
            
        public void reduce(IntWritable key, Iterator<Text> values, OutputCollector<IntWritable, IntWritable> output, Reporter reporter) throws IOException {
        	
        	ArrayList<Integer> neighbors = new ArrayList<Integer>();
        	ArrayList<Integer> candidates = new ArrayList<Integer>();
        	while (values.hasNext()){
        		String str = values.next().toString();
        		if (str.endsWith(EDGE)){
        			//adj table
        			String[] tmp = str.substring(0, str.length()-4).split("\\s+");
        			for (String i : tmp)
	        			neighbors.add(Integer.parseInt(i));
        		}
        		else{
        			//candidates
        			candidates.add(Integer.parseInt(str));
        		}
        	}
        	Collections.sort(neighbors);
        	int res = 0;
        	for (Integer tmp : candidates) {
        		int index = Collections.binarySearch(neighbors, tmp);
        		if (index >= 0) res ++;
        	}
        	output.collect(key, new IntWritable(res));
        }   
    }
	
	
    
    public static void main(String[] args) throws Exception {
    	
        String inputPath = args[0];
        String outputPath = args[1];
        //set number of reducers
        int numReducers = 2*16;
        
        //step 1
        JobConf conf0 = new JobConf(TriangleCounting.class);
        conf0.setJobName("TriangleCounting");
        
        conf0.setOutputKeyClass(IntWritable.class);
        conf0.setOutputValueClass(IntWritable.class);
        conf0.setMapOutputKeyClass(IntWritable.class);
        conf0.setMapOutputValueClass(Text.class);
        
        conf0.setMapperClass(TCMapper.class);
 		conf0.setReducerClass(TCReducer.class);
 		conf0.setInputFormat(TextInputFormat.class);
 		conf0.setOutputFormat(TextOutputFormat.class);
 		FileInputFormat.addInputPath(conf0, new Path(args[0]));
	    FileOutputFormat.setOutputPath(conf0, new Path(args[1]+"/i-1"));
	    
	    conf0.setNumReduceTasks(numReducers);
	    JobClient.runJob(conf0);
    }
}




