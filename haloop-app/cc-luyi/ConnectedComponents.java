import java.io.*;
 
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
 
public class ConnectedComponents {
 
      /*
      * vid\tcolor changed? u1 u2 u3
      */
     
 
      public static final String EGDE = "EDGE ";
      public static final String COLOR = "COLOR ";
      public static final String NEWCOLOR = "NEWCOLOR ";
 
      static enum CCUpdates {
            UPDATE
      };
 
      public static class FirstMapper extends
                  Mapper<Object, Text, IntWritable, Text> {
 
            public void map(Object key, Text value, Context context)
                        throws IOException, InterruptedException {
                  //IntWritable SourceId = new IntWritable(Integer.parseInt(key
                  //            .toString()));
 
                  String[] tokens = value.toString().split("\\s+");
                  
                  IntWritable SourceId = new IntWritable(Integer.parseInt(tokens[0]));
 
                  int color = Integer.parseInt(tokens[1]);
 
                  for (int i = 3; i < tokens.length; i++) {
                        color = Math.min(color, Integer.parseInt(tokens[i]));
                  }
 
                  context.write(SourceId, new Text(COLOR + color));
 
                  boolean changed = tokens[2].equals("1");
 
                  StringBuilder sb = new StringBuilder();
 
                  for (int i = 3; i < tokens.length; i++) {
                        if (sb.length() != 0) {
                              sb.append(" ");
                        }
 
                        sb.append(tokens[i]);
 
                        IntWritable TargetId = new IntWritable(
                                    Integer.parseInt(tokens[i].toString()));
 
                        if (changed) {
                              context.write(TargetId, new Text(NEWCOLOR + color));
                        }
                  }
 
                  context.write(SourceId, new Text(EGDE + sb.toString()));
            }
      }
 
      public static class FinalMapper extends
                  Mapper<Object, Text, IntWritable, Text> {
 
            public void map(Object key, Text value, Context context)
                        throws IOException, InterruptedException {
 
                  String[] tokens = value.toString().split("\\s+");
                  
                  IntWritable SourceId = new IntWritable(Integer.parseInt(tokens[0]));
 
                  context.write(SourceId, new Text(COLOR + tokens[1]));
 
                  boolean changed = tokens[2].equals("1");
 
                  StringBuilder sb = new StringBuilder();
 
                  for (int i = 3; i < tokens.length; i++) {
                        if (sb.length() != 0) {
                              sb.append(" ");
                        }
 
                        sb.append(tokens[i]);
 
                        IntWritable TargetId = new IntWritable(
                                    Integer.parseInt(tokens[i].toString()));
 
                        if (changed) {
                              context.write(TargetId, new Text(NEWCOLOR + tokens[0]));
                        }
                  }
 
                  context.write(SourceId, new Text(EGDE + sb.toString()));
            }
      }
 
      public static class FinalReducer extends
                  Reducer<IntWritable, Text, IntWritable, Text> {
 
            public void reduce(IntWritable key, Iterable<Text> values,
                        Context context) throws IOException, InterruptedException {
                  int minColor = Integer.MAX_VALUE;
                  int color = -1;
                  String edge = null;
                  boolean changed = false;
                  for (Text text : values) {
                        String str = text.toString();
                        if (str.startsWith(COLOR)) {
                              color = Integer.parseInt(str.substring(COLOR.length()));
                        } else if (str.startsWith(NEWCOLOR)) {
                              int newcolor = Integer.parseInt(str.substring(NEWCOLOR
                                          .length()));
                              minColor = Math.min(minColor, newcolor);
                        } else if (str.startsWith(EGDE)) {
                              edge = str.substring(EGDE.length());
                        }
                  }
                  assert (color != -1);
                  assert (edge != null);
 
                  if (minColor < color) {
                        color = minColor;
 
                        changed = true;
                  }
 
                  StringBuilder sb = new StringBuilder();
 
                  if (changed) {
                        context.getCounter(CCUpdates.UPDATE).increment(1);
                        sb.append(minColor);
                        sb.append(" 1 ");
                  } else {
                        sb.append(color);
                        sb.append(" 0 ");
 
                  }
 
                  sb.append(edge);
 
                  context.write(key, new Text(sb.toString()));
 
            }
 
      }
 
      public static class FinalCombiner extends
                  Reducer<IntWritable, Text, IntWritable, Text> {
            public void reduce(IntWritable key, Iterable<Text> values,
                        Context context) throws IOException, InterruptedException {
                  int minColor = Integer.MAX_VALUE;
                  for (Text text : values) {
                        String str = text.toString();
                        if (str.startsWith(COLOR)) {
                              context.write(key, text);
                        } else if (str.startsWith(NEWCOLOR)) {
                              int newcolor = Integer.parseInt(str.substring(NEWCOLOR
                                          .length()));
                              minColor = Math.min(minColor, newcolor);
                        } else if (str.startsWith(EGDE)) {
                              context.write(key, text);
                        }
                  }
                  if (minColor != Integer.MAX_VALUE) {
                        context.write(key, new Text(NEWCOLOR + minColor));
                  }
            }
 
      }
 
      public static void main(String[] args) throws Exception {
            Configuration conf = new Configuration();
 
            String[] otherArgs = new GenericOptionsParser(conf, args)
                        .getRemainingArgs();
 
            if (otherArgs.length != 2) {
                  System.err.println("Args: <in> <out>");
                  System.exit(2);
            }
 
            boolean hasUpdate = true;
 
            int step = 0;
            while (hasUpdate) {
                  Job job = new Job(conf, "Connected Components " + step);
 
                  job.setJarByClass(ConnectedComponents.class);
                  if (step == 0)
                        job.setMapperClass(FirstMapper.class);
                  else
                        job.setMapperClass(FinalMapper.class);
 
                  job.setReducerClass(FinalReducer.class);
                  job.setCombinerClass(FinalCombiner.class);
                  
                  job.setMapOutputKeyClass(IntWritable.class);
                  job.setMapOutputValueClass(Text.class);
 
                  job.setOutputKeyClass(IntWritable.class);
                  job.setOutputValueClass(Text.class);
                  job.setNumReduceTasks(3);
 
                  job.setInputFormatClass(TextInputFormat.class);
 
                  if (step == 0) {
                        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
                        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1] + "_"
                                    + step));
                  } else {
                        FileInputFormat.addInputPath(job, new Path(otherArgs[1] + "_"
                                    + (step - 1)));
                        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1] + "_"
                                    + step));
                  }
 
                  job.waitForCompletion(true);
                  step += 1;
 
                  hasUpdate = job.getCounters().findCounter(CCUpdates.UPDATE)
                              .getValue() > 0;
            }
 
      }
}
