import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;


public class WordOccurence {
	public static class PairsOccurrenceMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	    private IntWritable ONE = new IntWritable(1);

	    @Override
	    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	        String[] tokens = value.toString().split("\\s+");
	        if (tokens.length > 1) {
	          for (int i = 0; i < tokens.length-1; i++) {
	        	  String a = tokens[i].replaceAll("[^a-zA-Z0-9 ]", "");
	        	  if(a.length() == 0)
	        		  continue;
	               for (int j = i+1; j < tokens.length ; j++) {
	            	   String b = tokens[j].replaceAll("[^a-zA-Z0-9 ]", "");
	            	   if(b.length() == 0)
	            		   continue;
	            	   StringBuilder wordPair = new StringBuilder();  
	                   wordPair.append("[word = ").append(a)
	                   		.append(", neighbor = ").append(b).append("]");
	                   context.write(new Text(wordPair.toString()), ONE);
	              }
	          }
	      }
	  }
	}
	
	public static class PairsReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
	    private IntWritable totalCount = new IntWritable();
	    @Override
	    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
	        int count = 0;
	        for (IntWritable value : values) {
	             count += value.get();
	        }
	        totalCount.set(count);
	        context.write(key,totalCount);
	    }
	}
	
	public static class StripesOccurrenceMapper extends Mapper<LongWritable,Text,Text,MyMapWritable> {
		  private MyMapWritable occurrenceMap = new MyMapWritable();
		  private Text word = new Text();

		  @Override
		 protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		   int neighbors = context.getConfiguration().getInt("neighbors", 2);
		   String[] tokens = value.toString().split("\\s+");
		   if (tokens.length > 1) {
		      for (int i = 0; i < tokens.length; i++) {
		          word.set(tokens[i]);
		          occurrenceMap.clear();

		          int start = (i - neighbors < 0) ? 0 : i - neighbors;
		          int end = (i + neighbors >= tokens.length) ? tokens.length - 1 : i + neighbors;
		           for (int j = start; j <= end; j++) {
		                if (j == i) continue;
		                Text neighbor = new Text(tokens[j]);
		                if(occurrenceMap.containsKey(neighbor)){
		                   IntWritable count = (IntWritable)occurrenceMap.get(neighbor);
		                   count.set(count.get()+1);
		                }else{
		                   occurrenceMap.put(neighbor,new IntWritable(1));
		                }
		           }
		          context.write(word,occurrenceMap);
		     }
		   }
		  }
		}
	
	public static class StripesReducer extends Reducer<Text, MyMapWritable, Text, MyMapWritable> {
	    private MyMapWritable incrementingMap = new MyMapWritable();

	    @Override
	    protected void reduce(Text key, Iterable<MyMapWritable> values, Context context) throws IOException, InterruptedException {
	        incrementingMap.clear();
	        for (MyMapWritable value : values) {
	            addAll(value);
	        }
	        context.write(key, incrementingMap);
	    }

	    private void addAll(MyMapWritable mapWritable) {
	        Set<Writable> keys = mapWritable.keySet();
	        for (Writable key : keys) {
	            IntWritable fromCount = (IntWritable) mapWritable.get(key);
	            if (incrementingMap.containsKey(key)) {
	                IntWritable count = (IntWritable) incrementingMap.get(key);
	                count.set(count.get() + fromCount.get());
	            } else {
	                incrementingMap.put(key, fromCount);
	            }
	        }
	    }
	}
	
	public static class MyMapWritable extends MapWritable{
		
	    @Override
	    public String toString() {
		        StringBuilder result = new StringBuilder();
		        Set<Writable> keySet = this.keySet();

		        for (Object key : keySet) {
		            result.append("{" + key.toString() + " = " + this.get(key) + "}");
		        }
		        return result.toString();
		}
		
	}

public static void main(String[] args) throws Exception {
    Job job = Job.getInstance(new Configuration());
    job.setJarByClass(WordOccurence.class);
    if (args[0].equalsIgnoreCase("pairs")) {
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        doMapReduce(job, args[1], PairsOccurrenceMapper.class, args[2], "pairs-co-occur", PairsReducer.class);
    } else if (args[0].equalsIgnoreCase("stripes")) {
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(MyMapWritable.class);
        doMapReduce(job, args[1], StripesOccurrenceMapper.class, args[2], "stripes co-occur", StripesReducer.class);
    }

}


private static void doMapReduce(Job job, String path, Class<? extends Mapper> mapperClass, String outPath, String jobName, Class<? extends Reducer> reducerClass) throws Exception {
    try {
        job.setJobName(jobName);
        FileInputFormat.addInputPath(job, new Path(path));
        FileOutputFormat.setOutputPath(job, new Path(outPath));
        job.setMapperClass(mapperClass);
        job.setReducerClass(reducerClass);
        job.setCombinerClass(reducerClass);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    } catch (Exception e) {
      	e.printStackTrace();
    }
}
	 
}
