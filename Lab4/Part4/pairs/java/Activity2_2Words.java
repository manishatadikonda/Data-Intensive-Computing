import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Activity2_2Words {

	public static class PairsMapper extends Mapper<LongWritable, Text, Text, Text> {
		
		Map<String, ArrayList> lemma_map = new HashMap<String, ArrayList>();
		
		protected void setup(Context context) throws IOException, InterruptedException {
			String filename = "new_lemmatizer.csv";
			BufferedReader br = new BufferedReader(new FileReader(filename));
			String line = null;
			while((line = br.readLine()) != null){
			String[] words = line.split(",");
			if(words.length > 1){
			ArrayList<String> lemmas = new ArrayList<String>();
			for(int i = 1; i < words.length; i++){
				lemmas.add(words[i]);
			}
			lemma_map.put(words[0], lemmas);	
		}
			}
			br.close();
		}
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] data = value.toString().split(">");
			if(data.length > 1){
				String location = data[0] + ">" ;
				int neighbors = context.getConfiguration().getInt("neighbors", 2);
				String content = data[1];
				content = content.replaceAll("[^a-zA-Z0-9 ]", "");
				content = content.trim();
				String[] tokens = content.toString().split("\\s+");
				if (tokens.length > 1) {
					for (int i = 0; i < tokens.length; i++) {
						if(tokens[i] == null || tokens[i].length() == 0)
							continue;
						int start = (i - neighbors < 0) ? 0 : i - neighbors;
						int end = (i + neighbors >= tokens.length) ? tokens.length - 1 : i + neighbors;
						for (int j = start; j <= end; j++) {
							if (j == i) continue;
							if(tokens[j] == null || tokens[j].length() == 0) break;
							if(lemma_map.containsKey(tokens[i]))
								tokens[i] = lemma_map.get(tokens[i]).size() > 0? (String)lemma_map.get(tokens[i]).get(0) : tokens[i];
							if(lemma_map.containsKey(tokens[j]))
								tokens[j] = lemma_map.get(tokens[j]).size() > 0? (String)lemma_map.get(tokens[j]).get(0) : tokens[j];
							StringBuilder wordPair = new StringBuilder();
							wordPair.append("<").append(tokens[i]).append(",");
							wordPair.append(tokens[j]).append(">");
							context.write(new Text(wordPair.toString()), new Text(location));
						}
					}
				}
			}
		}
	}

	public static class PairsReducer extends Reducer<Text,Text,Text,Text> {
		
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			StringBuilder sb = new StringBuilder();
			for (Text value : values) {
				sb.append(value);
				sb.append(" ");
			}
			String value = sb.toString();
			if(value != null && value.length() > 0){
				value = value.trim();
				context.write(key,new Text(value));
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "latin pairs");
		job.setJarByClass(Activity2_2Words.class);
		job.setMapperClass(PairsMapper.class);
		job.setCombinerClass(PairsReducer.class);
		job.setReducerClass(PairsReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		String timeStamp = new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss").format(new Date());
		FileOutputFormat.setOutputPath(job, new Path(args[1] + "/" +timeStamp));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}