import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Lemma_A1 {

	public static class LemmaMapper
	extends Mapper<Object, Text, Text, Text>{
		Map<String, ArrayList> lemma_map = new HashMap<String, ArrayList>();
		protected void setup(Context context) throws IOException, InterruptedException {
			String filename = "la.lexicon.csv";
			BufferedReader br = new BufferedReader(new FileReader(filename));
			String line = br.readLine();
			String[] words = line.split(",");
			ArrayList<String> lemmas = new ArrayList<String>();
			if(!lemma_map.containsKey(words[0])){
				lemmas.add(words[2]);
				lemma_map.put(words[0], lemmas);
			}
			else{
				lemmas = lemma_map.get(words[0]);
				lemmas.add(words[2]);
				lemma_map.put(words[0], lemmas);
			}
		}

		public void map(Object key, Text value, Context context
				) throws IOException, InterruptedException {
			String[] data = value.toString().split(">");
			if(data.length > 1){
			String docDetails = data[0];
			String chapter = null;
			String line = null;
			String docId = null;
			String[] others = null;
			docDetails = docDetails.replaceAll("<", "");
			String[] details = docDetails.split(" ");
			StringBuilder sb = new StringBuilder();
			for(int i = 0; i < details.length -1; i++){
				sb.append(details[i]);
			}
			if(sb != null && sb.length() > 0){
				docId = sb.toString();
			}
			if(details.length > 0){
				others = details[details.length-1].split("\\.");
				
			}
			sb = new StringBuilder();
			for(int i =0; i < others.length-1; i++){
				sb.append(others[i]);
			}
			if(sb != null && sb.length() > 0){
				chapter = sb.toString();
			}
			if(others.length > 0){
				line = others[others.length-1];
			}
			docDetails = "{ docId = " +docId+ " chapter = " +chapter+ " line = " +line+ " }";
			String content = data[1];
			content = content.replaceAll("[^a-zA-Z0-9 ]", "");
			Text location = new Text();
			location.set(docDetails);
			String[] tokens = content.split(" ");
			for(int i =0; i < tokens.length; i++){
				Text word = new Text();
				String token = tokens[i];
				token = token.trim();
				if(token == null || token.length() == 0){
					continue;
				}
				//	normalization
				token = token.replaceAll("j", "i");
				token = token.replaceAll("v", "u");
				word.set(token);
				context.write(word, location);
				if(lemma_map.containsKey(token)){
					ArrayList<String> lemma_list = lemma_map.get(token);
					Map<String, Integer> map = new HashMap<String, Integer>();
					for(String keys: lemma_list){
						map.put(keys, i);
						word.set(keys);
						context.write(word, location);
					}		
				}
			}
			}
		}
	}

	public static class LemmaReducer extends Reducer<Text,Text,Text,Text> {
		public void reduce(Text key, Iterable<Text> values,
				Context context
				) throws IOException, InterruptedException {
			StringBuilder position = new StringBuilder();
			Text text = new Text();
			int count = 0;
			for (Text val : values) {
				position.append(val);
				position.append(" ");
				count++;
			}
			position.append("Count :" +count);
			String value = position.toString().trim();
			text.set(value);
			context.write(key, text);
		}
	}
	
	 public static void main(String[] args) throws Exception {
		    Configuration conf = new Configuration();
		    Job job = Job.getInstance(conf, "Lemmatization");
		    job.setJarByClass(Lemma_A1.class);
		    job.setMapperClass(LemmaMapper.class);
//		    job.setCombinerClass(LemmaReducer.class);
		    job.setReducerClass(LemmaReducer.class);
		    job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(Text.class);
		    FileInputFormat.addInputPath(job, new Path(args[0]));
		    FileOutputFormat.setOutputPath(job, new Path(args[1]));
		    System.exit(job.waitForCompletion(true) ? 0 : 1);
		  }
}
