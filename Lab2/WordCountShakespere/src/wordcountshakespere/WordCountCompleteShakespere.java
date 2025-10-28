package wordcountshakespere;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
//import java.io.IOException;
//import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WordCountCompleteShakespere extends Configured implements Tool{
	
	public int run(String[] args) throws Exception {
		Job  job = Job.getInstance(getConf(), "WordCountCompleteShakespere");
		Configuration conf = job.getConfiguration();
		job.setJarByClass(getClass());
		
		Path in = new Path(args[0]);
		Path out= new Path(args[1]);
		//remember to delete the output folder here.
		FileInputFormat.setInputPaths(job, in);
		FileOutputFormat.setOutputPath(job, out);
		
		job.setMapperClass(StopWordMapper.class);
		job.setReducerClass(StopWordReducer.class);
		job.setNumReduceTasks(3);
		
		URI stopWordsURI = new URI("/user/cloudera/stop_words.txt"+"#stop_words.txt");
		job.addCacheFile(stopWordsURI);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		return job.waitForCompletion(true)?0:1;
	}

	private static class StopWordMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
		// Declare a set to hold the stop words for fast lookups (0(1) average time)
		private final Set<String> stopWords = new HashSet<>();
		
		// Standard Mapper fields for output
		private static final IntWritable ONE = new IntWritable(1);
		private Text outputKey = new Text();
		
		protected void setup (Context context) throws IOException, InterruptedException{
			URI[] cacheFiles=context.getCacheFiles();
			
			if (cacheFiles!=null & cacheFiles.length>0){
				URI stopWordsURI = cacheFiles[0];
				String localFileName;
				if (stopWordsURI.getFragment()!=null){
					localFileName = stopWordsURI.getFragment();
				}else{
					String path = stopWordsURI.getPath();
					localFileName = new File(path).getName();
				}
				File stopWordsFile=new File(localFileName);
				if (stopWordsFile.exists()){
					try(BufferedReader reader = new BufferedReader(new FileReader(stopWordsFile))){
						String line;
						while((line = reader.readLine())!=null){
							stopWords.add(line.toLowerCase().trim());
						}
					}
					catch (Exception e){
						throw new IOException("Failer to read stop_words.txt from local cache:" + e.getMessage(),e);
					}
				} else{
					throw new IOException("Cached file '"+localFileName+"' not found in local working directory.");
				}
				
			}
		}
		public void map(LongWritable key, Text value, Context context)
						throws IOException, InterruptedException{
			String line = value.toString();
		    
		    // 1. Tokenization: Split the line by any character that is NOT an alphabet (a-z, A-Z).
		    // This is crucial for handling punctuation (like commas, periods, etc.) attached to words.
		    // The regular expression "[^a-zA-Z]+" means "one or more characters that are not a-z or A-Z".
		    String[] tokens = line.split("[^a-zA-Z]+"); 
		    
		    for (String word : tokens) {
		        
		        // 2. Initial Cleaning Check: Skip tokens that are empty (e.g., if multiple separators were together).
		        if (word.isEmpty()) {
		            continue; 
		        }
		        
		        // 3. Normalization: Convert the word to lowercase and trim any residual whitespace.
		        String cleanedWord = word.toLowerCase().trim();
		        
		        // 4. Final Filtering: Check if the word is still empty OR if it is in the stopWords Set.
		        if (!cleanedWord.isEmpty() && !stopWords.contains(cleanedWord)) {
		            
		            // 5. Emission: Emit the word as the key and a count of 1 as the value.
		            outputKey.set(cleanedWord);
		            context.write(outputKey, ONE);
		        }
		    }
		}
		
	}
	
	private static class StopWordReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
		private IntWritable outputValue = new IntWritable();

		protected void reduce(Text key, Iterable<IntWritable> values, Context context)
							 throws IOException, InterruptedException{
			int sum =0;
			
			for(IntWritable count :values){
				sum+=count.get();
			}
			outputValue.set(sum);
			context.write(key,outputValue);
		}
		
	}

	public static void main(String[] args) {
		int result=0;
		try{
			result = ToolRunner.run(new Configuration(),
								new WordCountCompleteShakespere(),
								args);
		} catch (Exception e ){
			e.printStackTrace();
		}
		System.exit(result);
		
	}


}
