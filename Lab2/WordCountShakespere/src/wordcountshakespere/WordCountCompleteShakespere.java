package wordcountshakespere;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;
//import java.util.List;
//import java.util.ArrayList;
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

/**
 * WordCountCompleteShakespere:
 * This is a complete Hadoop MapReduce program that performs a word count on text input
 * (e.g., Shakespeare's complete works) while implementingstop word filtering using
 * theDistributed Cache It extends Configured and implements Tool for robust
 * command-line execution via ToolRunner.
 */
public class WordCountCompleteShakespere extends Configured implements Tool{
	/**
	 * run:
	 * Configures and executes the MapReduce job.
	 * @param args Contains the HDFS input path (args[0]) and output path (args[1]).
	 * @return 0 on success, 1 on failure.
	 * @throws Exception for job configuration or execution errors.
	 */
	public int run(String[] args) throws Exception {
		// 1. Job Initialization: Create a new job instance.
		Job  job = Job.getInstance(getConf(), "WordCountCompleteShakespere");
		Configuration conf = job.getConfiguration();
		job.setJarByClass(getClass());
		
		Path in = new Path(args[0]);
		Path out= new Path(args[1]);
		//remember to delete the output folder here.
		// 2. Input/Output Paths: Set the HDFS input and output directories.
		FileInputFormat.setInputPaths(job, in);
		FileOutputFormat.setOutputPath(job, out);
		
		// 3. Mapper, Reducer, and Parallelism: Assign custom classes and set 3 reducers.
		job.setMapperClass(StopWordMapper.class);
		job.setReducerClass(StopWordReducer.class);
		job.setNumReduceTasks(3);
		
		// 4. Distributed Cache Setup:
		// Distribute the stop words file. The fragment "#stop_words.txt" provides the local filename.
		URI stopWordsURI = new URI("/user/cloudera/stop_words.txt"+"#stop_words.txt");
		job.addCacheFile(stopWordsURI);
		
		// 5. I/O Types: Define the key/value types for Map output and final job output.
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		// 6. I/O Formats: Set the standard text input and output formats.
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		// 7. Execution: Submit the job and wait for completion.
		return job.waitForCompletion(true)?0:1;
	}

	/**
	 * StopWordMapper:
	 * A custom Mapper that tokenizes text, normalizes words, and filters out common stop words.
	 * It reads the stop word list from the Distributed Cache in the setup method.
	 */
	private static class StopWordMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
		// Declare a set to hold the stop words for fast lookups (0(1) average time)
		private final Set<String> stopWords = new HashSet<>();
		
		// Standard Mapper fields for output
		private static final IntWritable ONE = new IntWritable(1);
		private Text outputKey = new Text();
		
		/**
		 * setup:
		 * Called once at the beginning of the task. It loads the stop words file
		 * that was distributed to the node via the Distributed Cache.
		 */
		protected void setup (Context context) throws IOException, InterruptedException{
			URI[] cacheFiles=context.getCacheFiles();
			
			if (cacheFiles!=null & cacheFiles.length>0){
				URI stopWordsURI = cacheFiles[0];
				String localFileName;
				// Determine the local filename using the URI fragment (if present).
				if (stopWordsURI.getFragment()!=null){
					localFileName = stopWordsURI.getFragment();
				}else{
					String path = stopWordsURI.getPath();
					localFileName = new File(path).getName();
				}
				File stopWordsFile=new File(localFileName);
				if (stopWordsFile.exists()){
					// Read the file and populate the stopWords set.
					try(BufferedReader reader = new BufferedReader(new FileReader(stopWordsFile))){
						String line;
						while((line = reader.readLine())!=null){
							String[] tokens = line.split("[^a-zA-Z]+"); 
            
							for (String stopWord : tokens) {
								// Normalize, check if not empty, and add each individual word to the set
								if (!stopWord.isEmpty()) {
									stopWords.add(stopWord.toLowerCase().trim());
								}
						}
					}
					}
					catch (Exception e){
						throw new IOException("Failed to read stop_words.txt from local cache:" + e.getMessage(),e);
					}
				} else{
					throw new IOException("Cached file '"+localFileName+"' not found in local working directory.");
				}
				//List<String> myList = new ArrayList<>(stopWords);

				// Access and print the element at index 0 (the first element in the list)
				//System.out.println("STOP_WORD: "+myList.get(0));
				
			}
		}
		
		/**
		 * map:
		 * Processes a single line of text.
		 */
		public void map(LongWritable key, Text value, Context context)
						throws IOException, InterruptedException{
			String line = value.toString();
		
		   // 1. Tokenization: Split the line by any character that is NOT an alphabet (a-z, A-Z).
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
	
	/**
	 * StopWordReducer:
	 * A standard Reducer that sums the counts for all intermediate key-value pairs (word, 1).
	 */
	private static class StopWordReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
		private IntWritable outputValue = new IntWritable();

		/**
		 * reduce:
		 * Sums all the '1' counts for a given word.
		 * @param key The word.
		 * @param values An iterable collection of IntWritable(1)s.
		 */
		protected void reduce(Text key, Iterable<IntWritable> values, Context context)
							 throws IOException, InterruptedException{
			int sum =0;
			
			for(IntWritable count :values){
				sum+=count.get();
			}
			// Emit the final word and its total count.
			outputValue.set(sum);
			context.write(key,outputValue);
		}
	}

	/**
	 * main:
	 * The entry point for the job. It uses ToolRunner to execute the job,
	 * ensuring proper configuration handling.
	 */
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
