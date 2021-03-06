import java.io.IOException;
import java.util.HashMap;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class WordCount {
	static class WordCountReducer extends Reducer<Text, Text, Text, IntWritable> {
		private static final Logger logger = LogManager.getLogger(WordCountReducer.class);

		public void reduce(Text key, Iterable<Text> docs, Context context) throws IOException, InterruptedException {
			/* We will store a map of words : wordcount in this hashmap */
			HashMap<Text, Integer> occurences = new HashMap<Text, Integer>();
			for (Text docId : docs) {
				Text k = new Text(key.toString() + "\t" + docId.toString());
				/*
				 * Either we will update an already existing document ID in the hashmap, or
				 * create a new one with with just '1' as the value
				 */
				if (occurences.containsKey(k)) {
					occurences.put(k, occurences.get(k) + 1);
				} else {
					occurences.put(k, 1);
				}
			}

			/*
			 * For each key : value of documentId : count, we will write (word, documentId,
			 * count) to the context
			 */
			occurences.forEach((Text k, Integer v) -> {
				try {
					context.write(k, new IntWritable(v));
				} catch (Exception e) {
					/*
					 * An IO Exception could be thrown here because a file write is happening, or an
					 * interrupted exception if a SIGINT occurs
					 */
					logger.error("An error occurred in the context writting portion of the reducer.", e);
				}
			});
		}
	}

	static class WordCountMapper extends Mapper<LongWritable, Text, Text, Text> {
		/**
		 * @param args the command line arguments
		 */
		private Text word = new Text();

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString().toLowerCase();
			/*
			 * Remove punctionation from line before tokenizing, so that single-term tokens
			 * don't get clobbered
			 */
			line = line.replaceAll("\\p{Punct}", "");
			StringTokenizer tokenizer = new StringTokenizer(line);

			/*
			 * Retreive file name to use as docId, we do this here so that we don't
			 * contantly overload I/O
			 */
			Text docId = new Text(((FileSplit) context.getInputSplit()).getPath().getName());

			while (tokenizer.hasMoreTokens()) {
				word.set(tokenizer.nextToken());
				/*
				 * Write a pair of word : document to the context, which will be reduced later
				 */
				context.write(word, docId);
			}
		}
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Logger logger = LogManager.getLogger(WordCount.class);
		if (args.length != 2) {
			logger.error("Usage: java WordCount <input path> <output path>");
			System.exit(-1);
		}
		logger.info("Input: "+args[0]+" Ouput: "+args[1]);
		Job job = new Job(new Configuration(), "wordcount");
		job.setJarByClass(WordCount.class);
		job.setJobName("WordCount");

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(WordCountMapper.class);
		job.setReducerClass(WordCountReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.waitForCompletion(true);
	}
}