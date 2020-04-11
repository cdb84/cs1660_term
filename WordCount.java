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

public class WordCount {
	static class WordCountReducer extends Reducer<Text, Text, Text, IntWritable> {
		public void reduce(Text key, Iterable<Text> docs, Context context) throws IOException, InterruptedException {
			/* We will store a map of words : wordcount in this hashmap */
			HashMap<Text, Integer> docCount = new HashMap<Text, Integer>();
			for (Text docId : docs) {
				/* Either we will update an already existing document ID in the hashmap, or create a new one with
				with just '1' as the value */
				docCount.put(docId, (docCount.containsKey(docId) ? docCount.get(docId) + 1 : 1));
			}

			String strKey = key.toString()+"\t";
			/* For each key : value of documentId : count, we will write (word, documentId, count) to the context */
			docCount.forEach((Text k, Integer v) -> {
				try {
					context.write(new Text(strKey+k), new IntWritable(v));
				} catch (Exception e) {
					System.out.println("An error occurred in the context writting portion of the reducer.");
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
			/* Remove punctionation from line before tokenizing, so that single-term tokens don't get clobbered */
			line = line.replaceAll("\\p{Punct}", "");
			StringTokenizer tokenizer = new StringTokenizer(line);

			/* Retreive file name to use as docId, we do this here so that we don't contantly overload I/O */
			Text docId = new Text(((FileSplit) context.getInputSplit()).getPath().getName());

			while (tokenizer.hasMoreTokens()) {
				word.set(tokenizer.nextToken());
				/* Write a pair of word : document to the context, which will be reduced later */
				context.write(word, docId);
			}
		}
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		if (args.length != 2) {
			System.err.println("Usage: java WordCount <input path> <output path>");
			System.exit(-1);
		}

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