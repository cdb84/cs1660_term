import java.io.IOException;
import java.util.HashMap;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCount {
    static class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable>
{
    public void reduce(Text key, Iterable<Text> docs, Context context)
        throws IOException, InterruptedException
    {
        HashMap<Text, Integer> docCount = new HashMap<Text, Integer>();

        for (Text docId : docs) {
            if(docCount.containsKey(docId)){
                Integer x = docCount.get(docId);
                docCount.put(docId, x+1);
            }else{
                docCount.put(docId, 1);
            }
        }

        docCount.forEach((Text k, Integer v) -> {
            try{
                context.write(k, new IntWritable(v));
            }
            catch (Exception e){
                System.out.println("An error occurred in the context writting portion of the reducer.");
            }
        });
    }
    }

    static class WordCountMapper extends Mapper<LongWritable, Text, Text, Text>
    {
        /**
         * @param args the command line arguments
         */
        private Text word = new Text();

        public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException
        {
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line);

            while (tokenizer.hasMoreTokens()) {
                word.set(tokenizer.nextToken());
                // to the context, write an instance of this word : document key value pair
                context.write(word, new Text(((FileSplit)context.getInputSplit()).getPath().toString()));
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