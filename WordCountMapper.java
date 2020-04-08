import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapreduce.Mapper;

// here we set the map to contain the following:
// LongWritable: record id
// Text: ?
// Text: word
// Text: document identifier
public class WordCountMapper extends Mapper<LongWritable, Text, Text, Text>
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