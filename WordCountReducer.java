import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable>
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