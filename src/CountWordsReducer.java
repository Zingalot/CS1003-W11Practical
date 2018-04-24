

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CountWordsReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

	// The output of the reducer is a map from unique words to their total counts.

	public void reduce(Text key, Iterable<LongWritable> values, Context output) throws IOException, InterruptedException {

		// The key is the word.
		// The values are all the counts associated with that word (commonly one copy of '1' for each occurrence).

		int sum = 0;
		for(LongWritable value : values){
			long l = value.get();
			sum += l;
		}
		output.write(key, new LongWritable(sum));
	}
}
