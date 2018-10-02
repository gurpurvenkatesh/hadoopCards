package cards;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class RecordReducer extends Reducer<Text, IntWritable, NullWritable, IntWritable> {

	public void reduce(Text key, Iterable<IntWritable> records, Context context) throws IOException, InterruptedException{	// Reducer value input is always an array of values. So Iterable array is being used.
		
		int sum = 0;
		for (IntWritable record : records){
			sum += record.get();
		}
		
		context.write(NullWritable.get(), new IntWritable(sum));
	}
}
