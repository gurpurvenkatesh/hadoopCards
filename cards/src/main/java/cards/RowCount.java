package cards;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class RowCount extends Configured implements Tool {


	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		
		Job job = Job.getInstance(getConf(), "Row Count using built in mapers & reducers"); // Gets the configuration files to run the Hadoop cluster.
		job.setJarByClass(getClass()); 	// Pass that class name which contains the Mapper & Reducer function.
			
		FileInputFormat.setInputPaths(job, new Path(args[0]));	// Setting first command line argument as input file name. This command line argument is given during mapreduce job execution 
		// job.setInputFormatClass(TextInputFormat.class);		// Setting the format of input as Text
		
		job.setMapperClass(RecordMapper.class);
		job.setMapOutputKeyClass(Text.class);	// Setting Output key format. Text is serialization alternative for primitive type(String).
		job.setMapOutputValueClass(IntWritable.class);	// Setting Output value format. IntWritable is serialization alternative for primitive type(int).
		/*
		 * Mapper output
		 * <count, 1>
		 * <count, 1> so on
		 */
		
		job.setReducerClass(RecordReducer.class);
		job.setOutputKeyClass(NullWritable.class);	// Setting Reducer Output key format. Text is serialization alternative for primitive type(String).
		job.setOutputValueClass(IntWritable.class);	// Setting Reducer Output value format. IntWritable is serialization alternative for primitive type(int).
		
		// job.setOutputFormatClass(TextInpuFormat.class);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));	// Setting second command line argument as output file name. This command line argument is given during mapreduce job execution
		
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub

		int exitCode = ToolRunner.run(new RowCount(), args); // Need to mention the class which contains the job configuration. In this case self-class contains all the job configuration.
		System.exit(exitCode);
	}

}
