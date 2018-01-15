package zulu.pagerank.sort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import zulu.pagerank.Node;

public class Sort {
	public void sort(String[] args, int numReducer) throws Exception {
		
		System.out.println("\n\n============ Do sorting now ============\n");
		
		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf, "Sort");
		job.setJarByClass(Sort.class);
		
		job.setInputFormatClass(KeyValueTextInputFormat.class);

		// set the class of each stage in mapreduce
		job.setMapperClass(SortMapper.class);
		job.setPartitionerClass(SortPartitioner.class);
		job.setReducerClass(SortReducer.class);

		// set the output class of Mapper and Reducer
		job.setMapOutputKeyClass(Node.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		
		// set the number of reducer
		job.setNumReduceTasks(numReducer);

		// add input/output path
		FileInputFormat.addInputPath(job, new Path(args[4]));
		FileOutputFormat.setOutputPath(job, new Path(args[6]));

		job.waitForCompletion(true);
		
	}
}
