package zulu.pagerank.prune;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;

public class Prune {
	public void prune(String[] args, long N, int numReducer) throws Exception {
		
		System.out.println("\n\n============ Do pruning now ============\n");
		
		Configuration conf = new Configuration();
		conf.setLong("#N", N);

		Job job = Job.getInstance(conf, "Prune");
		job.setJarByClass(Prune.class);
		
		job.setInputFormatClass(KeyValueTextInputFormat.class);

		// set the class of each stage in mapreduce
		job.setMapperClass(PruneMapper.class);
		job.setCombinerClass(PruneCombiner.class);
		job.setReducerClass(PruneReducer.class);

		// set the output class of Mapper and Reducer
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		// set the number of reducer
		job.setNumReduceTasks(numReducer);

		// add input/output path
		FileInputFormat.addInputPath(job, new Path(args[1]));
		FileOutputFormat.setOutputPath(job, new Path(args[4]));

		job.waitForCompletion(true);
	}
}
