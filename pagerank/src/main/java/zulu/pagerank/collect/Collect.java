package zulu.pagerank.collect;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import zulu.pagerank.Node;

public class Collect {
	public long collectDanglingNodePR(String[] args, int numReducer) throws Exception {
		
		System.out.println("\n\n============ Do collecting dangling now ============\n");
		
		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf, "Collect");
		job.setJarByClass(Collect.class);
		
		job.setInputFormatClass(KeyValueTextInputFormat.class);

		// set the class of each stage in mapreduce
		job.setMapperClass(CollectMapper.class);
		job.setReducerClass(CollectReducer.class);

		// set the output class of Mapper and Reducer
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Node.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		// set the number of reducer
		job.setNumReduceTasks(numReducer);

		// add input/output path
		FileInputFormat.addInputPath(job, new Path(args[4]));
		FileOutputFormat.setOutputPath(job, new Path(args[3]));

		job.waitForCompletion(true);
		
		System.out.println("!!! " + job.getCounters().findCounter("zulu.pagerank.collect.CollectReducer$COUNTERS", "DANGLING").getValue());
		return job.getCounters().findCounter("zulu.pagerank.collect.CollectReducer$COUNTERS", "DANGLING").getValue();
	}
}
