package zulu.pagerank.parse;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Parse {
	public long parse(String[] args, int numReducer) throws Exception {
		
		System.out.println("\n\n============ Do parsing now ============\n");
		
		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf, "Parse");
		job.setJarByClass(Parse.class);

		// set the class of each stage in mapreduce
		job.setMapperClass(ParseMapper.class);
		job.setCombinerClass(ParseCombiner.class);
		job.setReducerClass(ParseReducer.class);

		// set the output class of Mapper and Reducer
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		// set the number of reducer
		job.setNumReduceTasks(numReducer);

		// add input/output path
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
		
		// get distinct key number (N)
		Counters counters = job.getCounters();
		return counters.findCounter("org.apache.hadoop.mapreduce.TaskCounter", "MAP_INPUT_RECORDS").getValue();
	}
}
