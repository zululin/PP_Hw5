package zulu.pagerank.judge;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Judge {
	public long judgeError(String[] args) throws Exception {
		
		System.out.println("\n\n============ Do judging now ============\n");
		
		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf, "Judge");
		job.setJarByClass(Judge.class);
		
		job.setInputFormatClass(KeyValueTextInputFormat.class);

		// set the class of each stage in mapreduce
		job.setMapperClass(JudgeMapper.class);
		job.setCombinerClass(JudgeCombiner.class);
		job.setReducerClass(JudgeReducer.class);

		// set the output class of Mapper and Reducer
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// add input/output path
		FileInputFormat.addInputPath(job, new Path(args[4]));
		FileOutputFormat.setOutputPath(job, new Path(args[5]));

		job.waitForCompletion(true);
		
		System.out.println("!!! " + job.getCounters().findCounter("zulu.pagerank.judge.JudgeReducer$COUNTERS", "ERROR").getValue());
		
		return job.getCounters().findCounter("zulu.pagerank.judge.JudgeReducer$COUNTERS", "ERROR").getValue();
	}
}
