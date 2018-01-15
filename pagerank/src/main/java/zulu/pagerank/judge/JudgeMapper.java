package zulu.pagerank.judge;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class JudgeMapper extends Mapper<Text, Text, Text, DoubleWritable> {
	private static String PREFIX = "&amp;";
	private static String ERROR = "&amp;error";
	
	private DoubleWritable result = new DoubleWritable();
	
	public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
	
		if (isError(key)) {
			String[] split_line = value.toString().split(PREFIX);
			
			double PR = Double.parseDouble(split_line[0]);
			result.set(PR);
			
			context.write(key, result);
		}
	}
	
	public boolean isError(Text input) {
		return input.toString().equals(ERROR);
	}
}
