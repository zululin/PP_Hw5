package zulu.pagerank.sort;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import zulu.pagerank.Node;

public class SortMapper extends Mapper<Text, Text, Node, NullWritable> {
	private static String PREFIX = "&amp;";
	private static String ORACLE = "&amp;oracle";
	private static String ERROR = "&amp;error";
	
	private Node self;
	
	public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
		
		String[] split_line = value.toString().split(PREFIX);
		
		double PR = Double.parseDouble(split_line[0]);
		self = new Node(PR, new Text(key), true);
		
		if (!isOracle(key) && !isError(key))
			context.write(self, NullWritable.get());
	}
	
	public boolean isOracle(Text input) {
		return input.toString().equals(ORACLE);
	}
	
	public boolean isError(Text input) {
		return input.toString().equals(ERROR);
	}
}
