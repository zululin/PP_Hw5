package zulu.pagerank.collect;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import zulu.pagerank.Node;

public class CollectMapper extends Mapper<Text, Text, Text, Node> {
	private static String PREFIX = "&amp;";
	private static String ORACLE = "&amp;oracle";
	private static String ERROR = "&amp;error";
	private Text oracleNode = new Text(ORACLE);
	private Node msg;
	private Node self;
	
	public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
		String[] split_line = value.toString().split(PREFIX);
		
		double prePR = Double.parseDouble(split_line[0]);
		
		if (!isError(key)) {
			// Dangling node PR pass to oracle node	
			if (split_line.length == 1) {
				msg = new Node(prePR, new Text(), false);
				context.write(oracleNode, msg);
			}
			
			// pass self info
			self = new Node(prePR, new Text(value), true);
			context.write(key, self);
		}
	}
	
	public boolean isError(Text input) {
		return input.toString().equals(ERROR);
	}
}
