package zulu.pagerank.build;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class BuildMapper extends Mapper<Text, Text, Text, Text> {
	
	private static String PREFIX = "&gt;";
	private static String ORACLE = "&gt;oracle";
	private Text oracleNode = new Text(ORACLE);
	private Text token = new Text(PREFIX);
//	private Text self = new Text();
	private ArrayList<Text> parent = new ArrayList<Text>();
	
	public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
		
//		self.set(key);
		String[] split_line = value.toString().split(PREFIX);
		
		// send ack back to parent
		for (int i = 1; i < split_line.length; i++) {
			if (parent.size() <= i-1)
				parent.add(new Text(split_line[i]));
			else
				parent.set(i-1, new Text(split_line[i]));
				
			context.write(parent.get(i-1), key);
		}
		
		// send self for dangling node
		context.write(key, token);
		
		// (oracleNode, self) for constructing whole list
		context.write(oracleNode, key);
	}
}
