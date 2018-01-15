package zulu.pagerank.rank;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import zulu.pagerank.Node;

public class RankMapper extends Mapper<Text, Text, Text, Node> {
	private static String PREFIX = "&gt;";
	private ArrayList<Text> neighbor = new ArrayList<Text>();
	private Node msg;
	private Node self;
	
	public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
		
		String[] split_line = value.toString().split(PREFIX);
		
		double prePR = Double.parseDouble(split_line[0]);
		
		if (split_line.length > 1) {
			double propagatePR = prePR / (split_line.length - 1);
			
			msg = new Node(propagatePR, new Text(), false);
			
			// propagate PR info to neighbors
			for (int i = 1; i < split_line.length; i++) {
				if (neighbor.size() <= i-1)
					neighbor.add(new Text(split_line[i]));
				else
					neighbor.set(i-1, new Text(split_line[i]));
				
				context.write(neighbor.get(i-1), msg);
			}
		}
		
		// pass self info
		self = new Node(prePR, value, true);
		context.write(key, self);
	}
}
