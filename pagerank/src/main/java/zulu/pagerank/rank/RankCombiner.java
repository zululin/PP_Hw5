package zulu.pagerank.rank;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import zulu.pagerank.Node;

public class RankCombiner extends Reducer <Text, Node, Text, Node>{
	
	private Node selfInfo = null;
	private Node result = null;
	
	public void reduce(Text key, Iterable<Node> values, Context context) throws IOException, InterruptedException {		
		double neighborPR = 0.0;
		boolean isNode = false, isPR = false;
		
		for (Node val: values) {
			if (val.isNode()) {
				isNode = true;
				selfInfo = new Node(val.getPR(), new Text(val.getLinks()), true);
			}
			else {
				isPR = true;
				neighborPR += val.getPR();
			}
		}
		
		if (isNode)
			context.write(key, selfInfo);
		if (isPR) {
			result = new Node(neighborPR, new Text(), false);
			context.write(key, result);
		}
	}
}
