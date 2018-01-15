package zulu.pagerank.collect;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import zulu.pagerank.Node;

public class CollectCombiner extends Reducer <Text, Node, Text, Node>{
	private Node result;
	
	public void reduce(Text key, Iterable<Node> values, Context context) throws IOException, InterruptedException {
		boolean isOracle = false;
//		Node selfInfo = null;
		double newPR = 0.0;
		
		for (Node val: values) {
			// origin info
			if (val.isNode()) 
				result = new Node(val.getPR(), new Text(val.getLinks()), val.isNode());
			// oracle node update dangling nodes' PR
			else {
				newPR += val.getPR();
				isOracle = true;
			}
		}
		
		if (isOracle) 
			result.setPR(newPR);
			
//		result.set(selfInfo.toString());
		context.write(key, result);
	}
}
