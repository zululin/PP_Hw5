package zulu.pagerank.collect;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import zulu.pagerank.Node;

public class CollectReducer extends Reducer<Text, Node, Text, Text> {
	private Text result = new Text();
	private static String ORACLE = "&gt;oracle";
	
	public enum COUNTERS {
		DANGLING
	}
	
	public void reduce(Text key, Iterable<Node> values, Context context) throws IOException, InterruptedException {
		double newPR = 0.0;
		
		// store dangling nodes' PR into counter
		if (isOracle(key)) {
			for (Node val: values) {
				newPR += val.getPR();
			}
			
			context.getCounter(COUNTERS.DANGLING).setValue(Double.doubleToLongBits(newPR));
		}
		// origin info
		else {
			for (Node val: values) {
				result.set(val.toString());
				context.write(key, result);
			}	
		}
	}
	
	public boolean isOracle(Text input) {
		return input.toString().equals(ORACLE);
	}
}
