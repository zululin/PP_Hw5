package zulu.pagerank.rank;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import zulu.pagerank.Node;

public class RankReducer extends Reducer<Text, Node, Text, Text> {
	private static String ERROR = "&gt;error";
	private static String ORACLE = "&gt;oracle";
	private static double alpha = 0.85;
	private Text result = new Text();
	private Text errKey = new Text(ERROR);
	private Text errVal = new Text();
	
	public void reduce(Text key, Iterable<Node> values, Context context) throws IOException, InterruptedException {
		
		// get N from configuration
		long N = context.getConfiguration().getLong("#N", 0);
		long D = context.getConfiguration().getLong("#D", 0);
		
		if (N == 0) {
			try {
				throw new Exception("something worng");
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		double error = 0.0;
		double newPR = 0.0;
		double neighborPR = 0.0;
		Node selfInfo = null;
		
		for (Node val: values) {
			// origin info
			if (val.isNode()) 
				selfInfo = new Node(val.getPR(), new Text(val.getLinks()), true);
			// update neighbor PR
			else 
				neighborPR += val.getPR();
		}
		
		newPR = (1-alpha)*(1.0/N) + alpha*neighborPR + alpha*(Double.longBitsToDouble(D)/N);
		
		error = Math.abs(selfInfo.getPR() - newPR);
		
		if (!isOracle(key)) {
			errVal.set(String.valueOf(error));
			context.write(errKey, errVal);
		}
		
		selfInfo.setPR(newPR);
		result.set(selfInfo.toString());
		context.write(key, result);
	}
	
	public boolean isOracle(Text input) {
		return input.toString().equals(ORACLE);
	}
}
