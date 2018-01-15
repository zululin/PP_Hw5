package zulu.pagerank.prune;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PruneReducer extends Reducer<Text, Text, Text, Text> {
	
	private static String PREFIX = "&amp;";
	private Text result = new Text();
	
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
		// get N from configuration
		long N = context.getConfiguration().getLong("#N", 0);
		
		if (N == 0) {
			try {
				throw new Exception("something worng");
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		StringBuffer initPR = new StringBuffer(String.valueOf((1.0 / N)));
			
		// receive child ack
		for (Text val: values) {
			if (!isToken(val))
				initPR.append(val.toString());
		}
			
		result.set(initPR.toString());
		context.write(key, result);
	}
	
    public boolean isToken(Text input) {
    	String tmp = input.toString();
    	return tmp.length() == 5 && tmp.substring(0, 5).equals(PREFIX);
    }
}
