package zulu.pagerank.parse;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ParseCombiner extends Reducer <Text, Text, Text, Text> {
	
	private static String PREFIX = "&gt;";
	private Text result = new Text();
	private Text token = new Text(PREFIX);
	
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {		
		StringBuffer parent = new StringBuffer();
		boolean isParent = false, isToken = false;
		
		// iterate token or parents
    	for (Text val: values) {
    		// direct pass token 
    		if (!isToken && isToken(val)) {
    			isToken = true;
    		}
    		// combine parent
    		else {
    			isParent = true;
    			parent.append(PREFIX).append(val.toString());
    		}
    	}
    	
    	//construct: ("&gt;"+parent) * N 
    	result.set(parent.toString());
    	
    	if (isParent)
    		context.write(key, result);
    	
    	if (isToken)
    		context.write(key, token);
	}
	
    public boolean isToken(Text input) {
    	String tmp = input.toString();
    	return tmp.length() == 4 && tmp.substring(0, 4).equals(PREFIX);
    }
}
