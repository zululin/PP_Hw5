package zulu.pagerank.parse;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ParseCombiner extends Reducer <Text, Text, Text, Text> {
	
	private static String PREFIX = "&amp;";
	private Text result = new Text();
	private Text token = new Text();
//	private Text self = new Text();
	
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {		
		StringBuffer parent = new StringBuffer();
		boolean isParent = false, isToken = false;
		
		// iterate token or parents
    	for (Text val: values) {
    		// direct pass token 
    		if (!isToken && isToken(val)) {
    			isToken = true;
    			token.set(val);
    		}
    		// combine parent
    		else {
    			isParent = true;
    			parent.append(PREFIX).append(val.toString());
    		}
    	}
    	
    	//construct: ("&amp;"+parent) * N 
    	result.set(parent.toString());
//    	self.set(key);
    	
    	if (isParent)
    		context.write(key, result);
    	
    	if (isToken)
    		context.write(key, token);
	}
	
    public boolean isToken(Text input) {
    	String tmp = input.toString();
    	return tmp.length() == 5 && tmp.substring(0, 5).equals(PREFIX);
    }
}
