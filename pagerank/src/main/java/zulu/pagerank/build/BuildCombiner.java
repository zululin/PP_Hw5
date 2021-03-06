package zulu.pagerank.build;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class BuildCombiner extends Reducer <Text, Text, Text, Text> {
	
	private static String PREFIX = "&gt;";
	private Text result = new Text();
	private Text token = new Text(PREFIX);
	
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {		
		StringBuffer child = new StringBuffer();
		boolean isChild = false, isToken = false;
		
		// iterate token or parents
    	for (Text val: values) {
    		// direct pass token 
    		if (!isToken && isToken(val)) {
    			isToken = true;
    		}
    		// combine inter-child (due to many times combiner)
    		else if (isInterChild(val)) {
    			isChild = true;
    			child.append(val.toString());
    		}
    		// combine child
    		else {
    			isChild = true;
    			child.append(PREFIX).append(val.toString());
    		}
    	}
		
    	//construct: ("&gt;"+child) * N 
    	result.set(child.toString());
		
		if (isChild)
    		context.write(key, result);
    	
    	if (isToken)
    		context.write(key, token);
	}
	
    public boolean isToken(Text input) {
    	String tmp = input.toString();
    	return tmp.length() == 4 && tmp.substring(0, 4).equals(PREFIX);
    }
    
    public boolean isInterChild(Text input) {
    	String tmp = input.toString();
    	return tmp.length() > 4 && tmp.substring(0, 4).equals(PREFIX);
    }
}
