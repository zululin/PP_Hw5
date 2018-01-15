package zulu.pagerank.parse;

import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

import org.apache.hadoop.io.Text;

public class ParseReducer extends Reducer<Text, Text, Text, Text> {
	
	private static String PREFIX = "&gt;";
	private Text result = new Text();
//	private Text self = new Text();
	
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
    	StringBuffer parent = new StringBuffer();
    	boolean isExist = false;
    	
    	// iterate token or parents
    	for (Text val: values) {
    		// if token exist, the link is valid
    		if (isExist == false && isToken(val)) 
    			isExist = true;
    		// combine parent
    		else
    			parent.append(val.toString());
		}
    	
    	//construct: ("&gt;"+parent) *N 
    	result.set(parent.toString());
//    	self.set(key);
    	
    	if (isExist == true)
    		context.write(key, result);
	}
    
    public boolean isToken(Text input) {
    	String tmp = input.toString();
    	return tmp.length() == 4 && tmp.substring(0, 4).equals(PREFIX);
    }
}
