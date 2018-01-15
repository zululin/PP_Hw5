package zulu.pagerank.parse;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.mapreduce.Mapper;

public class ParseMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	private static String PREFIX = "&amp;";
	private Text self = new Text();
	private Text token = new Text(PREFIX);
	private ArrayList<Text> child = new ArrayList<Text>();
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String sentence = new String(value.toString());
         	
		/*  Match title pattern */  
		Pattern titlePattern = Pattern.compile("<title>(.+?)</title>");
		Matcher titleMatcher = titlePattern.matcher(sentence);
		
		if (titleMatcher.find()) {
			String tmp = new String(unescapeXML(extractTitle(titleMatcher.group())));
			self.set(tmp);
		}

		/*  Match link pattern */
        Pattern linkPattern = Pattern.compile("\\[\\[(.+?)([\\|#]|\\]\\])");
		Matcher linkMatcher = linkPattern.matcher(sentence);
		
		int i = 0;
		while (linkMatcher.find()) {
			try {
				String tmp = new String(unescapeXML(capitalizeFirstLetter(extractLink(linkMatcher.group()))));
				
				// (key, val) -> (child, parent): parent send msg to child
				if (child.size() <= i)
					child.add(new Text(tmp));
				else
					child.set(i, new Text(tmp));
				context.write(child.get(i), self);
				i++;
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		
		// (self, token) pair for proving self exist
		context.write(self, token);
	}

	private String extractTitle(String input) {
		int len = input.length();
		
		return input.substring(7, len-8);
	}
	
	private String extractLink(String input) throws Exception {
		int len = input.length();
			
		if (input.charAt(len-1) == ']') {
			return input.substring(2, len-2);
		}
		else if (input.charAt(len-1) == '|') {
			return input.substring(2, len-1);
		}
		else if (input.charAt(len-1) == '#'){
			return input.substring(2, len-1);
		}
		else {
			throw new Exception("Something wrong");
		}
	}
	
	private String unescapeXML(String input) {
		return input.replaceAll("&lt;", "<").replaceAll("&gt;", ">").replaceAll("&amp;", "&").replaceAll("&quot;", "\"").replaceAll("&apos;", "\'");
    }

    private String capitalizeFirstLetter(String input){
    	char firstChar = input.charAt(0);

        if ( firstChar >= 'a' && firstChar <='z'){
            if ( input.length() == 1 ){
                return input.toUpperCase();
            }
            else
                return input.substring(0, 1).toUpperCase() + input.substring(1);
        }
        else 
        	return input;
    }
}
