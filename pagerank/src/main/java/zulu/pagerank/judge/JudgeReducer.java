package zulu.pagerank.judge;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class JudgeReducer extends Reducer<Text, DoubleWritable, Text, Text> {
	private static String ERROR = "&gt;error";
	
	public enum COUNTERS {
		ERROR
	}

	public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
		double error = 0;
		
		if (!key.toString().equals(ERROR))
			System.out.println("Something wrong!!!!");
		
		
		for (DoubleWritable val: values) 
			error += val.get();
		
		System.out.println("Error: "+error);
		
		// done!
		if (error < 0.001)
			context.getCounter(COUNTERS.ERROR).setValue(-1);
		else
			context.getCounter(COUNTERS.ERROR).setValue((long) (error*1000));
	}
}
