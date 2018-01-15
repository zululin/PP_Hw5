package zulu.pagerank.judge;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class JudgeCombiner extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
	private DoubleWritable errorVal = new DoubleWritable();
	
	public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {		
		double error = 0.0;
		
		for (DoubleWritable val: values) 
			error += val.get();
		
		errorVal.set(error);
		context.write(key, errorVal);
	}
}
