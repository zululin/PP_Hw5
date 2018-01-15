package zulu.pagerank.sort;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import zulu.pagerank.Node;

public class SortReducer extends Reducer<Node, NullWritable, Text, DoubleWritable> {
	private Text title = new Text();
	private DoubleWritable PR = new DoubleWritable();
	
	public void reduce(Node key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
		title.set(new Text(key.getLinks()));
		PR.set(key.getPR());
		
		context.write(title, PR);
	}
}
