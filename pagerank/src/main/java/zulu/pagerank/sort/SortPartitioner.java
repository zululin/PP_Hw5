package zulu.pagerank.sort;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

import zulu.pagerank.Node;

public class SortPartitioner extends Partitioner <Node, NullWritable>{

	private double max = 0.01;
	
	@Override
	public int getPartition(Node key, NullWritable val, int numReduceTasks) {
		double gap = max / numReduceTasks;
		
		int num = 0;

		num = (int)Math.floor((key.getPR()) / gap);
		return (numReduceTasks-1) - num;
	}
}
