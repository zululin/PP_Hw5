package zulu.pagerank;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class Node implements WritableComparable<Object> {
	private static String PREFIX = "&amp;";
	private double PR;
	private boolean isNode;
	private Text links;
	private ArrayList<String> linkArray;
	
	public Node() {
		links = new Text();
		PR = 0.0;
		isNode = true;
	}
	
	public Node(double PR, Text links, boolean isNode) {
		this.PR = PR;
		this.isNode = isNode;
		this.links = links;
	}
	
	public void constructArray() {
		String[] split_line = links.toString().split(PREFIX);
		linkArray = new ArrayList<String>();
		
		for (int i = 1; i < split_line.length; i++) {
			linkArray.add(split_line[i]);
		}
	}

	public void setPR(double newPR) {
		PR = newPR;
	}
	
	public double getPR() {
		return PR;
	}
	
	public Text getLinks() {
		return links;
	}
	
	public boolean isNode() {
		return isNode;
	}
	
	public ArrayList<String> getLinkArray() {
		return linkArray;
	}
	
	public void readFields(DataInput in) throws IOException {
		PR = in.readDouble();
		isNode = in.readBoolean();
		links.readFields(in);
	}

	public void write(DataOutput out) throws IOException {
		out.writeDouble(PR);
		out.writeBoolean(isNode);
		links.write(out);
	}
	
	@Override
	public String toString() {
		StringBuffer output = new StringBuffer(String.valueOf(PR));
		String tmp = links.toString();
		int idx = tmp.indexOf(PREFIX);
		
		if (idx != -1) {
			return output.append(tmp.substring(idx)).toString();
		}
		else {
			return output.toString();
		}
	}

	public int compareTo(Object o) {
		double thisPR = this.getPR();
		double thatPR = ((Node)o).getPR();
		
		String thisTitle = this.getLinks().toString();
		String thatTitle = ((Node)o).getLinks().toString();
		
		if (thisPR == thatPR)
			return thisTitle.compareTo(thatTitle);
		else if (thisPR > thatPR)
			return -1;
		else
			return 1;
	}
}
