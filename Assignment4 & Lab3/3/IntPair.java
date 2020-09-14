

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

public class IntPair implements Writable {
	private IntWritable left;
	private IntWritable right;

	public IntPair() {
		left = new IntWritable(0);
		right = new IntWritable(0);
	}

	public IntPair(IntWritable left, IntWritable right) {
		this.left = left;
		this.right = right;
	}

	public IntWritable getLeft() {
		return left;
	}

	public void setLeft(IntWritable left) {
		this.left = left;
	}

	public IntWritable getRight() {
		return right;
	}

	public void setRight(IntWritable right) {
		this.right = right;
	}

	@Override
	public void readFields(DataInput in) throws IOException { 
		left.readFields(in);
		right.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException { 
		left.write(out);
		right.write(out);
	}

}
