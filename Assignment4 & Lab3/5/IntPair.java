
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

class IntPair implements WritableComparable<IntPair> {

	private IntWritable key;
	private IntWritable value;

	public IntPair(IntWritable key, IntWritable value) {
		super();
		this.key = key;
		this.value = value;
	}
	public IntPair() {
		super();
		this.key = new IntWritable();
		this.value = new IntWritable();
	}

	public IntWritable getKey() {
		return key;
	}

	public void setKey(IntWritable key) {
		this.key = key;
	}

	public IntWritable getValue() {
		return value;
	}

	public void setValue(IntWritable value) {
		this.value = value;
	}

	public void write(DataOutput out) throws IOException {
		key.write(out);
		value.write(out);
	}

	public void readFields(DataInput in) throws IOException {
		key.readFields(in);
		value.readFields(in);
	}

	public int compareTo(IntPair o) {
		if(this.key.equals(o.key))
			return this.value.compareTo(o.value);
		else
			return this.key.compareTo(o.key);
	}

	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + Math.round(key.get());
		result = prime * result + (int) (value.get() ^ (value.get() >>> 32));
		return result;
	}
	
	public String toString() {
		StringBuilder sb = new StringBuilder();		
		sb.append("(");
		sb.append(key);
		sb.append(", ");
		sb.append(value);
		sb.append(")");
		
		return sb.toString(); 
	}

}

