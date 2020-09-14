import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;


public class DescendingWritable implements WritableComparable<DescendingWritable>{

	private Text text;
	
	public DescendingWritable() {
		text = new Text("");
	}

	public DescendingWritable(Text text) {
		this.text = text;
	}
	
	public Text getText() {
		return text;
	}

	public void setText(Text text) {
		this.text = text;
	}

	@Override
	public void readFields(DataInput in) throws IOException { 
		text.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException { 
		text.write(out);
	}

	@Override
	public int compareTo(DescendingWritable o) { 
		return -text.compareTo(o.text);
	}
}
