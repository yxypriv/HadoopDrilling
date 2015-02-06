package knn.utils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class LabelDistance extends Pair<String, Double> implements
		WritableComparable<LabelDistance> {

	public LabelDistance(String v1, Double v2) {
		super(v1, v2);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		v1 = in.readUTF();
		v2 = in.readDouble();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(v1);
		out.writeDouble(v2);
	}

	@Override
	public int compareTo(LabelDistance o) {
		return this.v2.compareTo(o.v2);
	}

}
