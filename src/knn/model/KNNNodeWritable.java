package knn.model;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class KNNNodeWritable extends KNNNode implements
		WritableComparable<KNNNodeWritable> {
	static int maxId = 0;
	int id;

	KNNNodeWritable(String line) {
		super(line);
		id = ++maxId;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		id = in.readInt();
		features = new double[in.readInt()];
		for (int i = 0; i < features.length; i++) {
			features[i] = in.readDouble();
		}
		label = in.readUTF();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(id);
		out.writeInt(features.length);
		for (int i = 0; i < features.length; i++)
			out.writeDouble(features[i]);
		out.writeUTF(label);
	}

	@Override
	public int compareTo(KNNNodeWritable o) {
		return Integer.compare(id, o.id);
	}

}
