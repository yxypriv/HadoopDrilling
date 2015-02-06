package knn;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.PriorityBlockingQueue;

import knn.model.KNNNode;
import knn.utils.DistanceUtil;
import knn.utils.HitsUtil;
import knn.utils.ILabeler;
import knn.utils.Pair;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class KNNMapper extends Mapper<LongWritable, Text, Text, Text> {
	List<KNNNode> trainList;
	Integer k;
	Integer distanceMethod;

	@Override
	protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		String trainFilePath = context.getConfiguration().get("trainingFile");
		k = Integer.parseInt(context.getConfiguration().get("K"));
		try {
			distanceMethod = Integer.parseInt(context.getConfiguration().get(
					"distanceMethod"));
		} catch (NullPointerException ne) {
			distanceMethod = DistanceUtil.EuclideanDistance;
		}

		trainList = new ArrayList<KNNNode>();
		FileSystem fs = FileSystem.get(context.getConfiguration());
		BufferedReader reader = new BufferedReader(new InputStreamReader(//
				fs.open(new Path(trainFilePath)), "utf-8"));
		String line = null;
		while ((line = reader.readLine()) != null) {
			trainList.add(new KNNNode(line));
		}
		reader.close();
		super.setup(context);
	}

	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		final double[] distances = new double[trainList.size()];
		String[] split = value.toString().split(",");
		double[] features = new double[split.length];
		for (int i = 0; i < split.length; i++) {
			features[i] = Double.parseDouble(split[i]);
		}
		for (int i = 0; i < trainList.size(); i++)
			distances[i] = DistanceUtil.getDistance(trainList.get(i)//
					.getFeatures(), features, distanceMethod);
		PriorityBlockingQueue<Integer> queue = new PriorityBlockingQueue<Integer>(
				k, Collections.reverseOrder(new Comparator<Integer>() {
					@Override
					public int compare(Integer o1, Integer o2) {
						return Double.compare(distances[o1], distances[o2]);
					}
				}));

		for (int i = 0; i < distances.length; i++) {
			if (queue.size() < k)
				queue.add(i);
			else {
				if (distances[i] < distances[queue.peek()]) {
					queue.poll();
					queue.add(i);
				}
			}
		}
		Pair<String, Integer> mostHits = HitsUtil.getMostHits(queue,
				new ILabeler<Integer>() {
					@Override
					public String getLabel(Integer e) {
						return trainList.get(e).getLabel();
					}
				});

		context.write(value, new Text(mostHits.getV1()));
	}

	@Override
	protected void cleanup(
			Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		if (null != trainList)
			trainList.clear();
		super.cleanup(context);
	}
}