package knn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class KNNDriver extends Configured implements Tool {
	public static void main(String[] args) {
		int res;
		try {
			res = ToolRunner.run(new Configuration(), new KNNDriver(), args);
			System.exit(res);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		conf.set("trainingFile", args[1]);
		conf.set("K", args[3]);
		conf.set("distanceMethod", args[4]);
		Job job = Job.getInstance(conf, "KNN");
		job.setJarByClass(KNNDriver.class);
		job.setMapperClass(KNNMapper.class);
		// job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(KNNReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		return (job.waitForCompletion(true) ? 0 : 1);
	}

}
