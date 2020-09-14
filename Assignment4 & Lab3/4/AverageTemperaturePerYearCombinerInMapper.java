import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class AverageTemperaturePerYearCombinerInMapper extends Configured
		implements Tool {

 	public static class AverageTemperatureMapper extends
			Mapper<LongWritable, Text,DescendingWritable, IntPair> {

		private HashMap<String, IntPair> H;

		@Override
		public void setup(Context context) throws IOException,
				InterruptedException {
			H = new HashMap<String, IntPair>();
		}

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String year = value.toString().substring(15, 19);
			int temperature = Integer.parseInt(value
					.toString().substring(87, 92));
			int sum = H.containsKey(year)?H.get(year).getLeft().get()+temperature:temperature;
			int count = H.containsKey(year)?H.get(year).getRight().get()+1:1;
			H.put(year, new IntPair(new IntWritable(sum),new IntWritable(count)));
		}

		@Override
		public void cleanup(Context context) throws IOException,
				InterruptedException {
			for (Entry<String, IntPair> entry : H.entrySet()) {
				context.write(new DescendingWritable( new Text(entry.getKey())), entry.getValue());
			}
		}
	}

	public static class AverageTemperatureReducer extends
			Reducer<DescendingWritable, IntPair, Text, DoubleWritable> {

		@Override
		public void reduce(DescendingWritable key, Iterable<IntPair> values, Context context)
				throws IOException, InterruptedException {
			double sum = 0;
			int cnt = 0;
			for (IntPair val : values) {
				sum += val.getLeft().get();
				cnt += val.getRight().get();
			}
			sum /= 10;
			context.write(key.getText(), new DoubleWritable(sum / cnt));
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		int res = ToolRunner.run(conf,
				new AverageTemperaturePerYearCombinerInMapper(), args);

		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {

		// delete output folder
		FileSystem hdfs = FileSystem.get(this.getConf());
		if (hdfs.exists(new Path(args[1])))
			hdfs.delete(new Path(args[1]), true);

		Job job = new Job(getConf(), "Average Temperature Per Year");
		job.setJarByClass(AverageTemperaturePerYearCombinerInMapper.class);

		job.setMapperClass(AverageTemperatureMapper.class);
		job.setReducerClass(AverageTemperatureReducer.class);

		job.setMapOutputKeyClass(DescendingWritable.class);
		job.setMapOutputValueClass(IntPair.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		return job.waitForCompletion(true) ? 0 : 1;
	}
}