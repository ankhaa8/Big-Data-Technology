import java.io.IOException;

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

public class AverageTemperaturePerYearCombiner extends Configured implements
		Tool {

	public static boolean production = true;

	public static class AverageTemperatureMapper extends
			Mapper<LongWritable, Text, Text, IntPair> {

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			Text year = new Text(value.toString().substring(15, 19));
			IntWritable temperature = new IntWritable(Integer.parseInt(value
					.toString().substring(87, 92)));
			context.write(year, new IntPair(temperature, new IntWritable(1)));
		}
	}

	public static class AverageTemperatureCombier extends
			Reducer<Text, IntPair, Text, IntPair> {

		@Override
		public void reduce(Text key, Iterable<IntPair> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			int count = 0;
			for (IntPair val : values) {
				sum += val.getLeft().get();
				count += val.getRight().get();
			}
			context.write(key, new IntPair(new IntWritable(sum),
					new IntWritable(count)));
		}
	}

	public static class AverageTemperatureReducer extends
			Reducer<Text, IntPair, Text, DoubleWritable> {

		@Override
		public void reduce(Text key, Iterable<IntPair> values, Context context)
				throws IOException, InterruptedException {
			double sum = 0;
			int count = 0;
			for (IntPair val : values) {
				sum += val.getLeft().get();
				count += val.getRight().get();
			}
			sum /= 10;
			context.write(key, new DoubleWritable(sum / count));
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		int res = ToolRunner.run(conf, new AverageTemperaturePerYearCombiner(), args);

		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {

		// delete output folder
		FileSystem hdfs = FileSystem.get(this.getConf());
		if (hdfs.exists(new Path(args[1])))
			hdfs.delete(new Path(args[1]), true);

		Job job = new Job(getConf(), "Average Temperature Per Year");
		job.setJarByClass(AverageTemperaturePerYearCombiner.class);

		job.setMapperClass(AverageTemperatureMapper.class);
		job.setReducerClass(AverageTemperatureReducer.class);

		job.setCombinerClass(AverageTemperatureCombier.class);

		job.setMapOutputKeyClass(Text.class);
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