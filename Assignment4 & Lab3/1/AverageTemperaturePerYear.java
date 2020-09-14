
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
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

public class AverageTemperaturePerYear extends Configured implements Tool {

	public static class AverageTemperatureMapper extends
			Mapper<LongWritable, Text, Text, DoubleWritable> {

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			Text year = new Text(value.toString().substring(15, 19));
			DoubleWritable temperature = new DoubleWritable(
					Double.parseDouble(value.toString().substring(87, 92)) / 10);
			context.write(year, temperature);
		}
	}

	public static class AverageTemperatureReducer extends
			Reducer<Text, DoubleWritable, Text, DoubleWritable> {

		@Override
		public void reduce(Text key, Iterable<DoubleWritable> values,
				Context context) throws IOException, InterruptedException {
			double sum = 0;
			int count = 0;
			for (DoubleWritable val : values) {
				sum += val.get();
				count++;
			}
			context.write(key, new DoubleWritable(sum / count));
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		int res = ToolRunner.run(conf, new AverageTemperaturePerYear(), args);

		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {

		// delete output folder
		FileSystem hdfs = FileSystem.get(this.getConf());
		if (hdfs.exists(new Path(args[1])))
			hdfs.delete(new Path(args[1]), true);

		Job job = new Job(getConf(), "Average Temperature Per Year");
		job.setJarByClass(AverageTemperaturePerYear.class);

		job.setMapperClass(AverageTemperatureMapper.class);
		job.setReducerClass(AverageTemperatureReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		return job.waitForCompletion(true) ? 0 : 1;
	}
}
