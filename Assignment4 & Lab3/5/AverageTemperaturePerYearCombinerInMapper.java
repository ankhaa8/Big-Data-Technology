
import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class AverageTemperaturePerYearCombinerInMapper extends Configured
		implements Tool {

	public static class Map extends
			Mapper<LongWritable, Text, IntWritable, IntPair> {

		private IntWritable yearComparable;
		private java.util.Map<IntWritable, IntPair> tempHashMap;

		@Override
		protected void setup(
				Mapper<LongWritable, Text, IntWritable, IntPair>.Context context)
				throws IOException, InterruptedException {
			yearComparable = new IntWritable();
			tempHashMap = new HashMap<IntWritable, IntPair>();
		}

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			IntWritable yearToken;
			int tempToken;

			try {
				yearToken = new IntWritable(Integer.parseInt(line.substring(15,
						19)));
				tempToken = Integer.parseInt(line.substring(87, 92));

				if (tempHashMap.get(yearToken) == null) {
					tempHashMap.put(yearToken, new IntPair(new IntWritable(
							tempToken), new IntWritable(1)));
				} else {
					int newTemp = tempHashMap.get(yearToken).getKey().get()
							+ tempToken;
					int newCount = tempHashMap.get(yearToken).getValue().get() + 1;
					tempHashMap.put(yearToken, new IntPair(new IntWritable(
							newTemp), new IntWritable(newCount)));
				}

			} catch (Exception e) {

			}
		}

		@Override
		protected void cleanup(
				Mapper<LongWritable, Text, IntWritable, IntPair>.Context context)
				throws IOException, InterruptedException {
			for (IntWritable key : tempHashMap.keySet()) {
				yearComparable = key;
				context.write(yearComparable, tempHashMap.get(key));
			}
		}

	}

	public static class Reduce extends
			Reducer<IntWritable, IntPair, IntWritable, DoubleWritable> {

		private DoubleWritable result = new DoubleWritable();

		public void reduce(IntWritable key, Iterable<IntPair> values,
				Context context) throws IOException, InterruptedException {

			double sum = 0;
			int total = 0;
			for (IntPair val : values) {
				try {
					sum += val.getKey().get();
					total += val.getValue().get();
				} catch (Exception e) {

				}

			}
			double average = sum / total;
			result.set(average);
			context.write(key, result);
		}
	}

	public static class NcdcAvgTempYearDescComparator extends
			WritableComparator {
		protected NcdcAvgTempYearDescComparator() {
			super(IntWritable.class);
		}

		@Override
		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			int year1 = readInt(b1, s1);
			int year2 = readInt(b2, s2);
			int comp = (year1 == year2) ? 0 : (year1 < year2) ? 1 : -1;

			return comp;
		}
	}

	public static class YearPartitioner extends
			Partitioner<IntWritable, IntPair> {
		@Override
		public int getPartition(IntWritable key, IntPair value,
				int numReduceTasks) {
			return key.get() < 1930 ? 0 : 1 % numReduceTasks;
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		int res = ToolRunner.run(conf,
				new AverageTemperaturePerYearCombinerInMapper(), args);

		System.exit(res);
	}

	public int run(String[] args) throws Exception {

		Job job = new Job(getConf(), "AverageTempWithInMapperCombiner");
		job.setJarByClass(AverageTemperaturePerYearCombinerInMapper.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntPair.class);

		job.setSortComparatorClass(NcdcAvgTempYearDescComparator.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setNumReduceTasks(2);
		job.setPartitionerClass(YearPartitioner.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		FileSystem fs = FileSystem.get(new Configuration());
		fs.delete(new Path(args[1]), true);

		return job.waitForCompletion(true) ? 0 : 1;
	}

}
