package com.huike.join;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ReduceJoin extends Configured implements Tool {

	private final static String STATION_FILE = "Station.txt";
	private final static String TEMPERATURE_FILE = "Temperature.txt";

	public static class ReduceJoinMapper extends Mapper<Object, Text, Text, Text> {
		private Text joinKey = new Text();
		private Text combineValue = new Text();

		/**
		 * 为来自不同文件的key/value加标记。
		 */
		@Override
		protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String pathName = ((FileSplit) context.getInputSplit()).getPath().toString();
			// 如果数据来自于STATION_FILE，加一个STATION_FILE的标记
			if (pathName.endsWith(STATION_FILE)) {
				String[] valueItems = value.toString().split("\\s+");
				// 过滤掉脏数据
				if (valueItems.length != 3) {
					return;
				}
				joinKey.set(valueItems[0]);
				combineValue.set(STATION_FILE + valueItems[1] + "\t" + valueItems[2]);
			} else if (pathName.endsWith(TEMPERATURE_FILE)) {
				// 如果数据来自于TEMPERATURE_FILE，加一个TEMPERATURE_FILE的标记
				String[] valueItems = value.toString().split("\\s+");
				// 过滤掉脏数据
				if (valueItems.length != 3) {
					return;
				}
				joinKey.set(valueItems[0]);
				combineValue.set(TEMPERATURE_FILE + valueItems[1] + "\t" + valueItems[2]);
			}
			context.write(joinKey, combineValue);
		}
	}

	public static class ReduceJoinReducer extends Reducer<Text, Text, Text, Text> {
		private List<String> stations = new ArrayList<String>();
		private List<String> temperatures = new ArrayList<String>();
		private Text result = new Text();

		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			// 一定要清空数据
			stations.clear();
			temperatures.clear();
			// 相同key的记录会分组到一起，我们需要把相同key下来自于不同文件的数据分开
			for (Text value : values) {
				String val = value.toString();
				if (val.startsWith(STATION_FILE)) {
					stations.add(val.replaceFirst(STATION_FILE, ""));
				} else if (val.startsWith(TEMPERATURE_FILE)) {
					temperatures.add(val.replaceFirst(TEMPERATURE_FILE, ""));
				}
			}

			for (String station : stations) {
				for (String temperature : temperatures) {
					result.set(station + "\t" + temperature);
					context.write(key, result);
				}
			}
		}
	}

	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		// String[] otherArgs = new GenericOptionsParser(conf,
		// args).getRemainingArgs();
		// if (otherArgs.length < 2) {
		// System.err.println("Usage: reducejoin <in> [<in>...] <out>");
		// System.exit(2);
		// }
		Path mypath = new Path(args[2]);
		FileSystem hdfs = mypath.getFileSystem(conf);
		if (hdfs.isDirectory(mypath)) {
			hdfs.delete(mypath, true);
		}
		Job job = Job.getInstance(conf, "ReduceJoin");
		job.setJarByClass(ReduceJoin.class);
		job.setMapperClass(ReduceJoinMapper.class);
		job.setReducerClass(ReduceJoinReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		// job.setNumReduceTasks(2);
		// for (int i = 0; i < otherArgs.length - 1; ++i) {
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileInputFormat.addInputPath(job, new Path(args[1]));
		// }
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		String[] args0 = { "/test/join/Station.txt", "/test/join/Temperature.txt", "/test/join/output/" };
		int res = ToolRunner.run(new Configuration(), new ReduceJoin(), args0);
		System.out.println(res);

	}
}
