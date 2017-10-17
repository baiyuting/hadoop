package com.huike.join;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.huike.writable.TextPair;

public class ReduceJoinSecond extends Configured implements Tool {
	public static class JoinStationMapper extends Mapper<LongWritable, Text, TextPair, Text> {
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String[] arr = line.split("\\s+");// 解析气象站数据
			if (arr.length == 3) {// 满足这种数据格式
				// key=气象站id value=气象站名称
				context.write(new TextPair(arr[0], "0"), new Text(arr[1]));
			}
		}
	}

	public static class JoinTemperatureMapper extends Mapper<LongWritable, Text, TextPair, Text> {
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			String[] arr = line.split("\\s+");// 解析天气记录数据
			if (arr.length == 3) {
				// key=气象站id value=天气记录数据
				context.write(new TextPair(arr[0], "1"), new Text(arr[1] + "\t" + arr[2]));
			}
		}
	}

	public static class JoinReducer extends Reducer<TextPair, Text, Text, Text> {
		protected void reduce(TextPair key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			Iterator<Text> iter = values.iterator();
			Text stationName = new Text(iter.next());// 气象站名称
			while (iter.hasNext()) {
				Text record = iter.next();// 天气记录的每条数据
				Text outValue = new Text(stationName.toString() + "\t" + record.toString());
				context.write(key.getFirst(), outValue);
			}
		}
	}

	public static class KeyPartitioner extends Partitioner<TextPair, Text> {
		@Override
		public int getPartition(TextPair key, Text value, int numPartitions) {
			return key.getFirst().hashCode() % numPartitions;
		}
	}

	/**
	 * 继承WritableComparator
	 */
	public static class GroupingComparator extends WritableComparator {
		protected GroupingComparator() {
			super(TextPair.class, true);
		}

		@SuppressWarnings("rawtypes")
		@Override
		// Compare two WritableComparables.
		public int compare(WritableComparable w1, WritableComparable w2) {
			TextPair tp1 = (TextPair) w1;
			TextPair tp2 = (TextPair) w2;
			Text l = tp1.getFirst();
			Text r = tp2.getFirst();
			return l.compareTo(r);
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();// 读取配置文件

		Path mypath = new Path(args[2]);
		FileSystem hdfs = mypath.getFileSystem(conf);
		if (hdfs.isDirectory(mypath)) {
			hdfs.delete(mypath, true);
		}
		Job job = Job.getInstance(conf, "ReduceJoinSecond");// 新建一个任务
		job.setJarByClass(ReduceJoinSecond.class);// 主类

		Path stationInputPath = new Path(args[0]);// 气象站数据源
		Path recordInputPath = new Path(args[1]);// 天气记录数据源
		Path outputPath = new Path(args[2]);// 输出路径

		MultipleInputs.addInputPath(job, stationInputPath, TextInputFormat.class, JoinStationMapper.class);// 读取气象站Mapper
		MultipleInputs.addInputPath(job, recordInputPath, TextInputFormat.class, JoinTemperatureMapper.class);// 读取天气记录Mapper

		FileOutputFormat.setOutputPath(job, outputPath);

		job.setPartitionerClass(KeyPartitioner.class);//自定义分区，必须有
		job.setGroupingComparatorClass(GroupingComparator.class);// 自定义分组
		job.setReducerClass(JoinReducer.class);// Reducer

		job.setMapOutputKeyClass(TextPair.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		String[] args0 = { "/test/join/Station.txt", "/test/join/Temperature.txt", "/test/join/output" };
		int exitCode = ToolRunner.run(new ReduceJoinSecond(), args0);
		System.exit(exitCode);
	}
}
