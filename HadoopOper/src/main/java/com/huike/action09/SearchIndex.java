package com.huike.action09;

import java.io.IOException;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 数据格式： 李易峰    male 32670
 */
public class SearchIndex extends Configured implements Tool {
	public static class SearchIndexMapper extends Mapper<Object, Text, Text, Text> {
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] tokens = value.toString().split("\\s+");
			String name = tokens[0]; // 姓名
			String gender = tokens[1]; // 性别
			String num = tokens[2]; // 指数
			context.write(new Text(gender), new Text(name + "\t" + num));
		}
	}

	public static class SearchIndexCombiner extends Reducer<Text, Text, Text, Text> {

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			int max = Integer.MIN_VALUE;
			String name = "";
			for (Text value : values) {
				String[] valueArray = value.toString().split("\t");
				if (Integer.parseInt(valueArray[1]) > max) {
					max = Integer.parseInt(valueArray[1]);
					name = valueArray[0];
				}
			}

			context.write(key, new Text(name + "\t" + max));
		}
	}

	public static class SearchIndexPartitioner extends Partitioner<Text, Text> {
		@Override
		public int getPartition(Text key, Text value, int numReduceTasks) {
			// 根据性别进行分区
			String gender = key.toString();
			if (numReduceTasks == 0)
				return 0;
			if (gender.equals("female")) {
				return 0;
			} else
				return 1 % numReduceTasks;
		}
	}

	public static class SearchIndexReducer extends Reducer<Text, Text, Text, Text> {

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			int max = Integer.MIN_VALUE;
			String name = "";
			for (Text value : values) {
				String[] valueArray = value.toString().split("\t");
				if (Integer.parseInt(valueArray[1]) > max) {
					max = Integer.parseInt(valueArray[1]);
					name = valueArray[0];
				}
			}

			context.write(new Text(name + "\t" + key.toString()), new Text(String.valueOf(max)));
		}
	}

	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();// 读取配置文件

		Path mypath = new Path(args[1]);
		FileSystem hdfs = mypath.getFileSystem(conf);
		if (hdfs.isDirectory(mypath)) {
			hdfs.delete(mypath, true);
		}

		Job job = Job.getInstance(conf, "SearchIndex");// 新建一个任务
		job.setJarByClass(SearchIndex.class);// 主类
		job.setMapperClass(SearchIndexMapper.class);// Mapper
		job.setReducerClass(SearchIndexReducer.class);// Reducer
		job.setCombinerClass(SearchIndexCombiner.class);// Combiner
		job.setPartitionerClass(SearchIndexPartitioner.class); // Partitioner
		job.setNumReduceTasks(2);

		job.setOutputKeyClass(Text.class);// 输出结果 key类型
		job.setOutputValueClass(Text.class);// 输出结果 value 类型

		FileInputFormat.addInputPath(job, new Path(args[0]));// 输入路径
		FileOutputFormat.setOutputPath(job, new Path(args[1]));// 输出路径

		return job.waitForCompletion(true) ? 0 : 1;// 提交任务
	}

	public static void main(String[] args) throws Exception {
		String[] args0 = { "/test/action09/SearchIndex.txt", "/test/action09/output/" };
		int res = ToolRunner.run(new Configuration(), new SearchIndex(), args0);
		System.exit(res);
	}

}
