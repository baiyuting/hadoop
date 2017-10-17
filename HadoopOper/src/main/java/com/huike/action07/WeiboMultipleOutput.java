package com.huike.action07;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WeiboMultipleOutput extends Configured implements Tool {
	public static class WeiboMapper extends Mapper<LongWritable, Text, Text, Text> {
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] contents = value.toString().split("\\s+");
			if (contents.length < 5)
				return;
			context.write(new Text("follower"), new Text(contents[0] + "\t" + contents[2]));
			context.write(new Text("friend"), new Text(contents[0] + "\t" + contents[3]));
			context.write(new Text("statuses"), new Text(contents[0] + "\t" + contents[4]));
		}
	}

	public static class WeiboReducer extends Reducer<Text, Text, Text, Text> {
		private MultipleOutputs<Text, Text> mos;

		protected void setup(Context context) throws IOException, InterruptedException {
			mos = new MultipleOutputs<Text, Text>(context);
		}

		private Text name = new Text();
		private Text count = new Text();

		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			for (Text value : values) {
				String[] records = value.toString().split("\t");
				name.set(records[0]);
				count.set(records[1]);

				mos.write(name, count, key.toString());
			}
		}

		protected void cleanup(Context context) throws IOException, InterruptedException {
			mos.close();
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();// 配置文件对象
		Path mypath = new Path(args[1]);
		FileSystem hdfs = mypath.getFileSystem(conf);// 创建输出路径
		if (hdfs.isDirectory(mypath)) {
			hdfs.delete(mypath, true);
		}

		Job job = Job.getInstance(conf, "Weibo");// 构造任务
		job.setJarByClass(WeiboMultipleOutput.class);// 主类

		job.setMapperClass(WeiboMapper.class);// Mapper
		job.setMapOutputKeyClass(Text.class);// Mapper key输出类型
		job.setMapOutputValueClass(Text.class);// Mapper value输出类型

		job.setReducerClass(WeiboReducer.class);// Reducer
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));// 输入路径
		FileOutputFormat.setOutputPath(job, new Path(args[1]));// 输出路径

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		String[] args0 = { "/test/action07/Weibo.txt", "/test/action07/output" };
		int res = ToolRunner.run(new Configuration(), new WeiboMultipleOutput(), args0);
		System.exit(res);
	}
}
