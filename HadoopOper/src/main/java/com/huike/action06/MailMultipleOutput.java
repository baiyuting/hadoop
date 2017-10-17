package com.huike.action06;

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

public class MailMultipleOutput extends Configured implements Tool {

	public static class MailMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			context.write(value, one);
		}
	}

	public static class MailReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();
		private MultipleOutputs<Text, IntWritable> multipleOutputs;

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			// setup方法中构造一个 MultipleOutputs的实例
			multipleOutputs = new MultipleOutputs<Text, IntWritable>(context);
		}

		protected void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int begin = key.toString().indexOf("@");
			int end = key.toString().indexOf(".");
			if (begin >= end) {
				return;
			}
			// 获取邮箱类别，比如 qq
			String name = key.toString().substring(begin + 1, end);
			int sum = 0;
			for (IntWritable value : values) {
				sum += value.get();
			}
			result.set(sum);
			// 使用 MultipleOutputs实例输出， 而不是 context.write()方法，分别输出键、值、和文件名
			multipleOutputs.write(key, result, name);
		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			multipleOutputs.close();
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();// 读取配置文件

		Path mypath = new Path(args[1]);
		FileSystem hdfs = mypath.getFileSystem(conf);// 创建输出路径
		if (hdfs.isDirectory(mypath)) {
			hdfs.delete(mypath, true);
		}
		Job job = Job.getInstance(conf, "Mail");// 新建一个任务
		job.setJarByClass(MailMultipleOutput.class);// 主类

		FileInputFormat.addInputPath(job, new Path(args[0]));// 输入路径
		FileOutputFormat.setOutputPath(job, new Path(args[1]));// 输出路径

		job.setMapperClass(MailMapper.class);// Mapper
		job.setReducerClass(MailReducer.class);// Reducer

		job.setOutputKeyClass(Text.class);// key输出类型
		job.setOutputValueClass(IntWritable.class);// value输出类型

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		String[] args0 = { "/test/action06/Mail.txt", "/test/action06/output" };
		int res = ToolRunner.run(new Configuration(), new MailMultipleOutput(), args0);
		System.exit(res);
	}
}