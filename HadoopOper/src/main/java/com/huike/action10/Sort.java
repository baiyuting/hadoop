package com.huike.action10;

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
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class Sort  extends Configured implements Tool {

	public static class SortMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
		private IntWritable num = new IntWritable();

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			num.set(Integer.parseInt(value.toString()));
			context.write(num, value);

		}
	}

	public static class SortReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
		@Override
		protected void reduce(IntWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			for (Text value : values) {
				context.write(key, value);
			}

		}
	}

	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Path mypath = new Path(args[1]);
		// 删除已存在的输出目录
		FileSystem hdfs = mypath.getFileSystem(conf);
		if (hdfs.isDirectory(mypath)) {
			hdfs.delete(mypath, true);
		}

		Job job = Job.getInstance(conf, "Sort");
		job.setJarByClass(Sort.class);
		job.setMapperClass(SortMapper.class);
		job.setReducerClass(SortReducer.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(1);
		FileInputFormat.addInputPath(job, new Path(args[0]));// 输入路径
		FileOutputFormat.setOutputPath(job, new Path(args[1]));// 输出路径
		return job.waitForCompletion(true) ? 0 : 1;
	}
	
	public static void main(String[] args) throws Exception {
		String[] args0 = { "/test/action10/", "/test/action10/output/" };
		int res = ToolRunner.run(new Configuration(), new Sort(), args0);
		System.out.println(res);

	}
}