package com.huike.test00;

import java.io.IOException;
import java.util.StringTokenizer;

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

public class WordCount extends Configured implements Tool {

	public static class TokenizerMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
		// 定义在这里
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				word.set(itr.nextToken());
				context.write(word, one);
			}

		}
	}

	public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		// 定义在这里
		private IntWritable result = new IntWritable();
        
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable value : values) {
				sum += value.get();
			}
			result.set(sum);
			context.write(key, result);

		}

	}

	@Override
	public int run(String[] args) throws Exception {

		Configuration conf = getConf();// 读取配置文件

		// conf.setBoolean("mapreduce.map.output.compress",true);
		// conf.setClass("mapreduce.map.output.compress.codec",GzipCodec.class,CompressionCodec.class);
		// conf.setBoolean("mapreduce.output.fileoutputformat.compress",true);
		// conf.setClass("mapreduce.output.fileoutputformat.compress.codec",GzipCodec.class,CompressionCodec.class);

		if (args.length < 2) {
			System.err.println("Usage: WordCount <in> <out>");
			System.exit(2);
		}

		Job job = Job.getInstance(conf, "WordCount");// 新建一个任务

		job.setJarByClass(WordCount.class);

		Path in = new Path(args[0]);// 输入路径
		Path out = new Path(args[1]);// 输出路径

		FileSystem hdfs = out.getFileSystem(conf);
		// 如果输出文件路径存在，先删除
		if (hdfs.isDirectory(out)) {
			hdfs.delete(out, true);
		}

		FileInputFormat.addInputPath(job, in);// 输入路径
		FileOutputFormat.setOutputPath(job, out);// 输出路径

		job.setMapperClass(TokenizerMapper.class);// 设置自定义Mapper
		// job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		return job.waitForCompletion(true) ? 0 : 1; // 等待作业完成退出
	}

	public static void main(String[] args) throws Exception {
		// 程序参数：输入路径、输出路径
		// String[] args0 = { "/test/test00/WordCount.txt",
		// "/test/test00/output" };
		// 集群运行：可通过为args传入参数
		int res = ToolRunner.run(new Configuration(), new WordCount(), args);
		System.exit(res);
	}
}