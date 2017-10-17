package com.huike.test00;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

public class WordCountLog extends Configured implements Tool {

	private static final Logger wcLogger = Logger.getLogger(WordCountLog.class);

	public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
		private static final Logger mapLogger = Logger.getLogger(WordCountLog.TokenizerMapper.class);
		// 定义在这里
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				word.set(itr.nextToken());
				context.write(word, one);
			}

			mapLogger.debug("Debug message");
			mapLogger.info("Info message");
			mapLogger.warn("Warn message");
			mapLogger.error("Error message");
			mapLogger.fatal("Fatal message");

		}
	}

	public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		private static final Logger reduceLogger = Logger.getLogger(WordCountLog.IntSumReducer.class);
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

			reduceLogger.debug("Debug message");
			reduceLogger.info("Info message");
			reduceLogger.warn("Warn message");
			reduceLogger.error("Error message");
			reduceLogger.fatal("Fatal message");
		}

	}

	@Override
	public int run(String[] args) throws Exception {

		Configuration conf = getConf();// 读取配置文件

		wcLogger.debug("Debug message");
		wcLogger.info("Info message");
		wcLogger.warn("Warn message");
		wcLogger.error("Error message");
		wcLogger.fatal("Fatal message");

		// conf.setBoolean("mapreduce.map.output.compress",true);
		// conf.setClass("mapreduce.map.output.compress.codec",GzipCodec.class,CompressionCodec.class);
		// conf.setBoolean("mapreduce.output.fileoutputformat.compress",true);
		// conf.setClass("mapreduce.output.fileoutputformat.compress.codec",GzipCodec.class,CompressionCodec.class);

		if (args.length < 2) {
			System.err.println("Usage: WordCount <in> <out>");
			System.exit(2);
		}

		Job job = Job.getInstance(conf, "WordCount");// 新建一个任务
		try {
			job.setJarByClass(WordCountLog.class);
		} catch (Exception ex) {
			wcLogger.error("Caught some exception", ex);
		}
		
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
		int res = ToolRunner.run(new Configuration(), new WordCountLog(), args);
		System.exit(res);
	}
}