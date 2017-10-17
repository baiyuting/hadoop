package com.huike.test03;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ToolRunner;

/**
 * 
 * 统计美国各个气象站30年来的平均气温
 *
 */
public class Temperature extends Configured implements Tool {

	public static class TemperatureMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
		/**
		 * @function Mapper 解析气象站数据
		 * @input key=偏移量 value=气象站数据
		 * @output key=weatherStationId value=temperature
		 */
		// map 函数是一个合适去除无效数据的地方
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			String line = value.toString(); // 每行气象数据
			int temperature = Integer.parseInt(line.substring(14, 19).trim());// 每小时气温值
			if (temperature != -9999) { // 过滤无效数据
				FileSplit fileSplit = (FileSplit) context.getInputSplit();
				String weatherStationId = fileSplit.getPath().getName().substring(5, 10);// 通过文件名称提取气象站id
				context.write(new Text(weatherStationId), new IntWritable(temperature));
			}
		}
	}

	/**
	 * 
	 * @function Reducer 统计美国各个气象站的平均气温
	 * @input key=weatherStationId value=temperature
	 * @output key=weatherStationId value=average(temperature)
	 */
	public static class TemperatureReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		// reduce 函数的输入类型必须匹配 map 函数的输出类型
		// 在 map的输出结果中，所有相同的气象站（key）被分配到同一个reduce执行
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			int count = 0;
			// 统计每个气象站的气温值总和
			for (IntWritable val : values) {
				sum += val.get();
				count++;
			}
			// 求每个气象站的气温平均值
			result.set(sum / count);
			context.write(key, result);
		}
	}

	/**
	 * @function 任务驱动方法
	 * @param args
	 * @return
	 * @throws Exception
	 */
	@Override
	public int run(String[] args) throws Exception {
		// Configuration 类读取 Hadoop 的配置文件，如core-site.xml、mapred-site.xml、hdfs-site.xml等。
		Configuration conf = new Configuration();

		Path mypath = new Path(args[1]);
		FileSystem hdfs = mypath.getFileSystem(conf);
		if (hdfs.isDirectory(mypath)) {
			hdfs.delete(mypath, true);
		}

		Job job = Job.getInstance(conf, "Temperature");// 新建一个任务
		job.setJarByClass(Temperature.class);// 设置主类
		// addInputPath()路径可以是单个文件、一个目录（此时，将目录下所有文件当作输入）或符合特定文件模式的一系列文件。
		// 可以多次调用 addInputPath()
		FileInputFormat.addInputPath(job, new Path(args[0]));// 输入路径
		// setOutputPath() 来指定输出路径（只能有一个输出路径），指定reduce函数输出文件的写入目录
		// 在运行作业前该目录是不应该存在的，否则 Hadoop 会报错并拒绝运行作业，目的是防止误操作
		FileOutputFormat.setOutputPath(job, new Path(args[1]));// 输出路径

		job.setMapperClass(TemperatureMapper.class);// Mapper
		job.setReducerClass(TemperatureReducer.class);// Reducer

		
		// 有时需要setMapOutputKeyClass()和setMapOutputValueClass()来设置 map函数的输出类型。
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		// 输入的类型通过 InputFormat 类来控制，默认是TextInputFormat
		// 输出的类型通过 OutputFormat 类来控制，默认是TextOutputFormat
		// job.setInputFormatClass(TextInputFormat.class);//文件输入格式
		// job.setOutputFormatClass(TextOutputFormat.class);//文件输出格式

		return job.waitForCompletion(true) ? 0 : 1;// 提交任务
	}

	/**
	 * @function main 方法
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		// 数据输入路径和输出路径
		String[] args0 = { "/test/test03/Temperature/", "/test/test03/output" };
		int res = ToolRunner.run(new Configuration(), new Temperature(), args0);
		System.exit(res);
	}
}