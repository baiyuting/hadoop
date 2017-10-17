package com.huike.unittest;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ToolRunner;


public class Temperature extends Configured implements Tool {

	public static class TemperatureMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			String line = value.toString(); 
			int temperature = Integer.parseInt(line.substring(14, 19).trim());
			if (temperature != -9999) { 
				// 真实的气象站id是从文件名字中提取，为了便于单元测试，这里key设置为常量weatherStationId
				String weatherStationId = "weatherStationId";
				context.write(new Text(weatherStationId), new IntWritable(temperature));
			}
		}
	}


	public static class TemperatureReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			int count = 0;
			for (IntWritable val : values) {
				sum += val.get();
				count++;
			}
			result.set(sum / count);
			context.write(key, result);
		}
	}


	@Override
	public int run(String[] args) throws Exception {

		Configuration conf = new Configuration();

		Path mypath = new Path(args[1]);
		FileSystem hdfs = mypath.getFileSystem(conf);
		if (hdfs.isDirectory(mypath)) {
			hdfs.delete(mypath, true);
		}

		Job job = Job.getInstance(conf, "Temperature");
		job.setJarByClass(Temperature.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));

		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(TemperatureMapper.class);
		job.setReducerClass(TemperatureReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);


		return job.waitForCompletion(true) ? 0 : 1;
	}


	public static void main(String[] args) throws Exception {

		String[] args0 = { "/test/test03/Temperature/", "/test/test03/output" };
		int res = ToolRunner.run(new Configuration(), new Temperature(), args0);
		System.exit(res);
	}
}
