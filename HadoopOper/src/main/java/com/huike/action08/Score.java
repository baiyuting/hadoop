package com.huike.action08;

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

public class Score extends Configured implements Tool {
	public static class ScoreMapper extends Mapper<Object, Text, Text, Text> {
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] tokens = value.toString().split("<tab>");// 使用分隔符<tab>将数据解析成数组tokens
			if (tokens.length != 4)
				return;
			String gender = tokens[2].toString();// 性别
			String nameAgeScore = tokens[0] + "\t" + tokens[1] + "\t" + tokens[3];// 姓名+年龄+分数
			context.write(new Text(gender), new Text(nameAgeScore));// 输出key=gender，value=name+age+score
		}
	}

	public static class ScorePartitioner extends Partitioner<Text, Text> {
		@Override
		public int getPartition(Text key, Text value, int numReduceTasks) {
			String[] nameAgeScore = value.toString().split("\t");
			// 根据年龄进行分区
			String age = nameAgeScore[1];
			int ageInt = Integer.parseInt(age);
			if (numReduceTasks == 0)
				return 0;
			if (ageInt <= 20) {
				return 0;
			} else if (ageInt > 20 && ageInt <= 50) {
				return 1 % numReduceTasks;
			} else
				return 2 % numReduceTasks;
		}
	}

	public static class ScoreReducer extends Reducer<Text, Text, Text, Text> {
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			int maxScore = Integer.MIN_VALUE;
			String name = " ";
			String age = " ";
			String gender = " ";
			int score = 0;
			for (Text val : values) {
				String[] valTokens = val.toString().split("\t");
				score = Integer.parseInt(valTokens[2]);
				if (score > maxScore) {
					name = valTokens[0];
					age = valTokens[1];
					gender = key.toString();
					maxScore = score;
				}
			}
			context.write(new Text(name), new Text("age-" + age + "\tgender-" + gender + "\tscore-" + maxScore));
		}
	}

	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();// 读取配置文件

		Path mypath = new Path(args[1]);
		FileSystem hdfs = mypath.getFileSystem(conf);
		if (hdfs.isDirectory(mypath)) {
			hdfs.delete(mypath, true);
		}

		Job job = Job.getInstance(conf, "Score");// 新建一个任务
		job.setJarByClass(Score.class);// 主类
		job.setMapperClass(ScoreMapper.class);// Mapper
		job.setReducerClass(ScoreReducer.class);// Reducer

		job.setPartitionerClass(ScorePartitioner.class);// 设置Partitioner类
		job.setNumReduceTasks(3);// reduce个数设置为3

		job.setMapOutputKeyClass(Text.class);// map 输出key类型
		job.setMapOutputValueClass(Text.class);// map 输出value类型

		job.setOutputKeyClass(Text.class);// 输出结果 key类型
		job.setOutputValueClass(Text.class);// 输出结果 value 类型

		FileInputFormat.addInputPath(job, new Path(args[0]));// 输入路径
		FileOutputFormat.setOutputPath(job, new Path(args[1]));// 输出路径

		return job.waitForCompletion(true) ? 0 : 1;// 提交任务
	}

	public static void main(String[] args) throws Exception {
		String[] args0 = { "/test/action08/Score.txt", "/test/action08/output/" };
		int res = ToolRunner.run(new Configuration(), new Score(), args0);
		System.exit(res);
	}

}
