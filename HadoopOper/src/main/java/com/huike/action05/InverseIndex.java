package com.huike.action05;

import java.io.IOException;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

//工具类
class StringUtil {
	public static String getShortPath(String filePath) {
		if (filePath.length() == 0)
			return filePath;
		return filePath.substring(filePath.lastIndexOf("/") + 1);
	}

	public static String getSplitByIndex(String str, String regex, int index) {
		String[] splits = str.split(regex);
		if (splits.length < index)
			return "";
		return splits[index];
	}
}

public class InverseIndex extends Configured implements Tool {

	public static class InverseIndexMapper extends Mapper<LongWritable, Text, Text, Text> {
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			FileSplit split = (FileSplit) context.getInputSplit();
			String fileName = StringUtil.getShortPath(split.getPath().toString());
			StringTokenizer st = new StringTokenizer(value.toString());
			while (st.hasMoreTokens()) {
				String word = st.nextToken().toLowerCase();
				word = word + ":" + fileName;
				context.write(new Text(word), new Text("1"));
			}
		}
	}

	public static class InverseIndexCombiner extends Reducer<Text, Text, Text, Text> {
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			long sum = 0;
			for (Text value : values) {
				sum += Integer.valueOf(value.toString());
			}
			String wordKey = StringUtil.getSplitByIndex(key.toString(), ":", 0);
			String fileNameKey = StringUtil.getSplitByIndex(key.toString(), ":", 1);
			context.write(new Text(wordKey), new Text(fileNameKey + ":" + String.valueOf(sum)));
		}
	}

	public static class InverseIndexReducer extends Reducer<Text, Text, Text, Text> {
		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {

			StringBuilder sb = new StringBuilder("");
			for (Text v : values) {
				sb.append(v.toString() + " ");
			}
			context.write(key, new Text(sb.toString()));
		}
	}

	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();

		// 删除已存在的输出目录
		// FileSystem fileSystem = FileSystem.get(new URI(FILE_OUT_PATH), conf);
		// if (fileSystem.exists(new Path(FILE_OUT_PATH))) {
		// fileSystem.delete(new Path(FILE_OUT_PATH), true);
		// }

		Path mypath = new Path(args[1]);
		FileSystem hdfs = mypath.getFileSystem(conf);
		if (hdfs.isDirectory(mypath)) {
			hdfs.delete(mypath, true);
		}

		Job job = Job.getInstance(conf, "InverseIndex");
		job.setJarByClass(InverseIndex.class);
		job.setMapperClass(InverseIndexMapper.class);
		job.setCombinerClass(InverseIndexCombiner.class);
		job.setReducerClass(InverseIndexReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		String[] args0 = { "/test/action05/InverseIndex/", "/test/action05/output/" };
		int res = ToolRunner.run(new Configuration(), new InverseIndex(), args0);
		System.out.println(res);

	}
}