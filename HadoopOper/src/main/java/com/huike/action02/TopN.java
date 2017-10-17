package com.huike.action02;

import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class TopN extends Configured implements Tool {

	public static final int k = 3;

	public static class TopNMapper extends Mapper<LongWritable, Text, NullWritable, Text> {

		private TreeMap<Integer, String> map = new TreeMap<Integer, String>();

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();

			String[] parameters = line.split("\\s+");
			Integer clicks = Integer.parseInt(parameters[1]);
			map.put(clicks, value.toString());
			if (map.size() > k) {
				map.remove(map.firstKey());
			}

		}

		protected void cleanup(Context context) throws IOException, InterruptedException {
			for (String text : map.values()) {
				if (text.toString() != null && !text.toString().equals("")) {
					context.write(NullWritable.get(), new Text(text));
				}
			}
		}

	}

	public static class TopNReducer extends Reducer<NullWritable, Text, NullWritable, Text> {

		private TreeMap<Integer, String> map = new TreeMap<Integer, String>();

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			for (Text item : values) {
				String value[] = item.toString().split("\t");
				Integer clicks = Integer.parseInt(value[1]);
				map.put(clicks, item.toString());
				if (map.size() > k) {
					map.remove(map.firstKey());
				}
			}
			for (String text : map.values()) {
				context.write(NullWritable.get(), new Text(text));
			}
		}
	}

	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		Path mypath = new Path(args[1]);
		FileSystem hdfs = mypath.getFileSystem(conf);
		if (hdfs.isDirectory(mypath)) {
			hdfs.delete(mypath, true);
		}
		Job job = Job.getInstance(conf, "TopN");
		job.setMapperClass(TopNMapper.class);
		job.setReducerClass(TopNReducer.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		job.setJarByClass(TopN.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		String[] args0 = { "/test/action02/TopN.txt", "/test/action02/output/" };
		int res = ToolRunner.run(new Configuration(), new TopN(), args0);
		System.out.println(res);

	}
}