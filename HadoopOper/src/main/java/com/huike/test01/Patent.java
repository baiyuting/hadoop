package com.huike.test01;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class Patent extends Configured implements Tool {

	/**
	 * 一个继承 Mapper的静态类 MapClass map 方法包含三个参数： Text key引用的专利，Text value被引用的专利，
	 * Context context：Map的上下文 map 方法主要是把字符串解析成Key-Value的形式，发给 Reduce 端来统计
	 */
	public static class MapClass extends Mapper<Text, Text, Text, Text> {

		public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
			// 根据业务需求，将key value调换输出
			context.write(value, key);

		}
	}

	/**
	 * 一个继承 Reducer的静态类 ReduceClass reduce 方法包含三个参数： Text key：Map输出的
	 * Key值，Iterable<Text> values：Map输出的 Value集合（相同 Key的集合）， Context
	 * context：Reduce的上下文 reduce 方法的主要是获取 map方法的 key-value结果，相同的 Key发送到同一个
	 * reduce 里处理，然后迭代 Key，把 Value 相加，结果写到 HDFS中
	 */
	public static class ReduceClass extends Reducer<Text, Text, Text, Text> {
		
//		private Text content = new Text();
		
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			String csv = "";
			// 将引入相同专利编号拼接输出
			for (Text val : values) {
				if (csv.length() > 0)
					csv += ",";
				csv += val.toString();
			}

			context.write(key, new Text(csv));
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		// 读取 Hadoop 的配置文件，如 core-site.xml、mapred-site.xml、hdfs-site.xml等，也可以使用
		// set 方法进行重新设置，set方法设置的值会替代配置文件里面配置的值
		Configuration conf = getConf();
		// 自定义key value之间的分隔符（默认为tab）
		conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", ",");
		// Job实例：表示一个 MapReduce 任务，Job 的构造方法有两个参数，第一个参数为Configuration，第二个参数为Job的名称
		Job job = Job.getInstance(conf, "Template");// 新建一个任务
		job.setJarByClass(Patent.class);// 主类

		Path in = new Path(args[0]);// 输入路径
		Path out = new Path(args[1]);// 输出路径

		FileSystem hdfs = out.getFileSystem(conf);
		if (hdfs.isDirectory(out)) {// 如果输出路径存在就删除
			hdfs.delete(out, true);
		}

		FileInputFormat.setInputPaths(job, in);// 文件输入
		FileOutputFormat.setOutputPath(job, out);// 文件输出

		job.setMapperClass(MapClass.class);// 设置自定义Mapper
		job.setReducerClass(ReduceClass.class);// 设置自定义Reducer

		job.setInputFormatClass(KeyValueTextInputFormat.class);// 文件输入格式
		job.setOutputFormatClass(TextOutputFormat.class);// 文件输出格式
		job.setOutputKeyClass(Text.class);// 设置作业输出值 Key 的类
		job.setOutputValueClass(Text.class);// 设置作业输出值 Value 的类

		return job.waitForCompletion(true) ? 0 : 1;// 等待作业完成退出
	}

	/**
	 * @param args 输入文件、输出路径
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		// 程序参数：输入路径、输出路径
		String[] args0 = { "/test/test01/Patent.txt", "/test/test01/output" };
		int res = ToolRunner.run(new Configuration(), new Patent(), args0);
		System.exit(res);

	}
}