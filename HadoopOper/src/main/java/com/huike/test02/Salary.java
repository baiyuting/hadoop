package com.huike.test02;

import java.io.IOException;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class Salary extends Configured implements Tool {
	
	public static class SalaryMapper extends Mapper<Object, Text, Text, Text> {
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] record = value.toString().split("\\s+");
			// 输出(key:3-5年经验 value:15-30k)
			context.write(new Text(record[1]), new Text(record[2]));
		}
	}


	//注意：同一个 Key（相同 Hadoop工作年限）的所有 Value（工资水平）集合都会分配到同一个 reduce 方法中执行。
	public static class SalaryReducer extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text Key, Iterable<Text> Values, Context context) throws IOException, InterruptedException {
			int low = 0;// 记录最低工资
			int high = 0;// 记录最高工资
			int count = 1;
			// 统计特定工作年限的薪资范围
			for (Text value : Values) {
				String[] arr = value.toString().split("-");
				int l = filterSalary(arr[0]);
				int h = filterSalary(arr[1]);
				if (count == 1 || l < low) {
					low = l;
				}
				if (count == 1 || h > high) {
					high = h;
				}
				count++;
			}
			context.write(Key, new Text(low + "-" + high + "k"));
		}
	}

	/**
	 * salary使用正则表达式，解析出最低工资 low 和 最高工资 high
	 */
	public static int filterSalary(String salary) {
		String sal = Pattern.compile("[^0-9]").matcher(salary).replaceAll("");
		return Integer.parseInt(sal);
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();// 读取配置文件

		Job job = Job.getInstance(conf, "Salary");// 新建一个任务
		job.setJarByClass(Salary.class);// 主类

		Path in = new Path(args[0]);// 输入路径
		Path out = new Path(args[1]);// 输出路径

		FileSystem hdfs = out.getFileSystem(conf);
		// 如果输出文件路径存在，先删除
		if (hdfs.isDirectory(out)) {
			hdfs.delete(out, true);
		}

		FileInputFormat.addInputPath(job, in);// 输入路径
		FileOutputFormat.setOutputPath(job, out);// 输出路径

		job.setMapperClass(SalaryMapper.class);// 设置自定义Mapper
		job.setReducerClass(SalaryReducer.class);// 设置自定义Reducer
		job.setMapOutputKeyClass(Text.class);// Mapper key输出类型
		job.setMapOutputValueClass(Text.class);// Mapper value输出类型

		return job.waitForCompletion(true) ? 0 : 1;// 等待作业完成退出
	}

	/**
	 * @param args
	 *            输入文件、输出路径
	 */
	public static void main(String[] args) throws Exception {

		String[] args0 = { "/test/test02/Salary.txt", "/test/test02/output" };
		int res = ToolRunner.run(new Configuration(), new Salary(), args0);
		System.exit(res);

	}
}