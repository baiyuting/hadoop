package com.huike.join;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

public class MapJoin extends Configured implements Tool {

	public static class MapJoinMapper extends Mapper<Object, Text, Text, Text> {
		private HashMap<String, String> stationMap = new HashMap<String, String>();
		private String TemperatureStr;
		private String[] TemperatureItems;
		private String station; // 不包括stationId
		private Text outPutKey = new Text();
		private Text outPutValue = new Text();

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			BufferedReader br;
			String station;
			// 把缓存的小文件读取出来并缓存到hashmap中
			Path[] paths = context.getLocalCacheFiles();
			for (Path path : paths) {
				String pathStr = path.toString();
				if (pathStr.endsWith("Station.txt")) {
					br = new BufferedReader(new FileReader(pathStr));
					while (null != (station = br.readLine())) {
						String[] stationItems = station.split("\\s+");
						if (stationItems.length == 3) {// 去掉脏数据
							// 缓存到一个map
							stationMap.put(stationItems[0], stationItems[1] + "\t" + stationItems[2]);
						}
					}
				}
			}
		}

		@Override
		protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			TemperatureStr = value.toString();
			// 过滤脏数据
			if (TemperatureStr.equals("")) {
				return;
			}
			TemperatureItems = TemperatureStr.split("\\s+");
			if (TemperatureItems.length != 3) {
				return;
			}

			// 判断当前记录的joinkey字段是否在stationMap中
			station = stationMap.get(TemperatureItems[0]);
			if (null != station) {// 如果缓存中存在对应的joinkey，那就做连接操作并输出
				outPutKey.set(TemperatureItems[0]);
				outPutValue.set(station + "\t" + TemperatureItems[1] + "\t" + TemperatureItems[2]);
				context.write(outPutKey, outPutValue);
			}
		}
	}

	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		// String[] otherArgs = new GenericOptionsParser(conf,
		// args).getRemainingArgs();
		// if (otherArgs.length < 3) {
		// System.err.println("Usage: mapjoin <cachePath> <in> [<in>...]
		// <out>");
		// System.exit(2);
		// }
		Path mypath = new Path(args[2]);
		FileSystem hdfs = mypath.getFileSystem(conf);
		if (hdfs.isDirectory(mypath)) {
			hdfs.delete(mypath, true);
		}
		Job job = Job.getInstance(conf, "mapjoin");
		// 把小表的数据缓存起来(Station.txt)
		job.addCacheFile(new Path(args[0]).toUri());
		job.setJarByClass(MapJoin.class);
		job.setMapperClass(MapJoinMapper.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		// for (int i = 1; i < otherArgs.length - 1; ++i) {
		FileInputFormat.addInputPath(job, new Path(args[1]));
		// }
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		String[] args0 = { "/test/join/Station.txt", "/test/join/Temperature.txt", "/test/join/output/" };
		int res = ToolRunner.run(new Configuration(), new MapJoin(), args0);
		System.out.println(res);

	}
}
