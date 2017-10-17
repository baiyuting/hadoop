package com.huike.unittest;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.junit.Before;
import org.junit.Test;
import com.huike.unittest.Temperature;

/**
 * Mapper 和 Reducer 集成起来测试
 */
public class TemperatureTest {
	private Mapper<LongWritable, Text, Text, IntWritable> mapper;// 定义一个Mapper对象
	private Reducer<Text, IntWritable, Text, IntWritable> reducer;// 定义一个Reducer对象
	private MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable> driver;// 定义一个MapReduceDriver对象

	@Before
	public void init() {
		mapper = new Temperature.TemperatureMapper();// 实例化一个Temperature中的TemperatureMapper对象
		reducer = new Temperature.TemperatureReducer();// 实例化一个Temperature中的TemperatureReducer对象
		driver = new MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable>(mapper, reducer);// 实例化MapReduceDriver对象
	}

	@Test
	public void test() throws RuntimeException, IOException {
		// 输入两行行测试数据
		String line = "1985 07 31 02   200    94 10137   220    26     1     0 -9999";
		String line2 = "1985 07 31 11   100    56 -9999    50     5 -9999     0 -9999";
		driver.withInput(new LongWritable(), new Text(line))// 跟TemperatureMapper输入类型一致
				.withInput(new LongWritable(), new Text(line2))
				.withOutput(new Text("weatherStationId"), new IntWritable(150))// 跟TemperatureReducer输出类型一致
				.runTest();
	}
}
