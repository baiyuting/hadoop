package com.huike.unittest;

import java.io.IOException;

import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;
import com.huike.unittest.Temperature;

/**
 * Reducer 单元测试
 */
public class TemperatureReducerTest {
	private Reducer<Text, IntWritable, Text, IntWritable> reducer;// 定义一个Reducer对象
	private ReduceDriver<Text, IntWritable, Text, IntWritable> driver;// 定义一个ReduceDriver对象

	@Before
	public void init() {
		reducer = new Temperature.TemperatureReducer();// 实例化一个Temperature中的TemperatureReducer对象
		driver = new ReduceDriver<Text, IntWritable, Text, IntWritable>(reducer);// 实例化ReduceDriver对象
	}

	@Test
	public void test() throws IOException {
		String key = "weatherStationId";// 声明一个key值
		List<IntWritable> values = new ArrayList<IntWritable>();
		values.add(new IntWritable(200));// 添加第一个value值
		values.add(new IntWritable(100));// 添加第二个value值
		driver.withInput(new Text(key), values)// 跟TemperatureReducer输入类型一致
				.withOutput(new Text(key), new IntWritable(150))// 跟TemperatureReducer输出类型一致
				.runTest();
	}
}
