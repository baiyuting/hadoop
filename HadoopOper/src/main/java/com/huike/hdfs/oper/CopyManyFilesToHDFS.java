package com.huike.hdfs.oper;

import java.io.IOException;
import java.net.URISyntaxException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

/**
 * 将指定格式的多个文件上传至 HDFS
 *
 */
public class CopyManyFilesToHDFS {

	private static FileSystem fs = null;
	private static FileSystem local = null;

	/**
	 * @function Main 方法
	 * @param args
	 * @throws IOException
	 * @throws URISyntaxException
	 */
	public static void main(String[] args) throws IOException, URISyntaxException {
		// 文件上传路径
		String srcPathString = "/home/hadoop/txt/hdfs02/*";
		String dstPathString = "/test/hdfs02/";
		// 调用文件上传 copyToHDFS方法
		copyToHDFS(srcPathString, dstPathString);
	}

	/**
	 * 过滤文件格式,将多个文件上传至 HDFS
	 * 
	 * @param dstPath 目标路径
	 *      
	 * @throws IOException
	 * @throws URISyntaxException
	 */
	public static void copyToHDFS(String srcPathString, String dstPathString) throws IOException, URISyntaxException {
		// 读取配置文件
		Configuration conf = new Configuration();

		// 获取默认文件系统
		fs = FileSystem.get(conf);

		// 获取指定文件系统，本地环境运行，需要使用此种方法获取文件系统
		// URI uri = new URI("hdfs://cluster:9000");//HDFS 地址
		// fs = FileSystem.get(uri,conf);

		// 获取本地文件系统
		local = FileSystem.getLocal(conf);

		// 获取文件目录
		FileStatus[] localStatus = local.globStatus(new Path(srcPathString), new RegexAcceptPathFilter("^.*txt$"));
		// 获取所有文件路径
		Path[] listedPaths = FileUtil.stat2Paths(localStatus);
		// 输出路径
		Path destPath = new Path(dstPathString);
		// 查看目的路径是否存在
		if (!(fs.exists(destPath))) {
			// 如果路径不存在，即刻创建
			fs.mkdirs(destPath);
		}
		// 循环所有文件
		for (Path p : listedPaths) {
			// 将本地文件上传到HDFS
			fs.copyFromLocalFile(p, destPath);
		}
	}

	// 过滤文件
	public static class RegexAcceptPathFilter implements PathFilter {
		private final String regex;

		public RegexAcceptPathFilter(String regex) {
			this.regex = regex;
		}

		@Override
		public boolean accept(Path path) {
			boolean flag = path.toString().matches(regex);
			// 如果要接收 regex 格式的文件，则accept()方法就return flag
			// 如果想要过滤掉regex格式的文件，则accept()方法就return !flag
			return flag;
		}
	}

}
