package com.huike.hdfs.oper;

import java.io.IOException;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.IOUtils;

public class MergeSmallFile {

	private static FileSystem fs = null;
	private static FileSystem local = null;

	public static class RegexExcludePathFilter implements PathFilter {
		private final String regex;

		public RegexExcludePathFilter(String regex) {
			this.regex = regex;
		}

		@Override
		public boolean accept(Path path) {
			boolean flag = path.toString().matches(regex);
			// 过滤 regex 格式的文件，只需 return ！flag
			return !flag;
		}
	}

	public static class RegexAcceptPathFilter implements PathFilter {
		private final String regex;

		public RegexAcceptPathFilter(String regex) {
			this.regex = regex;
		}

		@Override
		public boolean accept(Path path) {
			boolean flag = path.toString().matches(regex);
			// 接受 regex 格式的文件，只需 return flag
			return flag;
		}
	}

	public static void merge(String[] args) throws IOException, URISyntaxException {

		// 读取配置文件
		Configuration conf = new Configuration();
		// 获取默认文件系统
		fs = FileSystem.get(conf);
		// 获得本地文件系统
		local = FileSystem.getLocal(conf);

		// 获取该目录下的合适子目录
		FileStatus[] dirstatus = local.globStatus(new Path(args[0]), new RegexExcludePathFilter("^.*2016-10-14$"));
		Path[] dirs = FileUtil.stat2Paths(dirstatus);

		FSDataOutputStream out = null;
		FSDataInputStream in = null;

		for (Path dir : dirs) {
			String fileName = dir.getName().replace("-", "");// 文件名称
			// 只接受目录下的.txt文件
			FileStatus[] localStatus = local.globStatus(new Path(dir + "/*"), new RegexAcceptPathFilter("^.*txt$"));
			// 获得目录下的所有文件
			Path[] listedPaths = FileUtil.stat2Paths(localStatus);
			// 输出路径
			Path outpath = new Path(args[1] + fileName + ".txt");
			// 打开输出流
			out = fs.create(outpath);
			for (Path p : listedPaths) {
				in = local.open(p);// 打开输入流
				IOUtils.copyBytes(in, out, 4096, false); // 复制数据
				// 关闭输入流
				in.close();
			}
			if (out != null) {
				// 关闭输出流
				out.close();
			}
		}
	}

	public static void main(String[] args) throws IOException, URISyntaxException {
		String[] args0 = { "/home/hadoop/txt/hdfs03/*", "/test/hdfs03/" };
		merge(args0);
	}

}
