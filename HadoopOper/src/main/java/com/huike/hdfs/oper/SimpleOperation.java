package com.huike.hdfs.oper;

import java.io.IOException;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;

public class SimpleOperation {
	private static String HDFS_PATH = "/test/hdfs01/";
	private static String LOCAL_PATH = "/home/hadoop/txt/hdfs01/";
	private static String FILE_NAME = "SimpleOperation.txt";

	// 获取 HDFS文件系统
	public static FileSystem getFileSystem() throws IOException, URISyntaxException {

		// 读取配置文件
		Configuration conf = new Configuration();
		// 返回默认文件系统
		FileSystem fs = FileSystem.get(conf);

		// 获取指定文件系统，本地环境运行，需要使用此种方法获取文件系统
		// URI uri = new URI("hdfs://cluster:9000");//HDFS 地址
		// fs = FileSystem.get(uri,conf);

		return fs;
	}

	// 创建文件目录
	public static void mkdir() throws Exception {

		// 获取文件系统
		FileSystem fs = getFileSystem();
		// 创建文件目录
		fs.mkdirs(new Path(HDFS_PATH));
		// 释放资源
		fs.close();
	}

	// 删除文件或者文件目录
	public static void rmdir() throws Exception {

		// 返回FileSystem对象
		FileSystem fs = getFileSystem();
		// 删除文件或者文件目录 delete(Path f)
		fs.delete(new Path(HDFS_PATH), true);
		// 释放资源
		fs.close();
	}

	// 获取目录下的所有文件
	public static void ListAllFile() throws IOException, URISyntaxException {

		// 返回FileSystem对象
		FileSystem fs = getFileSystem();

		// 列出目录和文件信息
		// FileStatus信息包括:绝对路径、文件大小、文件访问时间、块大小、
		// 文件所属用户、组、访问权限
		FileStatus[] status = fs.listStatus(new Path(HDFS_PATH));

		// 获取目录下的所有文件路径
		Path[] listedPaths = FileUtil.stat2Paths(status);

		// 循环读取每个文件
		for (Path p : listedPaths) {
			System.out.println(p);

		}
		// 释放资源
		fs.close();
	}

	// 文件上传至 HDFS
	public static void copyToHDFS() throws IOException, URISyntaxException {

		// 返回FileSystem对象
		FileSystem fs = getFileSystem();
		Path srcPath = new Path(LOCAL_PATH + FILE_NAME);
		// 目的路径
		Path dstPath = new Path(HDFS_PATH);
		// 实现文件上传
		fs.copyFromLocalFile(srcPath, dstPath);

		// 释放资源
		fs.close();
	}

	// 从 HDFS 下载文件
	public static void copyToLocal() throws IOException, URISyntaxException {

		// 返回FileSystem对象
		FileSystem fs = getFileSystem();
		// 源文件路径
		Path srcPath = new Path(HDFS_PATH + FILE_NAME);
		Path dstPath = new Path(LOCAL_PATH);
		// 下载hdfs上的文件
		fs.copyToLocalFile(srcPath, dstPath);

		// 释放资源
		fs.close();
	}

	// 获取 HDFS 集群节点信息
	public static void getHDFSNodes() throws IOException, URISyntaxException {

		// 返回FileSystem对象
		FileSystem fs = getFileSystem();

		// 获取分布式文件系统
		DistributedFileSystem hdfs = (DistributedFileSystem) fs;
		// 获取所有节点
		DatanodeInfo[] dataNodeStats = hdfs.getDataNodeStats();
		// 循环打印所有节点
		for (int i = 0; i < dataNodeStats.length; i++) {
			System.out.println("DataNode_" + i + "_Name:" + dataNodeStats[i].getHostName());
		}
	}

	// 查找某个文件在 HDFS 集群的位置
	public static void getFileLocation() throws IOException, URISyntaxException {

		// 返回FileSystem对象
		FileSystem fs = getFileSystem();
		// 文件路径
		Path path = new Path(HDFS_PATH + FILE_NAME);
		// 获取文件目录
		FileStatus filestatus = fs.getFileStatus(path);
		// 获取文件块位置列表
		BlockLocation[] blkLocations = fs.getFileBlockLocations(filestatus, 0, filestatus.getLen());
		// 循环输出块信息
		for (int i = 0; i < blkLocations.length; i++) {
			String[] hosts = blkLocations[i].getHosts();
			System.out.println("block_" + i + "_location:" + hosts[0]);
		}
	}

	public static void main(String[] args) throws Exception {
		mkdir();
		copyToHDFS();
		ListAllFile();
		getHDFSNodes();
		getFileLocation();
		// rmdir();
		// copyToLocal();
	}
}
