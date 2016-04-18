package com.cfets.door.util;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.io.IOUtils;
import org.apache.log4j.Logger;

/**
 * HDFS工具类 Author: 杨航 Since: 2016-04-14
 */
public class HDFSUtil {

	/**
	 * 判断路径是否存在
	 *
	 * @param conf
	 * @param path
	 * @return
	 * @throws IOException
	 */
	public static boolean exits(Configuration conf, String path)
			throws IOException {
		FileSystem fs = FileSystem.get(conf);
		return fs.exists(new Path(path));
	}

	/**
	 * 创建文件
	 *
	 * @param conf
	 * @param filePath
	 * @param contents
	 * @throws IOException
	 */
	public static void createFile(Configuration conf, String filePath,
			byte[] contents) throws IOException {
		FileSystem fs = FileSystem.get(conf);
		Path path = new Path(filePath);
		FSDataOutputStream outputStream = fs.create(path);
		outputStream.write(contents);
		outputStream.close();
		fs.close();
	}

	/**
	 * 创建文件
	 *
	 * @param conf
	 * @param filePath
	 * @param fileContent
	 * @throws IOException
	 */
	public static void create(Configuration conf, String filePath,
			String fileContent) throws IOException {
		createFile(conf, filePath, fileContent.getBytes());
	}
	
	/**
	 * 创建目录
	 *
	 * @param conf
	 * @param dirName
	 * @return
	 * @throws IOException
	 */
	public static boolean createDirectory(Configuration conf, String dirName)
			throws IOException {
		FileSystem fs = FileSystem.get(conf);
		Path dir = new Path(dirName);
		boolean result = fs.mkdirs(dir);
		fs.close();
		return result;
	}

	/**
	 * 上传目录或文件
	 * @param conf
	 * @param localFilePath
	 * @param remoteFilePath
	 * @throws IOException
	 */
	public static void upload(Configuration conf,
			String localFilePath, String remoteFilePath) throws IOException {
		FileSystem fs = FileSystem.get(conf);
		Path localPath = new Path(localFilePath);
		Path remotePath = new Path(remoteFilePath);
		fs.copyFromLocalFile(true, true, localPath, remotePath);
		fs.close();
	}

	/**
	 * 下载目录或文件
	 * @param conf
	 * @param localFilePath
	 * @param remoteFilePath
	 * @throws IOException
	 */
	public static void download(Configuration conf,
			String localFilePath, String remoteFilePath) throws IOException {
		FileSystem fs = FileSystem.get(conf);
		Path localPath = new Path(localFilePath);
		Path remotePath = new Path(remoteFilePath);
		fs.copyToLocalFile(false, localPath, remotePath);
		fs.close();
	}
	
	/**
	 * 删除目录或文件
	 *
	 * @param conf
	 * @param remoteFilePath
	 * @param recursive
	 * @return
	 * @throws IOException
	 */
	public static boolean delete(Configuration conf, String remoteFilePath,
			boolean recursive) throws IOException {
		FileSystem fs = FileSystem.get(conf);
		boolean result = fs.delete(new Path(remoteFilePath), recursive);
		fs.close();
		return result;
	}

	/**
	 * 删除目录或文件(如果有子目录,则级联删除)
	 *
	 * @param conf
	 * @param remoteFilePath
	 * @return
	 * @throws IOException
	 */
	public static boolean delete(Configuration conf, String remoteFilePath)
			throws IOException {
		return delete(conf, remoteFilePath, true);
	}

	/**
	 * 文件重命名
	 *
	 * @param conf
	 * @param oldFileName
	 * @param newFileName
	 * @return
	 * @throws IOException
	 */
	public static boolean rename(Configuration conf, String oldFileName,
			String newFileName) throws IOException {
		FileSystem fs = FileSystem.get(conf);
		Path oldPath = new Path(oldFileName);
		Path newPath = new Path(newFileName);
		boolean result = fs.rename(oldPath, newPath);
		fs.close();
		return result;
	}

	/**
	 * 列出指定目录下的文件\子目录信息（非递归）
	 *
	 * @param conf
	 * @param dirPath
	 * @return
	 * @throws IOException
	 */
	public static FileStatus[] getDirectoryStatus(Configuration conf, String dirPath)
			throws IOException {
		FileSystem fs = FileSystem.get(conf);
		FileStatus[] fileStatuses = fs.listStatus(new Path(dirPath));
		fs.close();
		return fileStatuses;
	}

	/**
	 * 读取文件内容
	 *
	 * @param conf
	 * @param filePath
	 * @return
	 * @throws IOException
	 */
	public static String read(Configuration conf, String filePath)
			throws IOException {
		String fileContent = null;
		FileSystem fs = FileSystem.get(conf);
		Path path = new Path(filePath);
		InputStream inputStream = null;
		ByteArrayOutputStream outputStream = null;
		try {
			inputStream = fs.open(path);
			outputStream = new ByteArrayOutputStream(inputStream.available());
			IOUtils.copyBytes(inputStream, outputStream, conf);
			fileContent = outputStream.toString();
		} finally {
			IOUtils.closeStream(inputStream);
			IOUtils.closeStream(outputStream);
			fs.close();
		}
		return fileContent;
	}

	/**
	 * 返回系统配置
	 *
	 * @param conf
	 * @throws IOException
	 */
	public static String getConfiguration(Configuration conf) throws IOException {
		FileSystem fs = FileSystem.get(conf);
		Iterator<Entry<String, String>> entrys = fs.getConf().iterator();
		StringBuffer sb = new StringBuffer();
		while (entrys.hasNext()) {
			Entry<String, String> item = entrys.next();
			sb.append(item.getKey()).append(": ").append(item.getValue())
					.append("\n");
		}
		return sb.toString();
	}
	
	/**
	 * 返回数据节点信息
	 * @param conf
	 * @return
	 * @throws IOException
	 */
	public static String getDatanodeReport(Configuration conf) throws IOException {
		FileSystem fs = FileSystem.get(conf);
		DistributedFileSystem dfs = (DistributedFileSystem) fs;
		StringBuffer sb = new StringBuffer();
		try {
			DatanodeInfo[] infos = dfs.getDataNodeStats();
			for (DatanodeInfo node : infos) {
				sb.append(node.getDatanodeReport()).append("\n");
			}
		} catch (Exception e) {
		}
		return sb.toString();
	}

	public static void main(String[] args) throws IOException {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://192.168.230.148:9000");
		// System.out.println(readFile(conf, "/input/input.txt"));
		System.out.println(getDatanodeReport(conf));
	}
}