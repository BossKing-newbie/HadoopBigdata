package com.SelectData;
//调试通过，以任务5-1为基础，任务5-1已写入"/user/hadoop/output2/part-m-00000"：
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.fs.*;
import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
public class ReadSequenceFile {
	public static void main(String[] args) throws IOException {
		// 获取配置
		Configuration conf = new Configuration();
		// conf.set("fs.default.name","hdfs://localhost:9000");
		conf.set("fs.defaultFS", "hdfs://localhost:9000");
		// conf.set("fs.defaultFS", "master:8020");

		// 获取文件系统
		FileSystem fs = FileSystem.get(conf);
		// 获取SequenceFile.Reader对象
		SequenceFile.Reader reader = new SequenceFile.Reader(fs, new Path("/user/hadoop/SelectDataOutput/part-r-00000"),
				conf);
		// 获取序列化文件中使用的键值类型hadoop/myfile/cs1.txt
		Text key = new Text();
		Text value = new Text();
		// 以下要求有本地文件目录"/usr/local/hadoop/myfile/存在，文件cs1.txt不用创建，可自动创建。
		BufferedWriter out = new BufferedWriter(
				new OutputStreamWriter(new FileOutputStream("/usr/local/hadoop/myfile/cs1.txt", true)));
		while (reader.next(key, value)) {
			out.write(key.toString() + "\t" + value.toString() + "\r\n");
		}
		System.out.println("OK");
		out.close();
		reader.close();
	}
}
