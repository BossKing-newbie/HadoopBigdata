package com.SelectData;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
public class SelectData{
	public static class SelectDataMapper extends Mapper<LongWritable, Text,Text,Text> {
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text,Text,Text>.Context context)
				throws IOException, InterruptedException {
			String[] val=value.toString().split(",");
			if(val[1].contains("2016-01") || val[1].contains("2016-02")){
				context.write(new Text(val[0]),new Text(val[1]));
			}
		}

	}
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		conf.set("fs.default.name","hdfs://localhost:9000");
		String[] otherArgs = new String[]{"SelectDataInput","SelectDataOutput"}; /* 直接设置输入参数 */
		if (otherArgs.length != 2) {
			System.err.println("Usage: wordcount <in> <out>");
			System.exit(2);
			}
		Job job=Job.getInstance(conf, "selectdata");
		job.setJarByClass(SelectData.class);
		job.setMapperClass(SelectDataMapper.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(TextInputFormat.class);//设置输入格式
		job.setOutputFormatClass(SequenceFileOutputFormat.class);//设置输出格式
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}


