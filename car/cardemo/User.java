package car.cardemo;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import car.cardemo.demo5.Main;

class UserMap extends Mapper<Object, Text, Text, IntWritable> {
	public void map(Object key, Text value,
			org.apache.hadoop.mapreduce.Mapper<Object, Text, Text, IntWritable>.Context context)
			throws IOException, InterruptedException {
		String[] str = value.toString().trim().split(",");
		if (str != null && str.length == 39 && str[8] != null && str[37] != null && str[37].matches("^\\d*$")
				&& str[38] != null) {
			int age = Integer.parseInt(str[4]) - Integer.parseInt(str[37]);
			int range1 = age / 10 * 10;
			int range2 = range1 + 10;
			context.write(new Text(str[8] + "," + (range1 + "-" + range2) + "," + str[38]), new IntWritable(1));
		}
	}
};

class UserReduce extends Reducer<Text, IntWritable, Text, Text> {
	public void reduce(Text key, java.lang.Iterable<IntWritable> values,
			Reducer<Text, IntWritable, Text, Text>.Context context) throws IOException, InterruptedException {
		int sum = 0;
		for (IntWritable val : values) {
			sum += val.get();
		}
		context.write(key, new Text("" + sum));
	}
};

public class User {
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://localhost:9000");
		String[] otherArgs = new String[] { "Car_in", "CarUser_out" }; /* 直接设置输入参数 */
		if (otherArgs.length != 2) {
			System.err.println("Usage: MaxTAndMinT <in> <out>");
			System.exit(2);
		}
		Job job = Job.getInstance(conf, "type count");
		job.setJarByClass(Main.class);

		job.setMapperClass(UserMap.class);
		job.setReducerClass(UserReduce.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		for (int i = 0; i < otherArgs.length - 1; ++i) {
			FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
		}
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
