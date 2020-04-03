package examineSeven.secondSort;

import java.io.IOException;
import java.net.URI;

import org.apache.commons.digester.parser.GenericParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

public class TwoColDataSort {

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		conf.set("fs.default.name", "hdfs://localhost:9000");
		String[] otherArgs = new String[] { "TwoColDataSort_in", "TwoColDataSort_out" }; /* 直接设置输入参数 */
		if (otherArgs.length != 2) {
			System.err.println("Usage: TwoColDataSort <in> <out>");
			System.exit(2);
		}

		Job job = Job.getInstance(conf, "erpx");

		job.setMapperClass(MyMap.class);

		job.setMapOutputKeyClass(NewKey.class);
		job.setMapOutputValueClass(NullWritable.class);
		// 设置reduce的个数
		job.setNumReduceTasks(1);

		job.setReducerClass(MyReduce.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(NullWritable.class);
		for (int i = 0; i < otherArgs.length - 1; ++i) {
			FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
		}
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

	public static class MyMap extends Mapper<LongWritable, Text, NewKey, NullWritable> {
		@SuppressWarnings("unchecked")
		@Override
		protected void map(LongWritable key, Text value, org.apache.hadoop.mapreduce.Mapper.Context context)
				throws IOException, InterruptedException {

			String[] split = value.toString().split("\t");
			long first = Long.parseLong(split[0]);
			long second = Long.parseLong(split[1]);
			// 在自定义类中进行比较
			NewKey newKey = new NewKey(first, second);
			context.write(newKey, NullWritable.get());
		}
	}

	public static class MyReduce extends Reducer<NewKey, NullWritable, LongWritable, NullWritable> {
		@SuppressWarnings("unchecked")
		protected void reduce(NewKey key, Iterable<NullWritable> values,
				org.apache.hadoop.mapreduce.Reducer.Context context) throws IOException, InterruptedException {

			context.write(key, NullWritable.get());

		}
	}
}
