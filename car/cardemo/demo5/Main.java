package car.cardemo.demo5;



import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Main {

	public static void main(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException {
		//Job job = new Job(new Configuration(), "type count");
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://localhost:9000");
		conf.set("fs.default.name", "hdfs://localhost:9000");
		String[] otherArgs = new String[] { "Car_in", "Cardemo5_out" }; /* 直接设置输入参数 */
		if (otherArgs.length != 2) {
			System.err.println("Usage: MaxTAndMinT <in> <out>");
			System.exit(2);
		}
		Job job=Job.getInstance(conf, "type count");
		
		
		
		job.setJarByClass(Main.class);

		job.setMapperClass(LineSplitMapper.class);
		job.setReducerClass(SumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		//FileInputFormat.addInputPath(job, new Path("/cars/cars.csv"));
		//FileOutputFormat.setOutputPath(job, new Path("/cars-output"));

		//job.waitForCompletion(true);
		
		for (int i = 0; i < otherArgs.length - 1; ++i) {
			FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
		}
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		
		
	}
}

