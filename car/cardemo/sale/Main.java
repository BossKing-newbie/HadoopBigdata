package car.cardemo.sale;

//package cars.e12;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Main {

	public static void main(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException {
		//Job job = new Job(new Configuration(), "month percent");
		
		Configuration conf = new Configuration();
		conf.set("fs.default.name", "hdfs://localhost:9000");
		String[] otherArgs = new String[] { "Car_in", "CarMain_out" }; /* 直接设置输入参数 */
		if (otherArgs.length != 2) {
			System.err.println("Usage: MaxTAndMinT <in> <out>");
			System.exit(2);
		}
		Job job=Job.getInstance(conf, "month percent");
		
		job.setJarByClass(Main.class);

		job.setMapperClass(SplitMapper.class);
		job.setReducerClass(CountReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

		//FileInputFormat.addInputPath(job, new Path("/cars/cars.csv"));
		//FileOutputFormat.setOutputPath(job, new Path("/cars-month-percent"));

		//job.waitForCompletion(true);
		
		
		for (int i = 0; i < otherArgs.length - 1; ++i) {
			FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
		}
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		

	}
}

