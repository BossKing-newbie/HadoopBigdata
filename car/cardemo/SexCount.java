package car.cardemo;
///性别比例
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

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
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class SexCount {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set("fs.default.name", "hdfs://localhost:9000");
		String[] otherArgs = new String[] { "Car_in", "CarSex_out" }; /* 直接设置输入参数 */
		if (otherArgs.length != 2) {
			System.err.println("Usage: MaxTAndMinT <in> <out>");
			System.exit(2);
		}
		Job job=Job.getInstance(conf, "xbxl");
		
		
		job.setJarByClass(SexCount.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setMapperClass(SexMap.class);
		job.setReducerClass(SexReduce.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		
	//	FileInputFormat.setInputPaths(job, new Path(args[0]));
	//	FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		//job.waitForCompletion(true);
		for (int i = 0; i < otherArgs.length - 1; ++i) {
			FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
		}
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		
		
	}
	static class SexMap extends Mapper<LongWritable, Text, Text, IntWritable>{
		public IntWritable one=new IntWritable(1);
		protected void map(LongWritable key, Text value, org.apache.hadoop.mapreduce.Mapper<LongWritable,Text,Text,IntWritable>.Context context) throws java.io.IOException ,InterruptedException {
			String[] lines = value.toString().split(",");
			if(lines!=null && lines.length==39){
				if(lines[38]!=null){
					String sex=lines[38];
					context.write(new Text(sex), one);
			}
			}
		};
	}
	static class SexReduce extends Reducer<Text, IntWritable, Text, DoubleWritable>{
		Map<String, Integer> maps=new HashMap<String,Integer>();
		double total=0;
		
		protected void reduce(Text key, java.lang.Iterable<IntWritable> values, org.apache.hadoop.mapreduce.Reducer<Text,IntWritable,Text,DoubleWritable>.Context context) throws java.io.IOException ,InterruptedException {
			int sum=0;
			for (@SuppressWarnings("unused") IntWritable count : values) {
				sum=sum+count.get();
			}
			total=total+sum;///求出性别总数　
			maps.put(key.toString(), sum);
		};
		protected void cleanup(org.apache.hadoop.mapreduce.Reducer<Text,IntWritable,Text,DoubleWritable>.Context context) throws java.io.IOException ,InterruptedException {
			Set<String> keySet = maps.keySet();
			for (String str : keySet) {
				int value = maps.get(str);
				//求出比例
				double percent=value/total;
				context.write(new Text(str), new DoubleWritable(percent));
			}
			
		};
	}
}


