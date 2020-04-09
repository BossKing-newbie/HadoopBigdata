package car.cardemo;

//每个月每种品牌的销售数量

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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

public class SaleCount {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set("fs.default.name", "hdfs://localhost:9000");
		String[] otherArgs = new String[] { "Car_in", "CarSaleCount_out" }; /* 直接设置输入参数 */
		if (otherArgs.length != 2) {
			System.err.println("Usage: MaxTAndMinT <in> <out>");
			System.exit(2);
		}
		Job job=Job.getInstance(conf, "SaleCount");
		//Job job=new Job(new Configuration(),"SaleCount");	
		
		
		job.setJarByClass(SaleCount.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setMapperClass(SaleMap.class);
		job.setReducerClass(SaleReduce.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		//FileInputFormat.setInputPaths(job, new Path(args[0]));
		//FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		//job.waitForCompletion(true);
		
		for (int i = 0; i < otherArgs.length - 1; ++i) {
			FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
		}
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		
	}
	//3	东风		1
	//12 北京	1
	//经过map之后变成
	//<3-东风,1> 
	//<12-北京,1>

	static class SaleMap extends Mapper<LongWritable, Text, Text, IntWritable>{
		protected void map(LongWritable key, Text value, org.apache.hadoop.mapreduce.Mapper<LongWritable,Text,Text,IntWritable>.Context context) throws java.io.IOException ,InterruptedException {
			//String[] lines = value.toString().split("\t");
			//if(lines!=null && lines.length==6){
				
				String line = value.toString();
				String[] arr = line.split(",");
				if(arr.length==39 && arr!=null){
				String month=arr[1]; //月份
				String brand=arr[7]; //品牗
				String count=arr[11]; //销售数量
				context.write(new Text(month+"-"+brand),
				 new IntWritable(Integer.parseInt(count)));
			}
		};
	}
	//<3-东风,{1,1,1,1……}>
	//<12-北京,｛1,1,1,1……｝>
	static class SaleReduce extends Reducer<Text, IntWritable, Text, Text>{
		protected void reduce(Text key, java.lang.Iterable<IntWritable> values, org.apache.hadoop.mapreduce.Reducer<Text,IntWritable,Text,Text>.Context context) throws java.io.IOException ,InterruptedException {
			int sum=0;
			for (IntWritable times : values) {
				sum=sum+times.get();
			}
			String keyString=key.toString();
			String month=keyString.substring(0,keyString.indexOf("-"));
			String brand=keyString.substring(keyString.indexOf("-"));
			
			context.write(new Text(month), new Text(brand+"\t"+sum));
		};
	}
}

