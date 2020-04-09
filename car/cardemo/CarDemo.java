package car.cardemo;
//品牌制造商的个数

import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class CarDemo {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set("fs.default.name", "hdfs://localhost:9000");
		String[] otherArgs = new String[] { "Car_in", "Car_out" }; /* 直接设置输入参数 */
		if (otherArgs.length != 2) {
			System.err.println("Usage: MaxTAndMinT <in> <out>");
			System.exit(2);
		}
		Job job=Job.getInstance(conf, "pbs");
		
		
		job.setJarByClass(CarDemo.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setMapperClass(CarMap.class);
		job.setReducerClass(CarReduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		for (int i = 0; i < otherArgs.length - 1; ++i) {
			FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
		}
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
		//System.exit(job.waitForCompletion(true)?0:1);
	}
	/**
	 * map()输出以下格式数据:
	 * <长治市，东风小康>
	 * <长治市，上汽通用>
	 * …………………………………………
	 * 以上数据经过shuffle处理之后，形成以下格式：
	 * <长治市，{东风小康,上汽通用,上汽通用,上汽通用,上汽通用……}>
	 * @author zkpk
	 *
	 */
	static class CarMap extends Mapper<LongWritable, Text, Text, Text>{
		Text city=new Text();//城市
		Text carFac=new Text();//汽车制造商
		protected void map(LongWritable key, Text value, org.apache.hadoop.mapreduce.Mapper<LongWritable,Text,Text,Text>.Context context) throws java.io.IOException ,InterruptedException {
			//String[] lines = value.toString().split("\t");
			//String[] lines = value.toString().split("\t");
			//if(lines.length==3 && lines!=null){
				//city.set(lines[1]);
				//carFac.set(lines[0]);
			String line = value.toString();
			String[] arr = line.split(",");
			if(arr.length==39 && arr!=null){
				city.set(arr[2]);
				carFac.set(arr[6]);
				context.write(city, carFac);
			}
		};
	}
	static class CarReduce extends Reducer<Text, Text, Text,Text>{
		protected void reduce(Text key, java.lang.Iterable<Text> values, org.apache.hadoop.mapreduce.Reducer<Text,Text,Text,Text>.Context context) throws java.io.IOException ,InterruptedException {
			Set<String> sets=new HashSet<String>();
			for (Text v : values) {
				//添加相同的数据，Set集合会去重。
				sets.add(v.toString()); 
			}
			//集合去重之后，集合中的元素个数即为汽车制造商的个数。
			String count=String.valueOf(sets.size());
			context.write(key, new Text(count));
		};
	}
}

