package car.cardemo;

//通过不同类型（品牌）车销售情况，来统计发动机型号和燃料种类;
//

import java.io.IOException;
import java.util.HashSet;


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

public class CarDemo2 {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		conf.set("fs.default.name", "hdfs://localhost:9000");
		String[] otherArgs = new String[] { "Car_in", "CarSale_out" }; /* 直接设置输入参数 */
		if (otherArgs.length != 2) {
			System.err.println("Usage: MaxTAndMinT <in> <out>");
			System.exit(2);
		}
		//Job job = new Job(new Configuration(), "type count");
		
		Job job=Job.getInstance(conf, "type count");
		
		
		
		job.setJarByClass(CarDemo2.class);

		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		//job.setOutputKeyClass(Text.class);
		//job.setOutputValueClass(Text.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

		//FileInputFormat.addInputPath(job, new Path(args[0]));
		//FileOutputFormat.setOutputPath(job, new Path(args[1]));

		//job.waitForCompletion(true);
		
		for (int i = 0; i < otherArgs.length - 1; ++i) {
			FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
		}
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	
	static class MyMapper extends Mapper<LongWritable, Text, Text, Text>{
		protected void map(LongWritable key, Text value, Context context) throws java.io.IOException ,InterruptedException {
			String line = value.toString();
			String[] arr = line.split(",");
                                              //arr[12] 发动机型号，arr[15] 燃油种类，arr[7] 车辆品牌
                                              //**:也可以把（arr[7]+"\t"+arr[12]+"\t"+arr[15]）设为key:
                                              //**:context.write(new Text（arr[7]+"\t"+arr[12]+"\t"+arr[15]）, new IntWritable(1));
                                              //**：相应在reduce端：只写一句即可：context.write(key, new Text(""))
			if(arr.length==39 && arr!=null){
			String arr1 = arr[12]+"\t"+arr[15];
			context.write(new Text(arr[7]), new Text(arr1));
			}
		};
	}
	
	static class MyReducer extends Reducer<Text, Text, Text, Text>{
		protected void reduce(Text key, Iterable<Text> values, Context context) throws java.io.IOException ,InterruptedException {
			HashSet<String> set = new HashSet<String>();
			for (Text text : values) {
				set.add(text.toString());
			}
			for (String string : set) {
				context.write(key, new Text(string));
			}
		
		};
	}

}

