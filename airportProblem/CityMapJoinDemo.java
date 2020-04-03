package examineSeven.airportProblem;

//package CityMapJoin;

import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CityMapJoinDemo {
	public static void main(String[] args) throws Exception {
		// 判断输入路径
		//tips: Set three path the first path is allcity.txt, the second path is somecity.txt,the last path is output-path
		String[] otherArgs = new String[] { "AllCity/allCity.txt", "SomeCity/someCity.txt","CityMapJoinDemo" }; /* 直接设置输入参数 */
		if (otherArgs.length != 3 || otherArgs == null) {
			System.err.println("Please Input Full Path!");
			System.exit(1);
		}
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://localhost:9000");
		
		// 创建Job
		Job job = Job.getInstance(conf, CityMapJoinDemo.class.getSimpleName());
		//no necessary set Jar 
		//job.setJarByClass(CityMapJoinDemo.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		// 设置Reducer阶段的处理类
		job.setReducerClass(CityReduce.class);
		// 通过MultipleInputs多输入的方式添加多个Map的处理类
		MultipleInputs.addInputPath(job, new Path(otherArgs[0]), TextInputFormat.class, AllCity.class);
		MultipleInputs.addInputPath(job, new Path(otherArgs[1]), TextInputFormat.class, SomeCity.class);
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));

		// job.waitForCompletion(true);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

	// 处理所有城市的map
	// <济南,a_济南>
	// <青岛,a_青岛>
	// <德州,a_德州>
	static class AllCity extends Mapper<LongWritable, Text, Text, Text> {
		public static final String LABEL = "a_";

		protected void map(LongWritable key, Text value,
				org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, Text>.Context context)
				throws java.io.IOException, InterruptedException {
			String cityName = value.toString();
			System.out.println(cityName);
			context.write(new Text(cityName), new Text(LABEL + cityName));
		};
	}

	// 处理只有飞机场的城市
	// <济南,s_济南 济南飞机场>
	// <青岛,s_青岛 青岛飞机场>
	static class SomeCity extends Mapper<LongWritable, Text, Text, Text> {
		public static final String LABEL = "s_";

		protected void map(LongWritable key, Text value,
				org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, Text>.Context context)
				throws java.io.IOException, InterruptedException {
			String[] lines = value.toString().split(",");
			String cityName = lines[0];
			System.out.println(cityName);
			context.write(new Text(cityName), new Text(LABEL + value.toString()));
		};
	}

	// 经过shuffle之后变成:
	// <济南,{a_济南,s_济南 济南飞机场}>
	// <德州,{a_德州}>
	// 青岛,{a_青岛,s_青岛 青岛机场}
	static class CityReduce extends Reducer<Text, Text, Text, Text> {
		protected void reduce(Text key, java.lang.Iterable<Text> values,
				org.apache.hadoop.mapreduce.Reducer<Text, Text, Text, Text>.Context context)
				throws java.io.IOException, InterruptedException {
			// 城市的名字
			String cityName = null;
			// 存放符合条件过滤出来的城市
			List<String> list = new ArrayList<String>();
			for (Text value : values) {
				// 如果列表中包含有s_开头的数据，则表明该数据是已经有飞机场的城市
				if (value.toString().startsWith(SomeCity.LABEL)) {
					int index = value.toString().indexOf("_");
					cityName = value.toString().substring(index + 1, index + 3);
				} else if (value.toString().startsWith(AllCity.LABEL)) {

					list.add(value.toString().substring(2));
				}
			}
			// 如果城市名为空并且list列表中有值，则列表中的值就是符合条件的数据
			if (cityName == null && list.size() > 0) {
				for (String str : list) {
					context.write(new Text(str), new Text(""));
				}
			}
		};
	}
}
