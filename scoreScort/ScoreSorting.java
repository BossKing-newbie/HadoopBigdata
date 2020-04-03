package com.scoreScort;

import java.io.IOException;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;


import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class ScoreSorting {
    // Mapper模块
        public static class MyMapper
        extends Mapper<LongWritable, Text, Text ,IntWritable>{
            Text course=new Text();
            IntWritable score=new IntWritable();
            public void map(LongWritable key, Text value,
                    Mapper<LongWritable, Text, Text ,IntWritable>.Context context) //map函数的编写要根据读取的文件内容和业务逻辑来写
                    throws IOException, InterruptedException {
                String line = value.toString();
                String array[] = line.trim().split(" ");//trim函数去掉两边多余的空格，指定空格为分隔符，组成数组
                course.set(array[0]);//第一列是科目
                score.set(Integer.parseInt(array[1]));//第二列是分数
                context.write(course, score);
            }
        }
        
        // Reducer模块
        public static class MyReducer
        extends Reducer<Text,IntWritable,Text,IntWritable> {//注意与上面输出对应
            private IntWritable result = new IntWritable();
            public void reduce(Text key, Iterable<IntWritable> values,
                    Reducer<Text,IntWritable,Text,IntWritable>.Context context)  
                    throws IOException, InterruptedException {
                int maxscore=0;                        //初始化最大值
                for (IntWritable score:values) {
                    if(maxscore < score.get()) {
                        maxscore=score.get();        //相同键内找最大值
                    }
                }
                result.set(maxscore);
                context.write(key, result);
                
            }
        }
        
	// Driver模块，主要是配置参数
	public static void main(String[] args) throws Exception { // 对有几个参数要有很强的敏感性，如果多可以用前面的遍历方式，如果少就可以直接指定。
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		conf.set("fs.default.name", "hdfs://localhost:9000");
		String[] otherArgs = new String[] { "ScoreSorting_input", "ScoreSorting_output" }; /* 直接设置输入参数 */
		if (otherArgs.length != 2) {
			System.err.println("Usage: wordcount <in> <out>");
			System.exit(2);
		}
		Job job = Job.getInstance(conf, "ScoreSorting");
		job.setJarByClass(ScoreSorting.class);
		job.setMapperClass(MyMapper.class);
		job.setCombinerClass(MyReducer.class);
		job.setReducerClass(MyReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
