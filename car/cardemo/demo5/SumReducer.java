package car.cardemo.demo5;



import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	
	private IntWritable num = new IntWritable();
	//非营运 {1,1,1,1...}
	protected void reduce(Text key, Iterable<IntWritable> values,
			Context context) throws IOException,InterruptedException{
		//1. sum
		int sum = 0;
		for(IntWritable v : values){
			sum += v.get();
		}
		num.set(sum);
		//2.write to HDFS
		//非营运 10
		context.write(key, num);
	}
}


