package car.cardemo.demo5;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class LineSplitMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	private Text k = new Text();
	private IntWritable one = new IntWritable(1);

	// 山西省,12,长治市,长治城区,2013,BJ6440BKV1A,北汽银翔汽车有限公司,北京,小型普通客
	// 车,个人,非营运,1,BJ415A,1500,75,,4440,,,,,,,,,,,,,,,,北汽银翔汽车有限公司,北京,BJ6440BKV1A,北汽银翔汽车有限公司,,1938,男性
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		// String line = value.toString();
		// 1.split the line
		// String[] arr = line.split(",");

		// if(null != arr && arr.length > 10){

		String line = value.toString();
		String[] arr = line.split(",");
		if (arr.length == 39 && arr != null) {
			// 2.get type
			k.set(arr[10]); // arr[10]:非营运
			// 3.write to next
			// context.write(new Text(arr[10]), new IntWritable(1));
			// 非营运 1
			context.write(k, one);
		}

	}

}
