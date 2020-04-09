package car.cardemo.sale;

//package cars.e12;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SplitMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	private Text month = new Text();
	private IntWritable num = new IntWritable();

	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		// 1.split line
		String line = value.toString();
		String[] arr = line.split(",");

		// 2. get fields
		// arr[1] month 月份
		// arr[11] num 数量
		if (arr.length > 11 && null != arr[11] && !"".equals(arr[11].trim())) {
			try {
				month.set(arr[1]);
				num.set(Integer.parseInt(arr[11]));
				// 3. write to next
				// 3 1
				// 12 1
				context.write(month, num);
			} catch (Exception e) {
				// do nothing
			}

		}

	}

}

