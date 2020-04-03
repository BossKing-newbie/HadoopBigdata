package com.particeOne;

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MaxTAndMinTReducer extends Reducer<Text, DoubleWritable, NullWritable, YearMaxTAndMinT> {
	private YearMaxTAndMinT year_max_min = new YearMaxTAndMinT();

	@Override
	protected void reduce(Text key, Iterable<DoubleWritable> values,
			Reducer<Text, DoubleWritable, NullWritable, YearMaxTAndMinT>.Context context)
			throws IOException, InterruptedException {
		double maxValue = Double.MIN_VALUE;// 获取整形最大值

		double minValue = Double.MAX_VALUE;// 获取最小值
		for (DoubleWritable value : values) {
			maxValue = Math.max(maxValue, value.get());// 获取最高温度
			minValue = Math.min(minValue, value.get());
		}
		year_max_min.setYear(key.toString());
		year_max_min.setMaxTemp(maxValue);
		year_max_min.setMinTemp(minValue);
		context.write(NullWritable.get(), year_max_min);
	}
}
