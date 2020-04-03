package com.particeOne;

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MaxTAndMinTCombiner extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
	@Override
	protected void reduce(Text key, Iterable<DoubleWritable> values,
			Reducer<Text, DoubleWritable, Text, DoubleWritable>.Context context)
			throws IOException, InterruptedException {
		double maxValue = Double.MIN_VALUE;// 获取整形最大值
		double minValue = Double.MAX_VALUE;// 获取最小值
		for (DoubleWritable value : values) {
			maxValue = Math.max(maxValue, value.get());// 获取最高温度
			minValue = Math.min(minValue, value.get());// 获取最低温度
		}
		context.write(key, new DoubleWritable(maxValue));
		context.write(key, new DoubleWritable(minValue));

	}
}
