package com.HandlerExamineOne;

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AvgScoreReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
	@Override
	protected void reduce(Text key, Iterable<DoubleWritable> value,
			Reducer<Text, DoubleWritable, Text, DoubleWritable>.Context context)
			throws IOException, InterruptedException {
		double avgScore = 0.0;
		double sum = 0.0;
		int count = 0;
		for (DoubleWritable val : value) {
			count += 1;
			sum += val.get();
		}
		avgScore = sum / count;
		context.write(key, new DoubleWritable(avgScore));
	}
}
