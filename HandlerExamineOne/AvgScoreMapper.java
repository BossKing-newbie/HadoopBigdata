package com.HandlerExamineOne;

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AvgScoreMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
	private String splitter = ",";
	private Text name = new Text();
	private DoubleWritable score = new DoubleWritable();

	

	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, DoubleWritable>.Context context)
			throws IOException, InterruptedException {
		String[] val = value.toString().split(splitter);
		name.set(val[0]);
		for (int i = 1; i < val.length; i++) {
			score.set(Double.parseDouble(val[i]));
			context.write(name, score);
		}
	}
}
