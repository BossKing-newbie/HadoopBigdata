package com.particeOne;

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MaxTAndMinTMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, DoubleWritable>.Context context)
			throws IOException, InterruptedException {
		String line = value.toString().trim();
		int[] indexs = getIndexs(line);// 获取年份和气温数据的索引范围
		String year = getYear(line, indexs);// 获取年份
		double airTemperature;
		String temperature = getTemperature(line, indexs);
		if (temperature.charAt(0) == '-') { // 每行数据中带 - 号的气温数据做负数处理
			airTemperature = 0 - Double.parseDouble(temperature.substring(1));// 获取气温数值
		} else {
			airTemperature = Double.parseDouble(temperature);// 获取气温数值
		}
		context.write(new Text(year), new DoubleWritable(airTemperature));
	}

	/**
	 * 
	 * 获取年份
	 * 
	 * @param line
	 * @param indexs
	 * @return
	 */

	public String getYear(String line, int[] indexs) {

		return line.substring(indexs[1], indexs[2]).replace(" ", "").substring(0, 4);

	}

	/**
	 * 
	 * 获取气温
	 * 
	 * @param line
	 * 
	 * @param indexs
	 * 
	 * @return
	 * 
	 */

	public String getTemperature(String line, int[] indexs) {
		return line.substring(indexs[2], indexs[3]).replace(" ", "");

	}

	/**
	 * 
	 * 获取年份和气温的索引范围
	 * 
	 * @param line
	 * 
	 * @return
	 * 
	 */

	public int[] getIndexs(String line) {

		int[] indexs = new int[4];

		int n = 0;

		for (int i = 0; i < line.length(); i++) {

			if (line.charAt(i) == ' ') {

				if (line.charAt(i + 1) != ' ') {

					indexs[n++] = i + 1;

				}

				if (n == 4) {

					break;

				}

			}

		}

		return indexs;

	}

}
