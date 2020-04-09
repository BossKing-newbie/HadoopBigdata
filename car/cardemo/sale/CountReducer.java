package car.cardemo.sale;


//package cars.e12;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CountReducer extends Reducer<Text, IntWritable, Text, DoubleWritable> {

	private DoubleWritable num = new DoubleWritable();
	private Map<String,Integer> map = new HashMap<String,Integer>();
	private int total = 0;

	// 3 {1,1,1}
	// 12 {1,1}
	protected void reduce(Text key, Iterable<IntWritable> values,
			Context context) throws IOException, InterruptedException {
		// 1.sum
		int sum = 0;
		for(IntWritable v : values){
			sum += v.get();
		}
		//2. total: global num
		total += sum;
		//3. put to hashmap
		map.put(key.toString(),sum);
	}

	protected void cleanup(Context context) throws IOException,
			InterruptedException {
		// 4. loop map
		Set<String> keys = map.keySet();
		for(String key : keys){
			//5. process the percent
			int value = map.get(key);
			double percent = value/(double)total;
			// 3 3
			// 12 2
			// 6. write to HDFS
			num.set(percent);
			context.write(new Text(key),num);
		}
		
	}

}

