package examineSeven.secondSort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

//自定义key应该实现的WritableComparable接口
public class NewKey implements WritableComparable<NewKey> {
	long first, second;

	public NewKey() {
	}

	public NewKey(long first, long second) {
		super();
		this.first = first;
		this.second = second;
	}

	@Override
	public String toString() {
		return first + " " + second;
	}

	@Override
	// 序列化，将NewKey转化成使用流传送的二进制
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeLong(first);
		out.writeLong(second);
	}

	@Override
	// 反序列化，从流中的二进制转换成NewKey
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		this.first = in.readLong();
		this.second = in.readLong();
	}

	@Override
	public int compareTo(NewKey o) {
		// key的比较
		long minute = this.first - o.first;
		if (minute != 0) {
			return (int) minute;
		} else {
			// value的比较
			return (int) (this.second - o.second);
		}
	}

}
