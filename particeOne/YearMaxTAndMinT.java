package com.particeOne;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class YearMaxTAndMinT implements Writable{

	private String year;
	private double maxTemp;
	private double minTemp;
	
	public YearMaxTAndMinT() {
		super();
		// TODO Auto-generated constructor stub
	}

	public String getYear() {
		return year;
	}

	public void setYear(String year) {
		this.year = year;
	}

	public double getMaxTemp() {
		return maxTemp;
	}

	public void setMaxTemp(double maxTemp) {
		this.maxTemp = maxTemp;
	}

	public double getMinTemp() {
		return minTemp;
	}

	public void setMinTemp(double minTemp) {
		this.minTemp = minTemp;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		 this.year=in.readUTF();
         this.maxTemp=in.readDouble();
         this.minTemp=in.readDouble();     
		
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeUTF(year);
        out.writeDouble(maxTemp);
        out.writeDouble(minTemp); 
	}

	@Override
	public String toString() {
		return "{\"year\":\"" + year + "\", \"maxTemp\":\"" + maxTemp + "\", \"minTemp\":\"" + minTemp + "\"}";
	}
	
	
}
