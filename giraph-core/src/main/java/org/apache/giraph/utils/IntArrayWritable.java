package org.apache.giraph.utils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Writable;

/**
 * Class for storing an array of integer values that implements writable
 * @author Walker Ravina
 *
 */
public class IntArrayWritable implements Writable {

	private ArrayList<Integer> list;
	
	public IntArrayWritable(){
		this.list = new ArrayList<Integer>();
	}
	
	public IntArrayWritable(int item){
		this.list = new ArrayList<Integer>();
		this.list.add(item);
	}
	
	public ArrayList<Integer> getArrayList(){
		return this.list;
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		int size = in.readInt();
		this.list = new ArrayList<Integer>(size);
		for(int i = 0; i < size; i++){
			this.list.add(in.readInt());
		}
		
		
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeInt(this.list.size());
		for(int i = 0; i < this.list.size(); i++){
			out.writeInt(this.list.get(i));
		}
		
	}

}
