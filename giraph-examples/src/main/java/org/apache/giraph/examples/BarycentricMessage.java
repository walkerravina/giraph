package org.apache.giraph.examples;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class BarycentricMessage implements Writable{
	
	public static final int POSITION = 0;
	/**
	* the id of the vertex initiating this message.
	*/
	private long sourceId;
	
	
	/**
	* the first value of this message
	*/
	private double value1;
	/**
	 * the second value for this message
	 */
	private long value2;
	
	public BarycentricMessage() {
	}
	/**
	* Constructor used by {@link org.apache.giraph.examples
	* .SimpleHopsComputation}
	*
	* @param sourceId the id of the source vertex which wants to
	* calculate the hops count
	* @param destinationId the id of the destination vertex between which the
	* hops count will be calculated
	*/
	public BarycentricMessage(long sourceId, double value1, long value2) {
		this.sourceId = sourceId;
		this.value1 = value1;
		this.value2 = value2;
	}
	
	public long getSourceId() {
		return this.sourceId;
	}
	public double getValue1() {
		return this.value1;
	}
	public long getValue2(){
		return this.value2;
	}
	
	@Override
	public void write(DataOutput dataOutput) throws IOException {
		dataOutput.writeLong(this.sourceId);
		dataOutput.writeDouble(this.value1);
		dataOutput.writeLong(this.value2);
	}
	@Override
	public void readFields(DataInput dataInput) throws IOException {
		this.sourceId = dataInput.readLong();
		this.value1 = dataInput.readDouble();
		this.value2 = dataInput.readLong();
	}
}
