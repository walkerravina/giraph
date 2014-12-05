package org.apache.giraph.examples;

import java.util.ArrayList;
import java.util.List;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * Message for the Barycentric Vertex class
 * @author cloudera
 *
 */
public class BarycentricMessage implements Writable{
	
	public static final int POSITION = 0;
	/**
	* the id of the vertex initiating this message.
	*/
	private long sourceId;
	
	
	/**
	* the first value of this message
	*/
	private ArrayList<Double> values;
	
	public BarycentricMessage() {
	}
	/**
	* Constructor used by {@link org.apache.giraph.examples
	* .SimpleHopsComputation}
	*
	* @param sourceId the id of the source vertex 
	* @param values the array of values for this message
	*/
	public BarycentricMessage(long sourceId, ArrayList<Double> values) {
		this.sourceId = sourceId;
		this.values = values;
	}
	
	public long getSourceId() {
		return this.sourceId;
	}
	public ArrayList<Double> getvalues() {
		return this.values;
	}

	
	@Override
	public void write(DataOutput dataOutput) throws IOException {
		dataOutput.writeLong(this.sourceId);
		dataOutput.writeInt(this.values.size());
		for(Double d: this.values){
			dataOutput.writeDouble(d);
		}
	}
	@Override
	public void readFields(DataInput dataInput) throws IOException {
		this.sourceId = dataInput.readLong();
		int length = dataInput.readInt();
		this.values = new ArrayList<Double>();
		for(int i = 0; i < length; i++){
			this.values.add(dataInput.readDouble());
		}
	}
}
