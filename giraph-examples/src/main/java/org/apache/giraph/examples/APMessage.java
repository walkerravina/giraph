/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements. See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership. The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License. You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package org.apache.giraph.examples;
import org.apache.hadoop.io.Writable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
/**
* Message-Type used by AP Vertex (Affinity Propagation) for communication.
*/
public class APMessage implements Writable {
	/**
	* the id of the vertex initiating this message.
	*/
	private long sourceId;
	
	/**
	* the value of this message
	*/
	private double value;
	
	public APMessage() {
	}
	/**
	* Constructor used by {@link org.apache.giraph.examples
	* .SimpleHopsComputation}
	*
	* @param sourceId the id of the source vertex 
	* @param destinationId the value of the message to be used in calculations
	*/
	public APMessage(long sourceId, double value) {
		this.sourceId = sourceId;
		this.value = value;
	}
	
	public long getSourceId() {
		return this.sourceId;
	}
	public double getValue() {
		return this.value;
	}
	
	@Override
	public void write(DataOutput dataOutput) throws IOException {
		dataOutput.writeLong(this.sourceId);
		dataOutput.writeDouble(this.value);
	}
	@Override
	public void readFields(DataInput dataInput) throws IOException {
		this.sourceId = dataInput.readLong();
		this.value = dataInput.readDouble();
	}
}