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
* Message-Type used by KTrusses Vertex for communication.
*/
public class KTrussesMessage implements Writable {
	/**
	* the id of the vertex initiating this message.
	*/
	private long sourceId;
	
	/**
	* the value of this message
	*/
	private long value;
	
	public KTrussesMessage() {
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
	public KTrussesMessage(long sourceId, long value) {
		this.sourceId = sourceId;
		this.value = value;
	}
	
	public long getSourceId() {
		return this.sourceId;
	}
	public long getValue() {
		return this.value;
	}
	
	@Override
	public void write(DataOutput dataOutput) throws IOException {
		dataOutput.writeLong(this.sourceId);
		dataOutput.writeLong(this.value);
	}
	@Override
	public void readFields(DataInput dataInput) throws IOException {
		this.sourceId = dataInput.readLong();
		this.value = dataInput.readLong();
	}
}