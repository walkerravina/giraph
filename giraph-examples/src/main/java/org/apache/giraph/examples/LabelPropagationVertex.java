/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */s

package org.apache.giraph.examples;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Implementation of the HCC algorithm that identifies connected components and
 * assigns each vertex its "component identifier" (the smallest vertex id
 * in the component)
 *
 * The idea behind the algorithm is very simple: propagate the smallest
 * vertex id along the edges to all vertices of a connected component. The
 * number of supersteps necessary is equal to the length of the maximum
 * diameter of all components + 1
 *
 * The original Hadoop-based variant of this algorithm was proposed by Kang,
 * Charalampos, Tsourakakis and Faloutsos in
 * "PEGASUS: Mining Peta-Scale Graphs", 2010
 *
 * http://www.cs.cmu.edu/~ukang/papers/PegasusKAIS.pdf
 */
@Algorithm(
    name = "Hello World",
    description = "Prints hello world for each vertex"
)
public class LabelPropagationVertex extends Vertex<IntWritable,
    IntWritable, NullWritable, LongWritable> {
	private Map<Integer, Integer> labelCounter = new HashMap<Integer, Integer>();
	private List<Map.Entry<Integer, Integer>> entryList;
  @Override
  public void compute(Iterable<LongWritable> messages) throws IOException {
	  int max, newVal, neighborOldValue, neighborValue;
	  if(getSuperstep() == 0) {
		  setValue(getId());
		  sendMessageToAllEdges(new LongWritable(packInt(0, getValue().get())));
	  }
	  else if(getSuperstep() == 1){
		  for(LongWritable msg : messages){
			  neighborValue = unpackB(msg.get());
			  labelCounter.put(neighborValue, 1);
		  }
		  entryList = new ArrayList<Map.Entry<Integer, Integer>>(labelCounter.entrySet());
		  Collections.shuffle(entryList);
		  max = -1;
		  newVal = getValue().get();
		  for (Map.Entry<Integer, Integer> entry : entryList) {
			  Integer val = entry.getValue();
			  Integer key = entry.getKey();
			    if(val > max){
			    	max = val;
			    	newVal = key;
			    }
			}
		  if(newVal != getValue().get()){
			  sendMessageToAllEdges(new LongWritable(packInt(getValue().get(), newVal)));
			  setValue(new IntWritable(newVal));
		  }
	  }
	  else{
		  for(LongWritable msg : messages){
			  neighborOldValue = unpackA(msg.get());
			  neighborValue = unpackB(msg.get());
			  labelCounter.put(neighborOldValue, labelCounter.get(neighborOldValue) - 1);
			  if(labelCounter.containsKey(neighborValue)){
				  labelCounter.put(neighborValue, labelCounter.get(neighborValue) + 1);
			  }
			  else{
				  labelCounter.put(neighborValue, 1); 
			  }
		  }
		  entryList = new ArrayList<Map.Entry<Integer, Integer>>(labelCounter.entrySet());
		  Collections.shuffle(entryList);
		  max = -1;
		  newVal = getValue().get();
		  for (Map.Entry<Integer, Integer> entry : entryList) {
			  Integer val = entry.getValue();
			  Integer key = entry.getKey();
			    if(val > max){
			    	max = val;
			    	newVal = key;
			    }
			}
		  if(newVal != getValue().get()){
			  setValue(new IntWritable(newVal));
			  sendMessageToAllEdges(new LongWritable(packInt(getId().get(), getValue().get())));
		  }
	  }
	  System.out.printf("%d: (%d, %d)\n", getSuperstep(), getId().get(), getValue().get());
	  voteToHalt();
  }
  
  private void incrementKey(int key){
	  if(labelCounter.containsKey(key)){
		  labelCounter.put(key, labelCounter.get(key) + 1);
	  }
	  else{
		  labelCounter.put(key, 1);
	  }
  }
  
  private void decrementKey(int key){
	  	int oldValue = labelCounter.get(key);
	  	if(oldValue == 1){
	  		
	  	}
		  labelCounter.put(key, labelCounter.get(key) + 1);
  }
  
  private long packInt(int a, int b){
	  return (long)getId().get() << 32 | (long)getValue().get() & 0xFFFFFFFL;
  }
  
  private int unpackA(long c){
	  return (int)(c >> 32);
  }
  
  private int unpackB(long c){
	  return (int)(c & 0xFFFFFFFFL);
  }
}