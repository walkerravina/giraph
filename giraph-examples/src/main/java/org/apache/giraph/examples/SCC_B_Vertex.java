package org.apache.giraph.examples;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.PriorityQueue;
import java.util.Random;

import org.apache.giraph.graph.Vertex;
import org.apache.giraph.utils.IntArrayWritable;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;

/**
 * Vertex class for finding the Strongly Connected Components (SCC)
 * for a directed graph. Using a different approach than SCCVertex class
 * 
 * @author Walker Ravina
 *
 */
public class SCC_B_Vertex extends Vertex <IntWritable, IntWritable, NullWritable, SCC_B_Message>{

	//store the transpose graph
	private ArrayList<IntWritable> transposeNeighbors = new ArrayList<IntWritable>();
	//once a vertex finds its SCC it becomes inactive and ignores all messages sent to it
	private PriorityQueue<Integer> forward_values = new PriorityQueue<Integer>();
	private PriorityQueue<Integer> backward_values = new PriorityQueue<Integer>();
	private boolean inActive = false;
	private boolean wants_to_propogate = false;
	
	@Override
	public void compute(Iterable<SCC_B_Message> messages) throws IOException {
		if(!inActive){
			//steps 0 and 1 form G^R
			if(getSuperstep() == 0){
				sendMessageToAllEdges(new SCC_B_Message(getId().get(), SCC_B_Message.FOWARD));
				setValue(new IntWritable(Integer.MIN_VALUE));
			}
			else if(getSuperstep() == 1){
				for(SCC_B_Message m : messages){
					transposeNeighbors.add(new IntWritable(m.getSourceId()));
				}
			}
			else if(getAggregatedValue(SCC_B_Master.PHASE_AGGREGATOR).equals(SCC_B_Master.TRAVERSAL_SELECTION_1)){
				//if our value is not Integer.MIN_VALUE then we found our SCC in the last attempt and should stop
				//being active from here out
				this.forward_values.clear();
				this.backward_values.clear();
				this.wants_to_propogate = false;
				//this is the first time we become inactive
				if(!this.inActive && getValue().get() != Integer.MIN_VALUE){
					this.inActive = true;
					aggregate(SCC_B_Master.INACTIVE_COUNT_AGGREGATOR, new IntWritable(1));
					voteToHalt();
				}
				else if(!this.inActive){
					//with probability p we attempt to serve as a propagating vertex
					//p is dynamically calculated based off k
					int k = SCC_B_Master.K.get(getConf());
					Random r = new Random();
					IntWritable sleeping = getAggregatedValue(SCC_B_Master.INACTIVE_COUNT_AGGREGATOR);
					double p = (double) k / (Math.max(getTotalNumVertices() - sleeping.get(), 1));
					if(r.nextDouble() <= p){
						this.wants_to_propogate = true;
						aggregate(SCC_B_Master.PROP_AGGREGATOR, new IntArrayWritable(getId().get()));
					}
				}
			}
			else if(getAggregatedValue(SCC_B_Master.PHASE_AGGREGATOR).equals(SCC_B_Master.TRAVERSAL_SELECTION_2)){
				//if we voted to propagate on the last iteration
				if(this.wants_to_propogate){
					IntArrayWritable choices = getAggregatedValue(SCC_B_Master.PROP_AGGREGATOR);
					ArrayList<Integer> l = choices.getArrayList();
					int id = getId().get();
					int upper = Math.min(l.size(), SCC_B_Master.K.get(getConf()));
					for(int i = 0; i < upper; i++){
						if(l.get(i) == id){
							sendMessageToAllEdges(new SCC_B_Message(id, SCC_B_Message.FOWARD));
							for(IntWritable v : this.transposeNeighbors){
								sendMessage(v, new SCC_B_Message(id, SCC_B_Message.BACKWARD));
							}
							setValue(getId());
							break;
						}
					}
				}
			}
			else if(getAggregatedValue(SCC_B_Master.PHASE_AGGREGATOR).equals(SCC_B_Master.TRAVERSAL_MAIN)){
				boolean valueChanged = false;
				//update heaps
				for(SCC_B_Message m : messages){
					if(m.getType() == SCC_B_Message.FOWARD && m.getSourceId() > getValue().get()){
						this.forward_values.add(m.getSourceId());
						valueChanged = true;
						sendMessageToAllEdges(new SCC_B_Message(m.getSourceId(), SCC_B_Message.FOWARD));
					}
					else if(m.getType() == SCC_B_Message.BACKWARD && m.getSourceId() > getValue().get()){
						this.backward_values.add(m.getSourceId());
						valueChanged = true;
						for(IntWritable v : this.transposeNeighbors){
							sendMessage(v, new SCC_B_Message(m.getSourceId(), SCC_B_Message.BACKWARD));
						}
					}
				}
				//search for higher value and update if needed
				for(SCC_B_Message m : messages){
					if(m.getSourceId() > getValue().get() && this.forward_values.contains(m.getSourceId()) 
							&& this.backward_values.contains(m.getSourceId())){
						setValue(new IntWritable(m.getSourceId()));
					}
				}
				//cleanup the heaps
				while(!this.forward_values.isEmpty() && getValue().get() > this.forward_values.peek()){
					this.forward_values.poll();
				}
				while(!this.backward_values.isEmpty() && getValue().get() > this.backward_values.peek()){
					this.backward_values.poll();
				}
				if(valueChanged){
					aggregate(SCC_B_Master.TRAVERSAL_AGGREGATOR, new BooleanWritable(true));
				}
			}			
		}
		else{
			voteToHalt();			
		}	
	}

}