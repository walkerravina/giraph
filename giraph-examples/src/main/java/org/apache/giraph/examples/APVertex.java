package org.apache.giraph.examples;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;

/**
 * Vertex for performing Affinity Propagation Clustering on a data set
 * 
 * @author Walker Ravina
 *
 */
public class APVertex extends Vertex<LongWritable, DoubleWritable, DoubleWritable, APMessage>{

	private Map<Long, Double> old_responsibilities;
	private Map<Long, Double> old_availabilities;
	
	@Override
	public void compute(Iterable<APMessage> messages) throws IOException {
		if(getSuperstep() == 0){
			//Initialize maps
			old_responsibilities = new HashMap<Long, Double>();
			old_availabilities = new HashMap<Long, Double>();
			//the first round of updates are special since the availabilities start at 0
			double value;
			for(Edge<LongWritable, DoubleWritable> e: getEdges()){
				value = Double.NEGATIVE_INFINITY;
				for(Edge<LongWritable, DoubleWritable> t: getEdges()){
					if(t.getTargetVertexId().equals(e.getTargetVertexId())){
						continue;
					}
					if(t.getValue().get() > value){
						value = t.getValue().get();
					}
					//default the availabilities to 0
					old_availabilities.put(e.getTargetVertexId().get(), 0.0);
				}
				value = e.getValue().get() - value;
				//update the old responsibility
				old_responsibilities.put(e.getTargetVertexId().get(), value);
				sendMessage(e.getTargetVertexId(), new APMessage(getId().get(), value));
			}
		}
		else if(getAggregatedValue(APMaster.PHASE_AGGREGATOR).equals(APMaster.ASSIGNMENT)){
			//determine cluster and vote to halt, this step can only happen when messages contain availabilities
			compute_assignment(messages);
		}
		else if(getAggregatedValue(APMaster.PHASE_AGGREGATOR).equals(APMaster.RESPONSIBILITY_UPDATE)){
			update_responsibilities(messages);
		}
		else if(getAggregatedValue(APMaster.PHASE_AGGREGATOR).equals(APMaster.AVAILABILITY_UPDATE)){
			update_availibilities(messages);
		}
	}
	
	/**
	 * Based on the availability values received, generate the
	 * new responsibility values and send them to each respective vertex 
	 * 
	 * @param messages Instances of APMessage class containing the id of the vertex which sent the message
	 * as well as the value associated with the message
	 */
	public void update_responsibilities(Iterable<APMessage> messages){
		double max, value;
		double lambda = APMaster.LAMBDA.get(getConf());
		for(Edge<LongWritable, DoubleWritable> e : getEdges()){
			max = Double.NEGATIVE_INFINITY;
			for(APMessage m : messages){
				if((new LongWritable(m.getSourceId())).equals(e.getTargetVertexId())){
					continue;
				}
				value = m.getValue() + getEdgeValue(new LongWritable(m.getSourceId())).get();
				if(value > max){
					max = value;
				}
			}
			value = e.getValue().get() - max;
			value = lambda * old_responsibilities.get(e.getTargetVertexId().get())
					+ (1 - lambda) * value;
			sendMessage(e.getTargetVertexId(), new APMessage(getId().get(), value));
			old_responsibilities.put(e.getTargetVertexId().get(), value);
		}
	}
	
	/**
	 * Based on the responsibility values received, generate the new availability
	 * values and send them to each respective vertex 
	 * 
	 * @param messages Instances of APMessage class containing the id of the vertex which sent the message
	 * as well as the value associated with the message
	 */
	public void update_availibilities(Iterable<APMessage> messages){
		double lambda = APMaster.LAMBDA.get(getConf());
		double value;
		double self_responsibility = 0;
		for(Edge<LongWritable, DoubleWritable> e : getEdges()){
			value = 0;
			for(APMessage m : messages){
				if((new LongWritable(m.getSourceId())).equals(e.getTargetVertexId())){
					continue;
				}
				if((new LongWritable(m.getSourceId())).equals(getId())){
					self_responsibility = m.getValue();
					continue;
				}
				value += Math.max(0, m.getValue());
			}
			//formula for a(i,k) i!=k
			if(!e.getTargetVertexId().equals(getId())){
				value = Math.min(0, value + self_responsibility);
			}
			//for a(k,k) the value is just the sum
			value = lambda * old_availabilities.get(e.getTargetVertexId().get()) 
					+ (1 - lambda) * value;
			sendMessage(e.getTargetVertexId(), new APMessage(getId().get(), value));
			old_availabilities.put(e.getTargetVertexId().get(), value);
		}
	}
	
	/**
	 * Assign this vertex to an exemplar based on the responsibilities contained in
	 * the messages, and the availabilities computed from these responsibilities.
	 * Set the vertex value and vote to halt
	 * 
	 * @param messages Instances of APMessage class containing the id of the vertex which sent the message
	 * as well as the value associated with the message
	 */
	public void compute_assignment(Iterable<APMessage> messages){
		long max_k = 0;
		double total_max_value = Double.NEGATIVE_INFINITY, total_value = 0;
		double resp_max_value = Double.NEGATIVE_INFINITY, resp_value = 0;
		//examine each possible choice of k for the cluster
		//m1 stores the availability a(i,k)
		for(APMessage m1 : messages){
			//System.out.println(m1.getValue());
			//for each choice of k compute the responsibility based on the current availabilities
			resp_max_value = Double.MIN_VALUE;
			//need to loop over the availabilities again to compute the responsibilities
			for(APMessage m2 : messages){
				if(m2.getSourceId() == m1.getSourceId()){
					continue;
				}
				resp_value = m2.getValue() + getEdgeValue(new LongWritable(m2.getSourceId())).get();
				if(resp_value > resp_max_value){
					resp_max_value = resp_value;
				}
			}
			//r(i,k) + a(i,k)
			total_value = getEdgeValue(new LongWritable(m1.getSourceId())).get() - resp_max_value + m1.getValue();
			if(total_value > total_max_value){
				total_max_value = total_value;
				max_k = m1.getSourceId();
			}
		}
		setValue(new DoubleWritable(max_k));
		voteToHalt();
	}

}
