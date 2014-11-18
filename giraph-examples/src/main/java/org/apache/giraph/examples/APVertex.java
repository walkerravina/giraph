package org.apache.giraph.examples;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

/**
 * Vertex for performing Affinity Propagation Clustering on a data set
 * Every vertex must have a self loop reflecting its a priori preference as a exemplar
 * The input graph need not be complete, missing similarities of form s(i,k) are treated
 * as having value of negative infinity
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
				}
				value = e.getValue().get() - value;
				//update the old responsibility
				old_responsibilities.put(e.getTargetVertexId().get(), value);
				sendMessage(e.getTargetVertexId(), new APMessage(getId().get(), value));
			}
		}
		else if(getAggregatedValue(APMaster.PHASE_AGGREGATOR).equals(APMaster.RESPONSIBILITY_UPDATE)){
			update_responsibilities(messages);
		}
		else if(getAggregatedValue(APMaster.PHASE_AGGREGATOR).equals(APMaster.AVAILABILITY_UPDATE)){
			update_availibilities(messages);
		}
		else if(getAggregatedValue(APMaster.PHASE_AGGREGATOR).equals(APMaster.ASSIGNMENT)){
			//determine cluster and vote to halt, this step can only happen when messages contain availabilities
			compute_assignment(messages);
		}
		else if(getAggregatedValue(APMaster.PHASE_AGGREGATOR).equals(APMaster.CHECK_CONSISTENCY_1)){
			//notify  my exemplar that I have chosen them
			sendMessage(new LongWritable((long)getValue().get()), new APMessage(0,0));
		}
		else if(getAggregatedValue(APMaster.PHASE_AGGREGATOR).equals(APMaster.CHECK_CONSISTENCY_2)){
			//aggregate a true value if the label is not consistent with being an exemplar
			if(messages.iterator().hasNext() && 
					!(new LongWritable((long)getValue().get())).equals(getId())){
				aggregate(APMaster.CONSISTENCY_AGGREGATOR, new BooleanWritable(true));
			}
			//if this point has chosen itself as an exemplar add it to the list of possible
			//exemplars for kmedoids
			if((new LongWritable((long)getValue().get())).equals(getId())){
				aggregate(APMaster.KMEDOIDS_AGGREGATOR, new Text(getId().get() + ","));
			}
		}
		else if(getAggregatedValue(APMaster.PHASE_AGGREGATOR).equals(APMaster.KMEDOIDS)){
			kmedoids_selection();
		}
		else if(getAggregatedValue(APMaster.PHASE_AGGREGATOR).equals(APMaster.HALT)){
			System.out.println("about to halt");
			voteToHalt();
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
		//respond to each responsibility update
		for(APMessage m1 : messages){
			value = 0;
			for(APMessage m2 : messages){
				if(m2.getSourceId() == m1.getSourceId()){
					continue;
				}
				if((new LongWritable(m2.getSourceId())).equals(getId())){
					self_responsibility = m2.getValue();
					continue;
				}
				value += Math.max(0, m2.getValue());
			}
			//formula for a(i,k) i!=k
			if(!(new LongWritable(m1.getSourceId())).equals(getId())){
				value = Math.min(0, value + self_responsibility);
			}
			//for a(k,k) the value is just the sum
			value = lambda * (getSuperstep() == 1 ? 0 : old_availabilities.get(m1.getSourceId()))
					+ (1 - lambda) * value;
			sendMessage(new LongWritable(m1.getSourceId()), new APMessage(getId().get(), value));
			old_availabilities.put(m1.getSourceId(), value);
		}
	}
	
	/**
	 * Assign this vertex to an exemplar based on the availabilities contained in
	 * the messages, and the responsibilities computed from these availabilities.
	 * Set the vertex value
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
			//for each choice of k compute the responsibility based on the current availabilities
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
			//store these values in the case of consistency checks for later
			if(APMaster.CONVERGANCE.get(getConf())){
				old_availabilities.put(m1.getSourceId(), m1.getValue());
				old_responsibilities.put(m1.getSourceId(), total_value - m1.getValue());	
			}
			if(total_value > total_max_value){
				total_max_value = total_value;
				max_k = m1.getSourceId();
			}
		}
		setValue(new DoubleWritable(max_k));
	}
	
	/**
	 * Out of the available exemplars, choose the one which is optimal for this point
	 */
	public void kmedoids_selection(){
		System.out.println("In Kmedoids selection");
		String s = getAggregatedValue(APMaster.KMEDOIDS_AGGREGATOR).toString();
		if(s.equals("")){
			//flag indicating that no point chose itself as an exemplar and 
			//thus sufficient iterations were not run
			setValue(new DoubleWritable(Double.NaN));
			return;
		}
		s = s.substring(0, s.length() - 2);
		System.out.println(s);
		String[] parts = s.split(",");
		long best_k = 0, k = 0;
		double best_value = Double.NEGATIVE_INFINITY, value = 0;
		for(int i = 0; i < parts.length; i++){
			System.out.println(parts[i]);
			/*
			 k = (long)Double.parseDouble(parts[i]);
			if(old_availabilities.containsKey(k) && old_responsibilities.containsKey(k)){
				 value = old_availabilities.get(k) + old_responsibilities.get(k);
				if( value > best_value){
					best_k = k;
					best_value = value;
				}
			}
			*/
		}
		setValue(new DoubleWritable(0));
	}

}
