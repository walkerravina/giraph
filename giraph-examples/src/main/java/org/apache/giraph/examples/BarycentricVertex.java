package org.apache.giraph.examples;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;

public class BarycentricVertex extends Vertex<LongWritable, DoubleWritable, DoubleWritable, BarycentricMessage> {

	private double degree;
	//position vector for this vertex, index i holds the position on the ith round
	private ArrayList<Double> values = new ArrayList<Double>();
	private HashMap<Long, Double> edge_lengths = new HashMap<Long, Double>();;
	private double neighborhood_sum;
	
	@Override
	public void compute(Iterable<BarycentricMessage> messages) throws IOException {
		//compute degree
		if(getSuperstep() == 0){
			double total = 0;
			for(Edge<LongWritable, DoubleWritable> e : getEdges()){
				total += e.getValue().get();
			}
			//initialize position vector with 1 slot for each restart and random normally distributed values
			int restarts =  (int) BarycentricMaster.RESTARTS.get(getConf());
			Random r = new Random();
			for(int i = 0; i < restarts; i++){
				this.values.add(r.nextGaussian());
			}
			this.degree = total;
			//send initial position updates
			sendMessageToAllEdges(new BarycentricMessage(getId().get(), this.values));
			//set initial connected component for later
			setValue(new DoubleWritable(getId().get()));
		}
		else if(getAggregatedValue(BarycentricMaster.PHASE_AGGREGATOR).equals(BarycentricMaster.UPDATE_POSITION)){
			update_values(messages);
		}
		else if(getAggregatedValue(BarycentricMaster.PHASE_AGGREGATOR).equals(BarycentricMaster.COMPUTE_EDGE_LENGTHS)){
			compute_edge_lengths(messages);
			compute_neighborhood();
		}
		//TODO: implement slackening
		else if(getAggregatedValue(BarycentricMaster.PHASE_AGGREGATOR).equals(BarycentricMaster.SLACKEN_1)){
		}
		else if(getAggregatedValue(BarycentricMaster.PHASE_AGGREGATOR).equals(BarycentricMaster.CUT_EDGES)){
			cut_edges(messages);
		}
		else if(getAggregatedValue(BarycentricMaster.PHASE_AGGREGATOR).equals(BarycentricMaster.FIND_COMPONENTS)){
			find_components(messages);
		}
		//TODO: implement cleanup
		else if(getAggregatedValue(BarycentricMaster.PHASE_AGGREGATOR).equals(BarycentricMaster.CLEANUP_1)){
			
		}
		else if(getAggregatedValue(BarycentricMaster.PHASE_AGGREGATOR).equals(BarycentricMaster.CLEANUP_2)){
			
		}
		else if(getAggregatedValue(BarycentricMaster.PHASE_AGGREGATOR).equals(BarycentricMaster.HALT)){
			voteToHalt();
		}
	}
	
	/**
	 * Recompute the position of this vertex based on the values
	 * of its neighbors. Perform this for all restarts in parallel.
	 * 
	 * @param messages Messages containing the id of their sender vertex and
	 * the position vector of that vertex
	 */
	public void update_values(Iterable<BarycentricMessage> messages){
		//perform the self position updates for each restart
		for(int i = 0; i < this.values.size(); i++){
			double curr = this.values.get(i);
			this.values.set(i, curr / (this.degree + 1));
		}
		//average the neighbors for each restart
		for(BarycentricMessage m : messages){
			//for this neighbor update the position for all the restarts
			List<Double> pos_updates = m.getvalues();
			for(int i = 0; i < pos_updates.size(); i++){
				double curr = this.values.get(i);
				this.values.set(i, curr + getEdgeValue(new LongWritable(m.getSourceId())).get() * pos_updates.get(i) / (this.degree + 1));
			} 
		}
		sendMessageToAllEdges(new BarycentricMessage(getId().get(), this.values));
	}
	
	/**
	 * Compute the average edge lengths for all runs
	 * and add this information to the hash map for edge lengths
	 * 
	 * @param messages Messages containing the id of their sender vertex and
	 * the position of that vertex
	 */
	public void compute_edge_lengths(Iterable<BarycentricMessage> messages){
		for(BarycentricMessage m : messages){
			//average the length of this edge over all restarts
			double avg = 0;
			List<Double> neighbor_values = m.getvalues();
			for(int i = 0; i < this.values.size(); i++){
				avg += Math.abs(this.values.get(i) - neighbor_values.get(i));
			}
			avg /= this.values.size();
			this.edge_lengths.put(m.getSourceId(), avg);
		}
	}
	
	/**
	 * Compute the neighborhood of this vertex to be used in slackening or cutting of edges
	 */
	public void compute_neighborhood(){
		//compute sum and size of the one hop neighborhood and send this to appropriate vertexes
		//we only need to send this information to vertexes with lower degree, otherwise we
		//are responsible for the edge
		double sum = 0;
		for(Edge<LongWritable, DoubleWritable> e: getEdges()){
			sum += edge_lengths.get(e.getTargetVertexId().get());
		}
		this.neighborhood_sum = sum;
		long id = getId().get();
		for(Edge<LongWritable, DoubleWritable> e : getEdges()){
			if(e.getTargetVertexId().compareTo(getId()) < 0){
				ArrayList<Double> l = new ArrayList<Double>();
				l.add(sum);
				l.add((double)getNumEdges());
				sendMessage(e.getTargetVertexId(), new BarycentricMessage(id, l));
			}
		}
	}
	
	/**
	 * Delete those edges from the graph whose score is positive (are longer than average)
	 *  
	 * @param messages Messages containing the id of their sender vertex and
	 * the neighborhood sum value of the vertex and the out degree value of the vertex
	 * based on this information we can determine whether to cut the edge
	 * 
	 * @throws IOException 
	 */
	public void cut_edges(Iterable<BarycentricMessage> messages) throws IOException{
		double avg, sum, size;
		//for each message we receive we are responsible for cutting that edge
		for(BarycentricMessage m : messages){
			List<Double> l = m.getvalues();
			sum = l.get(0);
			size = l.get(1);
			avg = (this.neighborhood_sum - edge_lengths.get(m.getSourceId()) + sum)
					/ (getNumEdges() + size - 1);
			if(edge_lengths.get(m.getSourceId()) > avg){
					//remove both edges
					removeEdgesRequest(getId(), new LongWritable(m.getSourceId()));
					removeEdgesRequest(new LongWritable(m.getSourceId()), getId());
			}
			//begin WCC traversal
			else{
				sendMessage(new LongWritable(m.getSourceId()), new BarycentricMessage(getId().get(), new ArrayList<Double>()));
			}
		}
	}
	
	/**
	 * Simple WCC algorithm to find the clusters after the edges have been cut
	 * @param messages
	 * @throws IOException
	 */
	public void find_components(Iterable<BarycentricMessage> messages) throws IOException{
		boolean changed = false;
		for(BarycentricMessage m : messages){
			if(m.getSourceId() < getValue().get()){
				setValue(new DoubleWritable(m.getSourceId()));
				changed = true;
				
			}
		}
		if(changed){
			aggregate(BarycentricMaster.TRAVERSAL_AGGREGATOR, new BooleanWritable(true));
			sendMessageToAllEdges(new BarycentricMessage((long)getValue().get(), new ArrayList<Double>()));
		}
	}


}
