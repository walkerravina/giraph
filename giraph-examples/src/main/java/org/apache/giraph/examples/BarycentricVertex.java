package org.apache.giraph.examples;

import java.io.IOException;
import java.util.Map;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;

public class BarycentricVertex extends Vertex<LongWritable, DoubleWritable, DoubleWritable, BarycentricMessage> {

	private double degree;
	private Map<Long, Double> edge_lengths;
	
	@Override
	public void compute(Iterable<BarycentricMessage> messages) throws IOException {
		// TODO Auto-generated method stub
		if(getSuperstep() == 0){
			double total = 0;
			for(Edge<LongWritable, DoubleWritable> e : getEdges()){
				total += e.getValue().get();
			}
		}
		else if(getAggregatedValue(BarycentricMaster.PHASE_AGGREGATOR).equals(BarycentricMaster.UPDATE_POSITION)){
			update_position(messages);
		}
		else if(getAggregatedValue(BarycentricMaster.PHASE_AGGREGATOR).equals(BarycentricMaster.COMPUTE_EDGE_LENGTHS_1)){
			//each vertex is responsible for the edges of which it is the minimum vertex id
			//we only need to send our position if we are not responsible for the edge
			for(Edge<LongWritable, DoubleWritable> e : getEdges()){
				if(e.getTargetVertexId().compareTo(getId()) < 0){
					sendMessage(e.getTargetVertexId(), new BarycentricMessage(getId().get(), getValue().get(), 0));
				}
			}
		}
		else if(getAggregatedValue(BarycentricMaster.PHASE_AGGREGATOR).equals(BarycentricMaster.COMPUTE_EDGE_LENGTHS_2)){
			//based on the positions which were sent in the last step, compute the edge lengths
			compute_edge_lengths(messages);
		}
		else if(getAggregatedValue(BarycentricMaster.PHASE_AGGREGATOR).equals(BarycentricMaster.SLACKEN_1)){
			compute_neighborhood();
		}
		else if(getAggregatedValue(BarycentricMaster.PHASE_AGGREGATOR).equals(BarycentricMaster.SLACKEN_2)){
			cut__or_slacken_edges(messages);
		}
		else if(getAggregatedValue(BarycentricMaster.PHASE_AGGREGATOR).equals(BarycentricMaster.CUT_EDGES_1)){
			compute_neighborhood();
		}
		else if(getAggregatedValue(BarycentricMaster.PHASE_AGGREGATOR).equals(BarycentricMaster.CUT_EDGES_2)){
			cut__or_slacken_edges(messages);
		}
		else if(getAggregatedValue(BarycentricMaster.PHASE_AGGREGATOR).equals(BarycentricMaster.CLEANUP_1)){
			sendMessageToAllEdges(new BarycentricMessage(getId().get(), getValue().get(), 0));
		}
		else if(getAggregatedValue(BarycentricMaster.PHASE_AGGREGATOR).equals(BarycentricMaster.CLEANUP_2)){
			//TODO:implement clean up here
			
		}
		
	}
	
	/**
	 * Recompute the position of this vertex based on the positions
	 * of its neighbors.
	 * 
	 * @param messages Messages containing the id of their sender vertex and
	 * the position of that vertex
	 */
	public void update_position(Iterable<BarycentricMessage> messages){
		double new_pos = 0;
		for(BarycentricMessage m : messages){
			new_pos += (getEdgeValue(new LongWritable(m.getSourceId())).get() * m.getValue1()) / (this.degree + 1); 
		}
		new_pos += getValue().get() / (this.degree + 1);
		setValue(new DoubleWritable(new_pos));
		sendMessageToAllEdges(new BarycentricMessage(getId().get(), new_pos, 0));
	}
	
	/**
	 * Compute the edge lengths for this run and add the information
	 * to the running total stored in the hash map
	 * 
	 * @param messages Messages containing the id of their sender vertex and
	 * the position of that vertex
	 */
	public void compute_edge_lengths(Iterable<BarycentricMessage> messages){
		long source_id;
		for(BarycentricMessage m : messages){
			source_id = m.getSourceId();
			edge_lengths.put(source_id, 
					edge_lengths.get(source_id) + Math.abs(m.getValue1() - getValue().get() ));
		}
	}
	
	/**
	 * compute the neighborhood of this vertex to be used in slackening or cutting of edges
	 */
	public void compute_neighborhood(){
		//compute average of the one hop neighborhood and send this to appropriate vertexes
		for(Edge<LongWritable, DoubleWritable> e : getEdges()){
			if(e.getTargetVertexId().compareTo(getId()) < 0){
				sendMessage(e.getTargetVertexId(), new BarycentricMessage(getId().get(), getValue().get(), getTotalNumEdges()));
			}
		}
	}
	/**
	 * Delete those edges from the graph whose score is positive (are longer than average)
	 *  
	 * @param messages Messages containing the id of their sender vertex and
	 * the neighborhood value of the vertex and the out degree value of the vertex
	 * 
	 * @throws IOException 
	 */
	public void cut__or_slacken_edges(Iterable<BarycentricMessage> messages) throws IOException{
		double avg;
		for(BarycentricMessage m : messages){
			avg = (m.getValue1() + this.degree - getEdgeValue(new LongWritable(m.getSourceId())).get()) 
					/ (m.getValue2() + getTotalNumEdges() - 1);
			if(edge_lengths.get(m.getSourceId()) > avg){
				if(getAggregatedValue(BarycentricMaster.PHASE_AGGREGATOR).equals(BarycentricMaster.CUT_EDGES_2)){
					//remove both edges
					removeEdgesRequest(getId(), new LongWritable(m.getSourceId()));
					removeEdgesRequest(new LongWritable(m.getSourceId()), getId());
				}
				else{
					//slacken both edges
					//TODO: slacken here
				}
			}
		}
	}


}
