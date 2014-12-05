package org.apache.giraph.examples;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;

/**
 * Vertex for finding the K-Trussess of a graph
 * @author Walker Ravina
 *
 */
public class KTrussesVertex extends Vertex<LongWritable, DoubleWritable, DoubleWritable, KTrussesMessage>{

	private boolean removed;
	private ArrayList<Long> neighborhs = new ArrayList<Long>();
	
	@Override
	public void compute(Iterable<KTrussesMessage> messages) throws IOException {
		//first we assign each edge to a vertex, directing edges from low degree
		//to high degree vertexes and breaking ties by smaller id
		if(getSuperstep() == 0){
			int degree = getNumEdges();
			sendMessageToAllEdges(new KTrussesMessage(getId().get(), degree));
			this.removed = false;
		}
		else if(getSuperstep() == 1){
			int degree = getNumEdges();
			for(KTrussesMessage m : messages){
				//delete this edge if we don't own it
				if((m.getValue() < degree) ||
						(m.getValue() == degree && m.getSourceId() < getId().get())){
					removeEdges(new LongWritable(m.getSourceId()));
				}
				//otherwise the edges value needs to reflect the degree
				else{
					setEdgeValue(new LongWritable(m.getSourceId()), new DoubleWritable(m.getValue()));
				}
			}
		}
		else if(this.removed){
			voteToHalt();
		}
		else if(getAggregatedValue(KTrussesMaster.PHASE_AGGREGATOR).equals(KTrussesMaster.TRIANGLE_QUERY)){
			for(Edge<LongWritable, DoubleWritable> e1: getEdges()){
				for(Edge<LongWritable, DoubleWritable> e2 : getEdges()){
					//ensure that we only consider each pair of edges once
					if(e1.getTargetVertexId().compareTo(e2.getTargetVertexId()) >= 0){
						continue;
					}
					//send a message to the vertex that would own the edge completing the hypothetical triangle
					if((e1.getValue().compareTo(e2.getValue()) < 0) ||
							(e1.getValue().compareTo(e2.getValue()) == 0 && e1.getTargetVertexId().compareTo(e2.getTargetVertexId()) < 0)){
						sendMessage(e1.getTargetVertexId(), new KTrussesMessage(getId().get(), e2.getTargetVertexId().get()));
					}
					else{
						sendMessage(e2.getTargetVertexId(), new KTrussesMessage(getId().get(), e1.getTargetVertexId().get()));
					}
				}
			}
		}
		else if(getAggregatedValue(KTrussesMaster.PHASE_AGGREGATOR).equals(KTrussesMaster.TRIANGLE_ANSWER)){
			for(KTrussesMessage m : messages){
				//if there is an edge answering the query than we need to notify 
				//all the vertexes which own edges in the triangle including ourself
				if(getEdgeValue(new LongWritable(m.getValue())) != null){
					//value of the message holds the other endpoint completing the edge for the vertex
					//we are sending the message too
					sendMessage(new LongWritable(m.getSourceId()), new KTrussesMessage(getId().get(), m.getValue()));
					sendMessage(getId(), new KTrussesMessage(getId().get(), m.getValue()));
					sendMessage(new LongWritable(m.getSourceId()), new KTrussesMessage(getId().get(), getId().get()));
				}
			}
		}
		else if(getAggregatedValue(KTrussesMaster.PHASE_AGGREGATOR).equals(KTrussesMaster.COUNT_EDGE_SUPPORT)){
			long count = 0;
			HashMap<Long, Integer> edge_support = new HashMap<Long, Integer>();
			int K = KTrussesMaster.K.get(getConf());
			for(KTrussesMessage m : messages){
				long j = m.getValue();
				if(edge_support.containsKey(j)){
					edge_support.put(j, edge_support.get(j) + 1);
				}
				else{
					edge_support.put(j, 1);
				}
			}
			for(Edge<LongWritable, DoubleWritable> e : getEdges()){
				long key = e.getTargetVertexId().get();
				//remove edge if it is lacking support
				if(!edge_support.containsKey(key) || edge_support.get(key) < K - 2){
					removeEdges(new LongWritable(key));
					aggregate(KTrussesMaster.EDGE_SUPPORT_AGGREGATOR, new BooleanWritable(true));
				}
				//send message notifying of support
				else{
					sendMessage(getId(), new KTrussesMessage(0,0));
					sendMessage(new LongWritable(key), new KTrussesMessage(0,0));
				}
			}
		}
		else if(getAggregatedValue(KTrussesMaster.PHASE_AGGREGATOR).equals(KTrussesMaster.REMOVE_NODES)){
			//no edges pointing to this node or coming out from this node
			if(!messages.iterator().hasNext()){
				this.removed = true;
				//-1 is flag value for not in a k truss
				setValue(new DoubleWritable(-1));
			}
		}
		else if(getAggregatedValue(KTrussesMaster.PHASE_AGGREGATOR).equals(KTrussesMaster.FIND_COMPONENTS_0)){
			//make edges bidirectional again for the DFS
			sendMessageToAllEdges(new KTrussesMessage(getId().get(), 0));
		}
		else if(getAggregatedValue(KTrussesMaster.PHASE_AGGREGATOR).equals(KTrussesMaster.FIND_COMPONENTS_1)){
			for(KTrussesMessage m : messages){
				this.neighborhs.add(m.getSourceId());
			}
			sendMessageToAllEdges(new KTrussesMessage(getId().get(), getId().get()));
			for(Long l : this.neighborhs){
				sendMessage(new LongWritable(l), new KTrussesMessage(getId().get(), getId().get()));
			}
			setValue(new DoubleWritable(getId().get()));
			voteToHalt();
		}
		else if(getAggregatedValue(KTrussesMaster.PHASE_AGGREGATOR).equals(KTrussesMaster.FIND_COMPONENTS_2)){
			boolean changed = false;
			for(KTrussesMessage m : messages){
				if(m.getValue() < getValue().get()){
					setValue(new DoubleWritable(m.getValue()));
					changed = true;
				}
			}
			if(changed){
				sendMessageToAllEdges(new KTrussesMessage(getId().get(), (long)getValue().get()));
				for(Long l : this.neighborhs){
					sendMessage(new LongWritable(l), new KTrussesMessage(getId().get(), getId().get()));
				}
			}
			voteToHalt();
		}
	}



}
