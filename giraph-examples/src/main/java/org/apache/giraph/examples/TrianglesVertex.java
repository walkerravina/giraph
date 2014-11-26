package org.apache.giraph.examples;
import java.io.IOException;
import java.util.HashMap;
import org.apache.giraph.edge.Edge;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

public class TrianglesVertex extends Vertex<IntWritable, Text, DoubleWritable, TriangleVertexMessage > {

	private int true_degree = 0;
	
	@Override
	public void compute(Iterable<TriangleVertexMessage> messages) throws IOException {
		//first we assign each edge to a vertex, directing edges from low degree
		//to high degree vertexes and breaking ties by smaller id
		if(getSuperstep() == 0){
			int degree = getNumEdges();
			//the degree before removing edges
			this.true_degree = degree;
			sendMessageToAllEdges(new TriangleVertexMessage(getId().get(), degree));
		}
		else if(getSuperstep() == 1){
			int degree = getNumEdges();
			for(TriangleVertexMessage m : messages){
				//delete this edge if we don't own it
				if((m.getValue() < degree) ||
						(m.getValue() == degree && m.getSourceId() < getId().get())){
					removeEdges(new IntWritable(m.getSourceId()));
				}
				//otherwise the edges value needs to reflect the degree
				else{
					setEdgeValue(new IntWritable(m.getSourceId()), new DoubleWritable(m.getValue()));
				}
			}
		}
		else if(getAggregatedValue(TrianglesVertexMaster.PHASE_AGGREGATOR).equals(TrianglesVertexMaster.TRIANGLE_QUERY)){
			for(Edge<IntWritable, DoubleWritable> e1: getEdges()){
				for(Edge<IntWritable, DoubleWritable> e2 : getEdges()){
					//ensure that we only consider each pair of edges once
					if(e1.getTargetVertexId().get() - e2.getTargetVertexId().get() >= 0){
						continue;
					}
					//send a message to the vertex that would own the edge completing the hypothetical triangle
					if((e1.getValue().compareTo(e2.getValue()) < 0) ||
							(e1.getValue().compareTo(e2.getValue()) == 0 && e1.getTargetVertexId().get() - e2.getTargetVertexId().get() < 0)){
						sendMessage(e1.getTargetVertexId(), new TriangleVertexMessage(getId().get(), e2.getTargetVertexId().get()));
					}
					else{
						sendMessage(e2.getTargetVertexId(), new TriangleVertexMessage(getId().get(), e1.getTargetVertexId().get()));
					}
				}
			}
		}
		else if(getAggregatedValue(TrianglesVertexMaster.PHASE_AGGREGATOR).equals(TrianglesVertexMaster.TRIANGLE_ANSWER)){
			for(TriangleVertexMessage m : messages){
				//if there is an edge answering the query than we need to notify 
				//all the vertexes in the triangle
				if(getEdgeValue(new IntWritable(m.getValue())) != null){
					//value of the message holds the id of the vertex responsible for counting the triangle in the global aggregation
					//the vertex responsible is the vertex with the open triad
					sendMessage(new IntWritable(m.getSourceId()), new TriangleVertexMessage(getId().get(), m.getSourceId()));
					sendMessage(getId(), new TriangleVertexMessage(getId().get(), m.getSourceId()));
					sendMessage(new IntWritable(m.getValue()), new TriangleVertexMessage(getId().get(), m.getSourceId()));
				}
			}
			voteToHalt();
		}
		else if(getAggregatedValue(TrianglesVertexMaster.PHASE_AGGREGATOR).equals(TrianglesVertexMaster.COMPUTE_STATS)){
			int local_count = 0;
			int aggregate_count = 0;
			for(TriangleVertexMessage m : messages){
				local_count++;
				if(m.getSourceId() == getId().get()){
					aggregate_count++;
				}
			}
			double clustering_coeff = 2.0 * local_count / (this.true_degree * (this.true_degree - 1));
			setValue(new Text(local_count + ", " + clustering_coeff));
			aggregate(TrianglesVertexMaster.TOTAL_TRIANGLES_AGGREGATOR, new IntWritable(aggregate_count));
			aggregate(TrianglesVertexMaster.GLOBAL_CLUSTERING_COEFF, new DoubleWritable(clustering_coeff));
		}
		else if(getAggregatedValue(TrianglesVertexMaster.PHASE_AGGREGATOR).equals(TrianglesVertexMaster.HALT)){
			voteToHalt();
		}
		
	}

}
