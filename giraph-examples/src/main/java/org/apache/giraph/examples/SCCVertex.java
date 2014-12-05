package org.apache.giraph.examples;
import java.io.IOException;
import java.util.ArrayList;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
/**
* Vertex class for finding the Strongly Connected Components (SCC)
* for a directed graph. Using the algorithm described in
* http://ilpubs.stanford.edu:8090/1077/3/p535-salihoglu.pdf
* 
* @author Walker Ravina
*
*/
public class SCCVertex extends Vertex <IntWritable, IntWritable, NullWritable, IntWritable>{
//store the transpose graph
private ArrayList<IntWritable> transposeNeighbors = new ArrayList<IntWritable>();;
//once a vertex finds its SCC it becomes inactive and ignores all messages sent to it
private boolean inActive = false;
@Override
	public void compute(Iterable<IntWritable> messages) throws IOException {
	if(!inActive){
		//steps 0 and 1 form G^R
		if(getSuperstep() == 0){
			sendMessageToAllEdges(getId());
		}
		else if(getSuperstep() == 1){
			for(IntWritable n : messages){
				transposeNeighbors.add(new IntWritable(n.get()));
			}
		}
		else if(getAggregatedValue(SCCMaster.PHASE_AGGREGATOR).equals(SCCMaster.FOWARD_TRAVERSAL_START)){
			setValue(getId());
			sendMessageToAllEdges(getValue());
		}
		else if(getAggregatedValue(SCCMaster.PHASE_AGGREGATOR).equals(SCCMaster.FOWARD_TRAVERSAL_MAIN)){
			boolean valueChanged = false;
			for(IntWritable n : messages){
				if(n.compareTo(getValue()) < 0){
					setValue(n);
					valueChanged = true;
				}
			}
			if(valueChanged){
				sendMessageToAllEdges(getValue());
				aggregate(SCCMaster.TRAVERSAL_AGGREGATOR, new BooleanWritable(valueChanged));
			}
		}
		else if(getAggregatedValue(SCCMaster.PHASE_AGGREGATOR).equals(SCCMaster.BACKWARD_TRAVERSAL_START)){
		if(getValue().equals(getId())){
		inActive = true;
		for(IntWritable v: transposeNeighbors){
		sendMessage(v, getValue());
		}
		}
		}
		else if(getAggregatedValue(SCCMaster.PHASE_AGGREGATOR).equals(SCCMaster.BACKWARD_TRAVERSAL_MAIN)){
			boolean colorsMatch = false;
			for(IntWritable n: messages){
				colorsMatch = colorsMatch || (n.compareTo(getValue()) == 0);
			}
			if(colorsMatch){;
				inActive = true;
				for(IntWritable v: transposeNeighbors){
					sendMessage(v, getValue());
				}
				aggregate(SCCMaster.TRAVERSAL_AGGREGATOR, new BooleanWritable(colorsMatch));
			}
		}
	}
	else{
		voteToHalt();
		}
	}
}