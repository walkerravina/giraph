package org.apache.giraph.examples;


import org.apache.giraph.aggregators.IntSumAggregator;
import org.apache.giraph.aggregators.BooleanOrAggregator;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;

/**
 * Master class for coordinating the SCC Vertex class in finding the Strongly connected components
 * 
 * @author Walker Ravina
 *
 */
public class SCCMaster extends DefaultMasterCompute {

	public static final IntWritable TRANSPOSE_GRAPH_FORMATION = new IntWritable(0);
	public static final IntWritable TRIMMING = new IntWritable(1);
	public static final IntWritable FOWARD_TRAVERSAL_MAIN = new IntWritable(3);
	public static final IntWritable BACKWARD_TRAVERSAL_START = new IntWritable(4);
	public static final IntWritable BACKWARD_TRAVERSAL_MAIN = new IntWritable(5);
	
	//tracks the phase of the SCC algorithm
	public static final String PHASE_AGGREGATOR = "SCC_PHASE_AGGREGATOR";
	//track the progress of the forward  and backward traversal by seeing if any changes are occurring
	public static final String TRAVERSAL_AGGREGATOR = "SCC_FOWARD_TRAVERSAL_AGGREGATOR"; 
	
	@Override
	public void compute(){
		//move from formation of the transpose graph to forward traversal start
		if(getSuperstep() == 2){
			setAggregatedValue(PHASE_AGGREGATOR, TRIMMING);
		}
		else if(getAggregatedValue(PHASE_AGGREGATOR).equals(TRIMMING)){
			setAggregatedValue(PHASE_AGGREGATOR, FOWARD_TRAVERSAL_MAIN);
		}
		//check for completion of forwards traversal
		else if(getAggregatedValue(PHASE_AGGREGATOR).equals(FOWARD_TRAVERSAL_MAIN) && 
				getAggregatedValue(TRAVERSAL_AGGREGATOR).equals(new BooleanWritable(false))){
			setAggregatedValue(PHASE_AGGREGATOR, BACKWARD_TRAVERSAL_START);
			
		}
		//always process one round
		else if(getAggregatedValue(PHASE_AGGREGATOR).equals(BACKWARD_TRAVERSAL_START)){
			setAggregatedValue(PHASE_AGGREGATOR, BACKWARD_TRAVERSAL_MAIN);
		}	
		//check for completion of backwards traversal
		else if(getAggregatedValue(PHASE_AGGREGATOR).equals(BACKWARD_TRAVERSAL_MAIN) && 
				getAggregatedValue(TRAVERSAL_AGGREGATOR).equals(new BooleanWritable(false))){
			setAggregatedValue(PHASE_AGGREGATOR, TRIMMING);
		}
	}
	
	@Override
	public void initialize() throws InstantiationException, IllegalAccessException{
		//Aggregator for tracking the phases between different parts of the SCC algorithm
		registerPersistentAggregator(PHASE_AGGREGATOR, IntSumAggregator.class);
		registerAggregator(TRAVERSAL_AGGREGATOR, BooleanOrAggregator.class);
		
	}
}