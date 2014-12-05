package org.apache.giraph.examples;


import org.apache.giraph.aggregators.IntArrayAggregator;
import org.apache.giraph.aggregators.IntSumAggregator;
import org.apache.giraph.aggregators.BooleanOrAggregator;
import org.apache.giraph.conf.IntConfOption;
import org.apache.giraph.conf.LongConfOption;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.giraph.utils.IntArrayWritable;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.giraph.conf.FloatConfOption;

/**
 * Master class for coordinating the SCC Vertex class in finding the Strongly connected components
 * 
 * @author Walker Ravina
 *
 */
public class SCC_B_Master extends DefaultMasterCompute {

	public static final IntWritable TRANSPOSE_GRAPH_FORMATION = new IntWritable(0);
	public static final IntWritable TRAVERSAL_SELECTION_1 = new IntWritable(1);
	public static final IntWritable TRAVERSAL_SELECTION_2 = new IntWritable(2);
	public static final IntWritable TRAVERSAL_MAIN = new IntWritable(3);
	
	//tracks the phase of the SCC algorithm
	public static final String PHASE_AGGREGATOR = "SCC_PHASE_AGGREGATOR";
	//track the progress of the forward  and backward traversal by seeing if any changes are occurring
	public static final String TRAVERSAL_AGGREGATOR = "SCC_FOWARD_TRAVERSAL_AGGREGATOR";
	public static final String PROP_AGGREGATOR = "SCC_PROP_AGGREGATOR";
	public static final String INACTIVE_COUNT_AGGREGATOR = "SCC_INACTIVE_AGGREGATOR";
	
	public static final IntConfOption K = new IntConfOption("SCC_B.K", 20);
	
	@Override
	public void compute(){
		//move from formation of the transpose graph to forward traversal start
		if(getSuperstep() == 2){
			setAggregatedValue(PHASE_AGGREGATOR, TRAVERSAL_SELECTION_1);
		}
		//selection round 1
		else if(getAggregatedValue(PHASE_AGGREGATOR).equals(TRAVERSAL_SELECTION_1)){
			setAggregatedValue(PHASE_AGGREGATOR, TRAVERSAL_SELECTION_2);
		}
		// we begin the traversal
		else if(getAggregatedValue(PHASE_AGGREGATOR).equals(TRAVERSAL_SELECTION_2)){
			setAggregatedValue(PROP_AGGREGATOR, new IntArrayWritable());
			setAggregatedValue(PHASE_AGGREGATOR, TRAVERSAL_MAIN);
		}
		//check for completion of forwards traversal
		else if(getAggregatedValue(PHASE_AGGREGATOR).equals(TRAVERSAL_MAIN) && 
				getAggregatedValue(TRAVERSAL_AGGREGATOR).equals(new BooleanWritable(false))){
			setAggregatedValue(PHASE_AGGREGATOR, TRAVERSAL_SELECTION_1);	
		}
	}
	
	@Override
	public void initialize() throws InstantiationException, IllegalAccessException{
		//Aggregator for tracking the phases between different parts of the SCC algorithm
		registerPersistentAggregator(PHASE_AGGREGATOR, IntSumAggregator.class);
		//Aggregator for tracking progress of the traversals
		registerAggregator(TRAVERSAL_AGGREGATOR, BooleanOrAggregator.class);
		//Aggregator for tracking the vertexes to propagate
		registerPersistentAggregator(PROP_AGGREGATOR, IntArrayAggregator.class);
		registerPersistentAggregator(INACTIVE_COUNT_AGGREGATOR, IntSumAggregator.class);
		
	}
}