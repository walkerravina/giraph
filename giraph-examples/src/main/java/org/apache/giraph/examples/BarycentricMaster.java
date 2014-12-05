package org.apache.giraph.examples;

import org.apache.giraph.aggregators.BooleanOrAggregator;
import org.apache.giraph.aggregators.IntSumAggregator;
import org.apache.giraph.conf.BooleanConfOption;
import org.apache.giraph.conf.LongConfOption;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;

/**
 * Master for performing Barycentric Clustering on the input graph
 * the edge weights are assumed to have already been normalized.
 * 
 * Implementation of Jonathan Cohen's Barycentric Clustering algorithm 
 * http://www.cslu.ogi.edu/~zak/cs506-pslc/barycentric.pdf
 * @author Walker Ravina
 *
 */
public class BarycentricMaster extends DefaultMasterCompute {
	
	public static final IntWritable UPDATE_POSITION = new IntWritable(0);
	public static final IntWritable COMPUTE_EDGE_LENGTHS = new IntWritable(1);
	public static final IntWritable SLACKEN_1 = new IntWritable(2);
	public static final IntWritable SLACKEN_2 = new IntWritable(3);
	public static final IntWritable CUT_EDGES_1 = new IntWritable(4);
	public static final IntWritable CUT_EDGES_2 = new IntWritable(5);
	public static final IntWritable FIND_COMPONENTS = new IntWritable(6);
	public static final IntWritable CLEANUP_1 = new IntWritable(7);
	public static final IntWritable CLEANUP_2 = new IntWritable(8);
	public static final IntWritable HALT = new IntWritable(9);
	
	/*
	 * String constants for the aggregators
	 */
	public static final String PHASE_AGGREGATOR = "phase_aggregator";
	public static final String ITERATIONS_AGGREGATOR = "iterations_aggregator";
	public static final String TRAVERSAL_AGGREGATOR = "traversal_aggregator";
	
	//whether to slacken edges midway through or not
	public static final BooleanConfOption SLACKEN = new BooleanConfOption("BarycentricVertex.Slacken", false);
	//how many iterations to cleanup the clusters for afterwards, default is to do none (0)
	public static final LongConfOption CLEANUP = new LongConfOption("BarycentricVertex.Cleanup", 0);
	//the number of iterations and restarts
	//note ITERATIONS should be at least 2 to avoid potential off by one errors
	public static final LongConfOption  ITERATIONS = new LongConfOption("BarycentricVertex.Iterations", 5);
	public static final LongConfOption  RESTARTS = new LongConfOption("BarycentricVertex.Restarts", 2);
	
	//counter for keeping track of cleanup iterations
	private static int cleanup_iterations = 0;
	
	public void compute(){
		//compute edge lengths when needed
		if(getAggregatedValue(PHASE_AGGREGATOR).equals(UPDATE_POSITION)
				&& getSuperstep() % (int)ITERATIONS.get(getConf()) == 0
				&& getSuperstep() != 0){
			setAggregatedValue(PHASE_AGGREGATOR, COMPUTE_EDGE_LENGTHS);
		}
		else if(getAggregatedValue(PHASE_AGGREGATOR).equals(UPDATE_POSITION)){
			//stay in position update phase
		}
		//either slacken or cut edges depending on input config
		else if(getAggregatedValue(PHASE_AGGREGATOR).equals(COMPUTE_EDGE_LENGTHS)){
			if(SLACKEN.get(getConf()) && getSuperstep() < 2 * (int)ITERATIONS.get(getConf())){
				setAggregatedValue(PHASE_AGGREGATOR, SLACKEN_1);
			}
			else{
				setAggregatedValue(PHASE_AGGREGATOR, CUT_EDGES_1);	
			}
		}
		//perform slackening or cutting
		else if(getAggregatedValue(PHASE_AGGREGATOR).equals(SLACKEN_1)){
			setAggregatedValue(PHASE_AGGREGATOR, SLACKEN_2);
		}
		else if(getAggregatedValue(PHASE_AGGREGATOR).equals(SLACKEN_2)){
			setAggregatedValue(PHASE_AGGREGATOR, UPDATE_POSITION);
		}
		else if(getAggregatedValue(PHASE_AGGREGATOR).equals(CUT_EDGES_1)){
			setAggregatedValue(PHASE_AGGREGATOR, CUT_EDGES_2);
		}
		else if(getAggregatedValue(PHASE_AGGREGATOR).equals(CUT_EDGES_2)){
			setAggregatedValue(PHASE_AGGREGATOR, FIND_COMPONENTS);
		}
		//find the components
		else if(getAggregatedValue(PHASE_AGGREGATOR).equals(FIND_COMPONENTS)
				&& getAggregatedValue(TRAVERSAL_AGGREGATOR).equals(new BooleanWritable(true))){
			//stay in FIND_COMPONENTS
		}
		else if(getAggregatedValue(PHASE_AGGREGATOR).equals(FIND_COMPONENTS)
				&& getAggregatedValue(TRAVERSAL_AGGREGATOR).equals(new BooleanWritable(false))){
			if(CLEANUP.get(getConf()) > 0){
				setAggregatedValue(PHASE_AGGREGATOR, CLEANUP_1);
			}
			else{
				setAggregatedValue(PHASE_AGGREGATOR, HALT);
			}
		}
		//cleanup the clusters
		else if(getAggregatedValue(PHASE_AGGREGATOR).equals(CLEANUP_1) && cleanup_iterations >= CLEANUP.get(getConf())){
			setAggregatedValue(PHASE_AGGREGATOR, HALT);
		}
		else if(getAggregatedValue(PHASE_AGGREGATOR).equals(CLEANUP_1)&& cleanup_iterations < CLEANUP.get(getConf())){
			setAggregatedValue(PHASE_AGGREGATOR, CLEANUP_2);
		}
		else if(getAggregatedValue(PHASE_AGGREGATOR).equals(CLEANUP_2)){
			setAggregatedValue(PHASE_AGGREGATOR, CLEANUP_1);
			cleanup_iterations++;
		}

	}
	
	@Override
	public void initialize() throws InstantiationException, IllegalAccessException{
		//Aggregator for tracking the phases between different parts of the SCC algorithm
		registerPersistentAggregator(PHASE_AGGREGATOR, IntSumAggregator.class);
		//Aggregator for tracking the traversal for finding WCC
		registerAggregator(TRAVERSAL_AGGREGATOR, BooleanOrAggregator.class);
	}

}
