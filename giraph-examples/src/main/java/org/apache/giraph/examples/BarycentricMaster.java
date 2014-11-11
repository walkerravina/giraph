package org.apache.giraph.examples;

import org.apache.giraph.aggregators.BooleanOrAggregator;
import org.apache.giraph.aggregators.IntSumAggregator;
import org.apache.giraph.aggregators.TextAppendAggregator;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.hadoop.io.IntWritable;

public class BarycentricMaster extends DefaultMasterCompute {
	
	public static final IntWritable UPDATE_POSITION = new IntWritable(0);
	public static final IntWritable COMPUTE_EDGE_LENGTHS_1 = new IntWritable(1);
	public static final IntWritable COMPUTE_EDGE_LENGTHS_2 = new IntWritable(2);
	public static final IntWritable SLACKEN_1 = new IntWritable(3);
	public static final IntWritable SLACKEN_2 = new IntWritable(4);
	public static final IntWritable CUT_EDGES_1 = new IntWritable(5);
	public static final IntWritable CUT_EDGES_2 = new IntWritable(6);
	public static final IntWritable FIND_COMPONENTS = new IntWritable(7);
	public static final IntWritable CLEANUP_1 = new IntWritable(8);
	public static final IntWritable CLEANUP_2 = new IntWritable(9);
	
	public static final String PHASE_AGGREGATOR = "phase_aggregator";
	
	
	@Override
	public void initialize() throws InstantiationException, IllegalAccessException{
		//Aggregator for tracking the phases between different parts of the SCC algorithm
		registerPersistentAggregator(PHASE_AGGREGATOR, IntSumAggregator.class);
	}

}
