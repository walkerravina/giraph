package org.apache.giraph.examples;

import org.apache.giraph.aggregators.DoubleSumAggregator;
import org.apache.giraph.aggregators.IntSumAggregator;
import org.apache.giraph.conf.IntConfOption;
import org.apache.giraph.conf.LongConfOption;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;

public class TrianglesVertexMaster extends DefaultMasterCompute {

	public static final IntWritable TRIANGLE_QUERY = new IntWritable(0);
	public static final IntWritable TRIANGLE_ANSWER = new IntWritable(1);
	public static final IntWritable COMPUTE_STATS = new IntWritable(2);
	public static final IntWritable HALT = new IntWritable(3);
	
	public static final IntConfOption K = new IntConfOption("KTrussesVertex.K", 5);
	
	
	public static final String PHASE_AGGREGATOR = "phase_aggregator";
	public static final String TOTAL_TRIANGLES_AGGREGATOR = "triangle_aggregator";
	public static final String GLOBAL_CLUSTERING_COEFF = "clustering_coeff";
	
	public void compute(){
		//we begin on superstep 2
		if(getSuperstep() == 2){
			setAggregatedValue(PHASE_AGGREGATOR, TRIANGLE_QUERY);
		}
		else if(getAggregatedValue(PHASE_AGGREGATOR).equals(TRIANGLE_QUERY)){
			setAggregatedValue(PHASE_AGGREGATOR, TRIANGLE_ANSWER);;
		}
		else if(getAggregatedValue(PHASE_AGGREGATOR).equals(TRIANGLE_ANSWER)){
			setAggregatedValue(PHASE_AGGREGATOR, COMPUTE_STATS);
		}
		else if(getAggregatedValue(PHASE_AGGREGATOR).equals(COMPUTE_STATS)){
			setAggregatedValue(PHASE_AGGREGATOR, HALT);
		}
		
	}
	@Override
	public void initialize() throws InstantiationException, IllegalAccessException{
		//Aggregator for tracking the phases between different parts of the KTrusses algorithm
		registerPersistentAggregator(PHASE_AGGREGATOR, IntSumAggregator.class);
		registerPersistentAggregator(TOTAL_TRIANGLES_AGGREGATOR, IntSumAggregator.class);
		registerPersistentAggregator(GLOBAL_CLUSTERING_COEFF, DoubleSumAggregator.class);
	}
}
