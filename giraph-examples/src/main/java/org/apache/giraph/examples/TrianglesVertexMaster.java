package org.apache.giraph.examples;

import org.apache.giraph.aggregators.DoubleSumAggregator;
import org.apache.giraph.aggregators.IntSumAggregator;
import org.apache.giraph.conf.IntConfOption;
import org.apache.giraph.conf.LongConfOption;
import org.apache.giraph.master.DefaultMasterCompute;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;

/**
 * Master Class for counting Triangles suing the approach described in
 * http://delivery.acm.org/10.1145/2460000/2457085/4799a457.pdf?ip=128.61.68.178&id=2457085&acc=NO%20RULES&key=A79D83B43E50B5B8.5E2401E94B5C98E0.4D4702B0C3E38B35.4D4702B0C3E38B35&CFID=605571014&CFTOKEN=48494189&__acm__=1417824601_29c2ea80c5405c62986c42c6ada8683e
 * @author Walker Ravina
 *
 */
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
		//Aggregator for tracking the phases between different parts of the Triangle Counting  algorithm
		registerPersistentAggregator(PHASE_AGGREGATOR, IntSumAggregator.class);
		registerPersistentAggregator(TOTAL_TRIANGLES_AGGREGATOR, IntSumAggregator.class);
		registerPersistentAggregator(GLOBAL_CLUSTERING_COEFF, DoubleSumAggregator.class);
	}
}
